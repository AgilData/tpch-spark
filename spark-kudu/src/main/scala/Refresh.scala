package tpch

import java.io.{BufferedReader, File, FileReader}
import java.util.Random

import org.kududb.client.SessionConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.kududb.client
import org.kududb.spark.kudu._

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by drewmanlove on 6/15/16.
  */
object Refresh {

  def executeRF1(dir: String, set: Int, execCtx: ExecCtx): Unit = {

    println("Executing RF1...")
    val sc = execCtx.sparkCtx
    val sqlContext = execCtx.sqlCtx
    val kuduContext = execCtx.kuduCtx.value

    // TODO would hdfs be more performant?
    val ordersU = dir + s"/orders.tbl.u${set}"
    println(s"Loading customer updates from $ordersU")
    val order = sqlContext.createDataFrame(Source.fromFile(new File(ordersU)).getLines().toList.map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)))

    val lineItemU = dir + s"/lineitem.tbl.u${set}"
    println(s"Loading lineitem updates from $lineItemU")
    val lineitem = sqlContext.createDataFrame(Source.fromFile(new File(lineItemU)).getLines().toList.map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)))

    // Simulate transactionality
    val session = kuduContext.getSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val random = new Random()
    val lineItemCol = lineitem.collect().iterator
    var inserts = 0

    // This dataframe will have the appropriate number of rows for our SF
    order.collect().foreach(row => {
      kuduContext.insert(row, "order", session)
      inserts += 1

      for (i <- 1 to (random.nextInt(7 - 1) + 1)) {
        kuduContext.insert(lineItemCol.next(), "lineitem", session)
        inserts += 2
      }

      session.flush()

    })

    session.close()

    println(s"RF1 completes $inserts inserts")

  }

  def executeRF2(dir: String, set: Int, execCtx: ExecCtx, sf: Int): Unit = {
    println("Executing RF2...")

    val sc = execCtx.sparkCtx
    val sqlContext = execCtx.sqlCtx
    val kuduContext = execCtx.kuduCtx.value

    import sqlContext.implicits._

    val deletesU = dir + s"/delete.${set}"
    println(s"Loading delete keys from $deletesU")
    val lines = Source.fromFile(new File(deletesU)).getLines()

    // Simulate transactionality
    val session = kuduContext.getSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    var oDeletes = 0
    var lDeletes = 0

    var totalODelete = 0L
    var totalLDelete = 0L
    var totalFlush = 0L
    var totalLookup = 0L

    while (lines.hasNext) {
      val line = lines.next().split("|")
      val orderKey = Integer.parseInt(line(0))
      totalODelete += ResultHelper.time() {kuduContext.delete(Seq(orderKey), Seq("o_orderkey"),"order", session)}._1
      oDeletes += 1

      val lineItemKeys: ListBuffer[(Int,Int)] = new ListBuffer()

      // Workaround to query kudu direct and get a range pk scan
      val r = ResultHelper.time() {
        val c = execCtx.kuduCtx.value.syncClient
        val t = c.openTable("lineitem")
        val b = c.newScannerBuilder(t)
        val p = new client.PartialRow(t.getSchema)
        p.addInt("l_orderkey", orderKey)
        p.addInt("l_linenumber", 1)
        b.lowerBound(p)

        val e = new client.PartialRow(t.getSchema)
        e.addInt("l_orderkey", orderKey)
        // TODO not totally correct as new rows are added as part of RF1
        e.addInt("l_linenumber", 6001215*sf)
        b.exclusiveUpperBound(e)

        import scala.collection.JavaConverters._
        b.setProjectedColumnNames(List("l_orderkey", "l_linenumber").asJava)
        val s = b.build()
        while (s.hasMoreRows) {
          val result = s.nextRows()
          while (result.hasNext) {
            val t = result.next()
            lineItemKeys += ((t.getInt(0), t.getInt(1)))
          }
        }
        // TODO this spark job does a full table scan rather than a pkey range scan
//        val rdd = sqlContext.table("lineitem")
//        .where($"l_orderkey" === orderKey)
//          .select("l_linenumber")
//        //rdd.explain(true)
//        rdd.collect()
      }

      totalLookup += r._1
      val rows = r._2

      lineItemKeys.foreach(f => {
        totalLDelete += ResultHelper.time() { kuduContext.delete(Seq(f._1, f._2), Seq("l_orderkey", "l_linenumber"),"lineitem", session)}._1
        lDeletes +=1
      })

      totalFlush += ResultHelper.time() {session.flush()}._1

    }

    println(s"RF2 completes $oDeletes order and $lDeletes lineitem deletes!")
    println(s"RF2 completes avg order delete time: ${totalODelete.toDouble/oDeletes}ms")
    println(s"RF2 completes avg lineitem delete time: ${totalLDelete.toDouble/lDeletes}ms")
    println(s"RF2 completes avg lookup time: ${totalLookup.toDouble/oDeletes}ms")
    println(s"RF2 completes avg flush time: ${totalFlush.toDouble/oDeletes}ms")

    session.close()

  }

}
