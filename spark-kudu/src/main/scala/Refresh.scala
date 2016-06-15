package tpch

import java.io.{BufferedReader, File, FileReader}
import java.util.Random

import org.kududb.client.SessionConfiguration

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
//    var index = 0

    // This dataframe will have the appropriate number of rows for our SF
    order.collect().foreach(row => {
      kuduContext.insert(row, "order", session)

      for (i <- 1 to (random.nextInt(7 - 1) + 1)) {
        kuduContext.insert(lineItemCol.next(), "lineitem", session)
      }

      session.flush()

    })

    session.close()

  }

  def executeRF2(dir: String, set: Int, execCtx: ExecCtx): Unit = {

    val sc = execCtx.sparkCtx
    val sqlContext = execCtx.sqlCtx
    val kuduContext = execCtx.kuduCtx.value

    // TODO would hdfs be more performant?
    val deletesU = dir + s"/delete.tbl.u${set}"
    println(s"Loading delete keys from $deletesU")
    val lines = Source.fromFile(new File(deletesU)).getLines()
//    val order = sqlContext.createDataFrame(Source.fromFile(new File(ordersU)).getLines().toList.map(_.split('|')).map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt, p(8).trim)))

    // Simulate transactionality
    val session = kuduContext.getSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    while (lines.hasNext) {
      val line = lines.next().split("|")
      kuduContext.delete(Integer.parseInt(line(0)), "order", session)
      kuduContext.delete(Integer.parseInt(line(0)), "lineitem", session)
      session.flush()
    }
    
    session.close()

  }

}
