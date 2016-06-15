package tpch

import java.io.File
import java.util.Random

import org.kududb.client.SessionConfiguration

/**
  * Created by drewmanlove on 6/15/16.
  */
object Refresh {

  def executeRF1(dir: String, set: Int, execCtx: ExecCtx): Unit = {

    println("Executing RF1...")
    val sc = execCtx.sparkCtx
    val sqlContext = execCtx.sqlCtx
    val kuduContext = execCtx.kuduCtx.asInstanceOf[ExtendedKuduContext]

    val customer = sqlContext.createDataFrame(sc.textFile(dir + s"/customer.tbl.u${set}").map(_.split('|')).map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)))
    val lineitem = sqlContext.createDataFrame(sc.textFile(dir + s"/lineitem.tbl.u${set}").map(_.split('|')).map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt, p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)))

    // Simulate transactionality
    val session = kuduContext.getSession()
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val random = new Random()

    val lineItemIT = lineitem.toLocalIterator()
    // This dataframe will have the appropriate number of rows for our SF
    customer.foreach(row => {
      kuduContext.insert(row, "customer", session)

      for (i <- random.nextInt(7 - 1) + 1) {
        kuduContext.insert(lineItemIT.next(), "lineitem", session)
      }

      session.flush()

    })

    session.close()

  }

  def executeRF2(dir: String, set: Int, execCtx: ExecCtx): Unit = {

  }

}
