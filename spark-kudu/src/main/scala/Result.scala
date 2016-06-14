package tpch

import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

class Result(concurrency: Int) {

  val power : scala.collection.mutable.Map[Int, Long] = scala.collection.mutable.Map()
  val throughput : scala.collection.mutable.Map[Int, ListBuffer[Long]] = scala.collection.mutable.Map()

  def recordPowerRes(index: Int, time: Long): Unit = {
    //val timeInSec = time.toDouble / 1000
    //power += (index -> Seq(BigDecimal(timeInSec).setScale(2, RoundingMode.HALF_EVEN), BigDecimal(scala.math.log(timeInSec)).setScale(2, RoundingMode.HALF_EVEN)))
    power += (index -> time)
  }

  def recordThroughputRes(index: Int, time: Long): Unit = {
    throughput.synchronized {
      if (!throughput.contains(index)) {
        throughput += (index -> ListBuffer(time))
      } else {
        throughput.get(index).get += time
      }
    }
  }

  def record(dir: String): Unit = {
    val d = new File(dir)
    if (!d.exists()) {
      d.mkdir()
    }

    // record power
    val powerCsvOut = new PrintWriter(new File(dir, "power.csv"))
    powerCsvOut.write("Query,Time(ms),ln()\n")
    power foreach( t => {
      val row = Seq(t._1) ++ Seq(t._2)
      powerCsvOut.write(row.mkString(","))
      powerCsvOut.write("\n")
    })
    powerCsvOut.close()

    // record throughput
    val tpCsvOut = new PrintWriter(new File(dir, "throughput.csv"))
    val b = new StringBuilder("Query")
    1 to concurrency foreach(n => b.append(",").append(s"time${n}(ms)"))
    tpCsvOut.write(b.toString())
    tpCsvOut.write("\n")

    throughput foreach( t => {
      val row = Seq(t._1) ++ t._2.toList
      tpCsvOut.write(row.mkString(","))
      tpCsvOut.write("\n")
    })
    tpCsvOut.close()


  }

}
