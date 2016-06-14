package tpch

import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

class Result(concurrency: Int) {

  val power : scala.collection.mutable.Map[Int, Long] = scala.collection.mutable.Map()
  val throughputPerQ : scala.collection.mutable.Map[Int, ListBuffer[Long]] = scala.collection.mutable.Map()
  val throughputE2E: scala.collection.mutable.Map[Int, Long] = scala.collection.mutable.Map()

  def recordPowerRes(index: Int, time: Long): Unit = {
    //val timeInSec = time.toDouble / 1000
    //power += (index -> Seq(BigDecimal(timeInSec).setScale(2, RoundingMode.HALF_EVEN), BigDecimal(scala.math.log(timeInSec)).setScale(2, RoundingMode.HALF_EVEN)))
    power += (index -> time)
  }

  // Per query concurrent result
  def recordThroughputQRes(index: Int, time: Long): Unit = {
    throughputPerQ.synchronized {
      if (!throughputPerQ.contains(index)) {
        throughputPerQ += (index -> ListBuffer(time))
      } else {
        throughputPerQ.get(index).get += time
      }
    }
  }

  // End to end concurrent result
  def recordThroughputE2E(thread: Int, time: Long): Unit = {
    throughputE2E.synchronized {
      throughputE2E += (thread -> time)
    }
  }

  def record(dir: String): Unit = {
    val d = new File(dir)
    if (!d.exists()) {
      d.mkdir()
    }

    // record power
    val powerFile = new File(dir, "power.csv")
    val powerCsvOut = new PrintWriter(powerFile)
    println(s"Writing power results to ${powerFile.getAbsolutePath}")
    powerCsvOut.write("Query,Time(ms)\n")
    power.toSeq.sortBy(_._1) foreach( t => {
      val row = Seq(t._1) ++ Seq(t._2)
      powerCsvOut.write(row.mkString(","))
      powerCsvOut.write("\n")
    })
    powerCsvOut.close()

    // record throughput, per query times
    val tpCsvFile = new File(dir, "throughputPerQ.csv")
    val tpCsvOut = new PrintWriter(tpCsvFile)
    println(s"Writing throughput per query results to ${tpCsvFile.getAbsolutePath}")
    val b = new StringBuilder("Query")
    1 to concurrency foreach(n => b.append(",").append(s"time${n}(ms)"))
    tpCsvOut.write(b.toString())
    tpCsvOut.write("\n")

    throughputPerQ.toSeq.sortBy(_._1) foreach(t => {
      val row = Seq(t._1) ++ t._2.toList
      tpCsvOut.write(row.mkString(","))
      tpCsvOut.write("\n")
    })
    tpCsvOut.close()

    // record throughput, end to end
    val tpECsvFile = new File(dir, "throughputE2E.csv")
    val tpECsvOut = new PrintWriter(tpECsvFile)
    println(s"Writing throughput end to end results to ${tpECsvFile.getAbsolutePath}")

    tpECsvOut.write("Thread,Time(ms)\n")
    throughputE2E.toSeq.sortBy(_._1) foreach(t => {
      val row = Seq(t._1) ++ Seq(t._2)
      tpCsvOut.write(row.mkString(","))
      tpCsvOut.write("\n")
    })
    tpCsvOut.close()


  }

  /*
  return tuple of Power, Throughput, QphH
   */
  def compute(): (BigDecimal, BigDecimal, BigDecimal) = {
    throw new UnsupportedOperationException
  }

}

object ResultHelper {

  object Mode extends Enumeration {
    val Power, ThroughputQ, ThroughputE2E = Value
  }

  def timeAndRecord[R](result: Result, index: Int, mode: Mode.Value)(block: => R): R = {
    val t0 = System.currentTimeMillis()
    val res = block    // call-by-name
    val t1 = System.currentTimeMillis()
    mode match {
      case Mode.Power => result.recordPowerRes(index, t1 - t0)
      case Mode.ThroughputQ => result.recordThroughputQRes(index, t1 - t0)
      case Mode.ThroughputE2E => result.recordThroughputE2E(index, t1 - t0)
      case _ => throw new IllegalStateException()
    }
    res
  }
}
