package tpch

import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer
import scala.math.BigDecimal.RoundingMode

class Result(concurrency: Int, sf: Int) {

  val power : scala.collection.mutable.Map[Int, Long] = scala.collection.mutable.Map()
  val throughputPerQ : scala.collection.mutable.Map[Int, ListBuffer[(Int, Long)]] = scala.collection.mutable.Map()
  val throughputE2E: scala.collection.mutable.Map[Int, Long] = scala.collection.mutable.Map()

  def recordPowerRes(index: Int, time: Long): Unit = {
    //val timeInSec = time.toDouble / 1000
    //power += (index -> Seq(BigDecimal(timeInSec).setScale(2, RoundingMode.HALF_EVEN), BigDecimal(scala.math.log(timeInSec)).setScale(2, RoundingMode.HALF_EVEN)))
    power += (index -> time)
  }

  // Per query concurrent result
  def recordThroughputQRes(index: Int, time: Long, threadNo: Int): Unit = {
    throughputPerQ.synchronized {
      if (!throughputPerQ.contains(index)) {
        throughputPerQ += (index -> ListBuffer((threadNo,time)))
      } else {
        throughputPerQ.get(index).get += ((threadNo, time))
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
      val b = new StringBuilder()
      b.append(t._1)
      // Sort by threadNo
      t._2.toList.sortBy(_._1).foreach(e => b.append(",").append(e._2))
      val row = Seq(t._1) ++ t._2.toList
      tpCsvOut.write(b.toString())
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
      tpECsvOut.write(row.mkString(","))
      tpECsvOut.write("\n")
    })
    tpECsvOut.close()

    // record tpch metrics
    val results = compute()
    val resultFile = new File(dir, "result.csv")
    val resultsFileOut = new PrintWriter(resultFile)

    println(s"Writing TPCH Metric Results to ${resultFile.getAbsolutePath}")

    resultsFileOut.write("Power@Size,Throughput@Size,QphH\n")
    resultsFileOut.write(s"${results._1},${results._2},${results._3}\n")
    resultsFileOut.close()

  }

  /*
  return tuple of (Power, Throughput, QphH)
  see http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
   */
  def compute(): (Double, Double, Double) = {
    val power = computePower()
    val throughput = computeThroughput()
    val qphh = computeQphH(power, throughput)

    (power, throughput, qphh)
  }

  // See http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
  // section 5.4.1.1
  def computePower(): Double = {
    // TODO ratio thresholds

    val productTimes = {
      var ret: Double = 1d
      power.foreach(e => ret = ret * (e._2.toDouble / 1000))
      ret
    }

    // TODO refresh functions?
//    val productRF = 0.0d

    (3600 * sf) / scala.math.pow(productTimes, 1d/24)
  }

  //TPC-H Throughput@Size = (S*22*3600)/Ts *SF
  // See http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
  // section 5.4.2.1
  def computeThroughput(): Double = {
    val max = {
      var ret: Double = 0d
      throughputE2E.foreach(e => {
        val secs = e._2.toDouble / 1000
        if (ret < secs) {
          ret = secs
        }
      })
      ret
    }
    (concurrency * 22 * 3600) / max * sf
  }

  // See http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
  // section 5.4.3.1
  def computeQphH(power: Double, throughput: Double): Double = {
    scala.math.sqrt(power * throughput)
  }

}

object ResultHelper {

  object Mode extends Enumeration {
    val Power, ThroughputQ, ThroughputE2E = Value
  }

  def timeAndRecord[R](result: Result, index: Int, mode: Mode.Value, threadNo: Int = 0)(block: => R): R = {
    val t0 = System.currentTimeMillis()
    val res = block    // call-by-name
    val t1 = System.currentTimeMillis()
    mode match {
      case Mode.Power => result.recordPowerRes(index, t1 - t0)
      case Mode.ThroughputQ => result.recordThroughputQRes(index, t1 - t0, threadNo)
      case Mode.ThroughputE2E => result.recordThroughputE2E(index, t1 - t0)
      case _ => throw new IllegalStateException()
    }
    res
  }
}
