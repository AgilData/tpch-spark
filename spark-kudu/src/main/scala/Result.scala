package tpch

import java.io.{File, PrintWriter}

import scala.math.BigDecimal.RoundingMode

class Result {

  val power : scala.collection.mutable.Map[Int, Seq[BigDecimal]] = scala.collection.mutable.Map()


  def recordPowerRes(index: Int, time: Long): Unit = {
    val timeInSec = time.toDouble / 60
    power += (index -> Seq(BigDecimal(timeInSec).setScale(2, RoundingMode.HALF_EVEN), BigDecimal(scala.math.log(timeInSec)).setScale(2, RoundingMode.HALF_EVEN)))
  }

  def record(dir: String): Unit = {
    val d = new File(dir)
    if (!d.exists()) {
      d.mkdir()
    }

    // record power
    val powerCsvOut = new PrintWriter(new File(dir, "power.csv"))
    powerCsvOut.write("Query,Time(sec),ln()")
    power foreach( t => {
      val row = Seq(t._1) ++ t._2
      powerCsvOut.write(row.mkString(","))
    })
    powerCsvOut.close()

  }

}
