package tpch

import java.io.File

import org.apache.commons.cli.{Options, BasicParser}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

case class ExecCtx(sparkCtx: SparkContext, sqlCtx: SQLContext, kuduCtx: Broadcast[ExtendedKuduContext])

/**
  * Created by andy on 5/6/16.
  */
object Main {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    val options = new Options
    options.addOption("k", "kuduMaster", true, "IP:PORT of kudu master")
    options.addOption("s", "sparkMaster", true, "Spark master spark://IP:PORT or local[*]")
    options.addOption("i", "inputDir", true, "Location of dbgen data")
    options.addOption("m", "mode", true, "populate, sql, csv")
    options.addOption("q", "queryFile", true, "queryFile")
    options.addOption("f", "file", true, "file")
    options.addOption("e", "executorMemory", true, "spark.executor.memory")

    val parser = new BasicParser
    val cmd = parser.parse(options, args)

    val KUDU_MASTER = cmd.getOptionValue("k", "127.0.0.1:7050")
    val SPARK_MASTER = cmd.getOptionValue("s", "local[*]")
    val INPUT_DIR = cmd.getOptionValue("i", "./dbgen")
    val MODE = cmd.getOptionValue("m")
    val EXEC_MEM = cmd.getOptionValue("e", "1g")
    val OUTPUT_DIR = "/tmp"
    println(s"KUDU_MASTER=$KUDU_MASTER")
    println(s"INPUT_DIR=$INPUT_DIR")
    println(s"SPARK_MASTER=$SPARK_MASTER")
    println(s"EXEC_MEM=$EXEC_MEM")

    // get the name of the class excluding dollar signs and package
    val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("TPC-H " + className)
      .setExecutorEnv("spark.executor.memory", EXEC_MEM)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkCtx = new SparkContext(conf)
    sparkCtx.addJar("/mnt/data/maven_repository/org/kududb/kudu-spark_2.11/1.0.0-SNAPSHOT/kudu-spark_2.11-1.0.0-SNAPSHOT.jar")
    sparkCtx.addJar("/mnt/data/tpch-spark/spark-kudu/target/scala-2.11/spark-tpc-h-queries_2.11-1.1-SNAPSHOT.jar")
    val sqlCtx = new org.apache.spark.sql.SQLContext(sparkCtx)
    val kuduCtx = sparkCtx.broadcast(new ExtendedKuduContext(KUDU_MASTER))
    val execCtx = ExecCtx(sparkCtx, sqlCtx, kuduCtx)

    MODE match {
      case "populate" => Populate.executeImport(execCtx, INPUT_DIR)
      case "sql" => RunQueries.execute(execCtx, cmd.getOptionValue("q"))
      case "csv" => {
        val file = new File(cmd.getOptionValue("f"))
        val queryIdx = "*"
        new TpchQuery(execCtx).executeQueries(file, queryIdx)
      }
      case _ => println("first param required: must be populate, sql, or csv")
    }
  }

}
