package tpch

import java.io.File
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

case class ExecCtx(sparkCtx: SparkContext, sqlCtx: SQLContext, kuduCtx: Broadcast[ExtendedKuduContext])

/**
  * Created by andy on 5/6/16.
  */
object Main {
  val concurrency = 5 // TODO concurrency configurable

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
    options.addOption("u", "users", true, "Number of concurrent users for benchmark")
    options.addOption("p", "partitionCount", true, "spark.sql.shuffle.partitions")

    val parser = new BasicParser
    val cmd = parser.parse(options, args)

    val KUDU_MASTER = cmd.getOptionValue("k", "127.0.0.1:7050")
    val SPARK_MASTER = cmd.getOptionValue("s", "local[*]")
    val INPUT_DIR = cmd.getOptionValue("i", "./dbgen")
    val MODE = cmd.getOptionValue("m")
    val EXEC_MEM = cmd.getOptionValue("e", "1g")
    val PARTITION_COUNT = cmd.getOptionValue("p", "20")
    val OUTPUT_DIR = "/tmp"
    println(s"KUDU_MASTER=$KUDU_MASTER")
    println(s"INPUT_DIR=$INPUT_DIR")
    println(s"SPARK_MASTER=$SPARK_MASTER")
    println(s"EXEC_MEM=$EXEC_MEM")
    println(s"PARTITION_COUNT=$PARTITION_COUNT")

    // get the name of the class excluding dollar signs and package
    val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")
    val conf = new SparkConf()
      .setMaster(SPARK_MASTER)
      .setAppName("TPC-H " + className)
      .setExecutorEnv("spark.executor.memory", EXEC_MEM)
      .setExecutorEnv("spark.sql.tungsten.enabled", "true")
      .setExecutorEnv("spark.sql.shuffle.partitions", PARTITION_COUNT)
      .setExecutorEnv("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", EXEC_MEM)
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.sql.shuffle.partitions", PARTITION_COUNT)
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
        //val users = cmd.getOptionValue("u", concurrency)
        val result = new Result(concurrency)

        // Power (single thread)
        new TpchQuery(execCtx, result).executeQueries(file, queryIdx, ResultHelper.Mode.Power)

//        // Throughput (concurrency)
//        val pool: ExecutorService = Executors.newFixedThreadPool(concurrency)
//        val tasks = {
//          for (i <- 1 to concurrency) yield
//
//             new Callable[String]() {
//              def call(): String = {
//                ResultHelper.timeAndRecord(result, i, ResultHelper.Mode.ThroughputE2E) {
//                  new TpchQuery(execCtx, result).executeQueries(file, queryIdx, ResultHelper.Mode.ThroughputQ)
//                }
//                "OK"
//              }
//            }
//
//        }
//
//        import scala.collection.JavaConversions._
//        pool.invokeAll(tasks.toList)
//        pool.shutdown()

        result.record("./tpch_result")
      }
      case _ => println("first param required: must be populate, sql, or csv")
    }
  }

}
