package tpch

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.ini4j.{Ini, IniPreferences}

/**
  * Created by andy on 5/6/16.
  */
object Main {
  val concurrency = 5

  object BenchMode extends Enumeration {
    val All, Power, Throughput = Value
  }

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
    options.addOption("u", "users", true, s"Number of concurrent users for benchmark, default is ${concurrency}")
    options.addOption("p", "partitionCount", true, "spark.sql.shuffle.partitions")
    options.addOption("d", "kuduPartitionCount", true, "Kudu partition count")
    options.addOption("w", "power", false, "run only the power benchmark")
    options.addOption("t", "throughput", false, "run only the throughput benchmark")
    options.addOption("c", "scale-factor", true, "scale factor of data population")
    options.addOption("r", "maven-repo", true, "location of maven repository")
    options.addOption("x", "query-index", true, "The index of the query to run, or * for all")
    options.addOption("a", "skip-rf", true, "Skip the RF portions of the test")
    options.addOption("b", "direct", true, "Work directly from s3")

    val parser = new BasicParser
    val cmd = parser.parse(options, args)

    val benchMode = {
      if (cmd.hasOption("w") && !cmd.hasOption("t")) {
        BenchMode.Power
      } else if (cmd.hasOption("t") && !cmd.hasOption("w")) {
        BenchMode.Throughput
      } else {
        BenchMode.All
      }
    }

    val KUDU_MASTER = cmd.getOptionValue("k", "127.0.0.1:7050")
    val SPARK_MASTER = cmd.getOptionValue("s", "local[*]")
    val INPUT_DIR = cmd.getOptionValue("i", "./dbgen")
    val MODE = cmd.getOptionValue("m")
    val EXEC_MEM = cmd.getOptionValue("e", "1g")
    val PARTITION_COUNT = cmd.getOptionValue("p", "20")
    val KUDU_PARTITION_COUNT = Integer.parseInt(cmd.getOptionValue("d", "20"))
    val OUTPUT_DIR = "/tmp"
    val MAVEN_REPO = cmd.getOptionValue("r", s"${System.getProperty("user.home")}/.m2/repository")
    val queryIdx = cmd.getOptionValue("x", "*")
    val skipRF =  cmd.getOptionValue("a", "false").toBoolean
    val scaleFactor = Integer.parseInt(cmd.getOptionValue("c", "1"))
    val direct = cmd.getOptionValue("b", "false").toBoolean

    println(s"KUDU_MASTER=$KUDU_MASTER")
    println(s"INPUT_DIR=$INPUT_DIR")
    println(s"SPARK_MASTER=$SPARK_MASTER")
    println(s"EXEC_MEM=$EXEC_MEM")
    println(s"PARTITION_COUNT=$PARTITION_COUNT")
    println(s"KUDU_PARTITION_COUNT=$KUDU_PARTITION_COUNT")
    println(s"queryIdx=$queryIdx")

    // get the name of the class excluding dollar signs and package
    val className = this.getClass.getName.split("\\.").last.replaceAll("\\$", "")
    val execCtx = SparkHelper.getExecContext(SPARK_MASTER, KUDU_MASTER, EXEC_MEM, PARTITION_COUNT, className, MAVEN_REPO)


    val home = new File(System.getProperty("user.home"))
    val creds = new File(home, ".aws/credentials")
    if(creds.exists()) {
      val prefs = new IniPreferences(new Ini(creds))
      val keyId = prefs.node("default").get("aws_access_key_id", null)
      val accessKey = prefs.node("default").get("aws_secret_access_key", null)
      println(s"------ $keyId")
      execCtx.sparkCtx.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", keyId)
      execCtx.sparkCtx.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", accessKey)
    }

    MODE match {
      case "populate" =>
        new Populate(execCtx, INPUT_DIR, KUDU_PARTITION_COUNT).executeImport()
      case "ingest" => {
        if(direct) {
          return; // Don't ingest in direct mode
        }
        new Populate(execCtx, INPUT_DIR, KUDU_PARTITION_COUNT).executeIngest(scaleFactor)
      }
      case "split" => {
        val scaleFactor = Integer.parseInt(cmd.getOptionValue("c"))
        new Populate(execCtx, INPUT_DIR, KUDU_PARTITION_COUNT).splitCsv(scaleFactor)
      }
      case "sql" =>
        RunQueries.execute(execCtx, cmd.getOptionValue("q"))
      case "csv" => {
        if (!cmd.hasOption("c")) {
          throw new RuntimeException("Missing required arg: [-c, --scale-factor]")
        }
        if (!cmd.hasOption("i")) {
          throw new RuntimeException("Missing required arg: [-i, --inputDir]")
        }

        val scaleFactor = Integer.parseInt(cmd.getOptionValue("c"))
        val inputDir = cmd.getOptionValue("i")
        val file = new File(cmd.getOptionValue("f"))

        val users = benchMode match {
          case BenchMode.Throughput | BenchMode.All => Integer.parseInt(cmd.getOptionValue("u", s"$concurrency"))
          case _ => concurrency
        }

        val result = new Result(users, scaleFactor)

        benchMode match {
          case BenchMode.Power => executePower(result, execCtx, queryIdx, file, inputDir, skipRF, scaleFactor, direct)
          case BenchMode.Throughput => executeThroughput(result, execCtx, queryIdx, file, users, inputDir, skipRF, scaleFactor, direct)
          case BenchMode.All =>
            executePower(result, execCtx, queryIdx, file, inputDir, skipRF, scaleFactor, direct)
            executeThroughput(result, execCtx, queryIdx, file, users, inputDir, skipRF, scaleFactor, direct)
          case _ => throw new IllegalStateException()
        }

        result.record("./tpch_result")
      }
      case _ => println("first param required: must be populate, sql, or csv")
    }
  }

  def executePower(result: Result, execCtx: ExecCtx, queryIdx: String, file: File, inputDir: String, skipRF: Boolean, scaleFactor: Int, direct: Boolean): Unit = {
    println("Executing power benchmark...")
    new TpchQuery(execCtx, result, inputDir, direct, scaleFactor).executeQueries(file, queryIdx, ResultHelper.Mode.Power, skipRF)
  }

  def executeThroughput(result: Result, execCtx: ExecCtx, queryIdx: String, file: File, users: Int, inputDir: String, skipRF: Boolean, scaleFactor: Int, direct: Boolean): Unit = {
    println(s"Executing throughput benchmark... Concurrency: $users")
    val pool: ExecutorService = Executors.newFixedThreadPool(users)
    val incrementor = new AtomicInteger(0)
    val tasks = {
      for (i <- 0 to users) yield

        if (i == 0) {
          // RF thread
          new Callable[String]() {
            def call(): String = {
              println(s"Executing RF thread. ThreadNo $i")
              new TpchQuery(execCtx, result, inputDir, direct, scaleFactor).executeRFStream(users, Some(incrementor))
              println(s"RF thread $i COMPLETE.")
              "OK"
            }
          }
        } else {
          new Callable[String]() {
            def call(): String = {
              println(s"Executing Query stream thread. ThreadNo $i")
              ResultHelper.timeAndRecord(result, i, ResultHelper.Mode.ThroughputE2E) {
                new TpchQuery(execCtx, result, inputDir, direct, scaleFactor).executeQueries(file, queryIdx, ResultHelper.Mode.ThroughputQ, skipRF, i, Some(incrementor))
              }
              println(s"Query stream thread $i COMPLETE.")
              "OK"
            }
          }
        }
    }

    import scala.collection.JavaConversions._
    pool.invokeAll(tasks.toList)
    pool.shutdown()
  }
}
