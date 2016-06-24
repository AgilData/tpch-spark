package tpch
import java.io.File

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by drewmanlove on 6/24/16.
  */
object SparkHelper {

  def getExecContext(sparkMaster: String,
                     kuduMaster: String,
                      execMem: String,
                      partitionCount: String,
                      className: String,
                      mvnRepoPath: String
                     ): ExecCtx = {
    val conf = new SparkConf()
      .setMaster(sparkMaster)
      .setAppName("TPC-H " + className)
      .setExecutorEnv("spark.executor.memory", execMem)
      .setExecutorEnv("spark.sql.tungsten.enabled", "true")
      .setExecutorEnv("spark.sql.shuffle.partitions", partitionCount)
      .setExecutorEnv("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", execMem)
      .set("spark.sql.tungsten.enabled", "true")
      .set("spark.sql.shuffle.partitions", partitionCount)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.metrics.conf", "/mnt/data/spark/conf/metrics.properties")

    val sparkCtx = new SparkContext(conf)

    sparkCtx.addJar(s"$mvnRepoPath/org/kududb/kudu-spark_2.11/1.0.0-SNAPSHOT/kudu-spark_2.11-1.0.0-SNAPSHOT.jar")
    sparkCtx.addJar(s"${new File(".").getCanonicalPath}/target/scala-2.11/spark-tpc-h-queries_2.11-1.1-SNAPSHOT.jar")
    //sparkCtx.addFile("/mnt/data/spark/conf/metrics.properties")
    val sqlCtx = new org.apache.spark.sql.SQLContext(sparkCtx)
    val kuduCtx = sparkCtx.broadcast(new ExtendedKuduContext(kuduMaster))
    ExecCtx(sparkCtx, sqlCtx, kuduCtx)
  }
}

case class ExecCtx(sparkCtx: SparkContext, sqlCtx: SQLContext, kuduCtx: Broadcast[ExtendedKuduContext])

