package tpch

import org.scalatest.{FlatSpec, Matchers}


/**
  * Created by drewmanlove on 6/24/16.
  */
class Test extends FlatSpec with Matchers {

  /*
    * This assumes you've already executed the ./docker_setup.sh and added the /etc/hosts entry from the script's output
    */
//    "Main" should "Execute against a local docker setup" in {
//    val basedir = new java.io.File(".").getCanonicalPath()
//    println(basedir)
//    val userDir = System.getProperty("user.home")
//    println(userDir)
//
//
//
//    Main.main(Array[String](
//      "--kuduMaster", "kudu-master:7051", "--sparkMaster", "spark://kudu-master:7077",
//      "-f", s"$basedir/src/main/resources/example_queries.csv",
//      "--partitionCount", "20", "--executorMemory",
//      "1g", "-c", "1", "-u", "1", "-i", s"$basedir/src/main/resources/",
//      "-w", "-r", s"$userDir/.m2/repository",
//      "--mode", "csv"
//    ))
//  }

//  "Main" should "Execute RF2 against docker" in {
//    val basedir = new java.io.File(".").getCanonicalPath()
//    println(basedir)
//    val userDir = System.getProperty("user.home")
//    println(userDir)
//
//    val dbgenInputDir = s"$basedir/src/main/resources/"
//    val execCtx = SparkHelper.getExecContext("local[*]", "kudu-master:7051", "1g", "20", "Test", s"$userDir/.m2/repository")
//    val tpchQuery = new TpchQuery(execCtx, new Result(1, 1), dbgenInputDir)
//
//    Refresh.executeRF2(s"$basedir/src/main/resources/", 1, execCtx)
//
//  }
}
