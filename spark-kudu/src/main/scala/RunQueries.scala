package tpch

import java.io.{BufferedReader, File, FileReader}

import org.kududb.spark.kudu._

object RunQueries {

  def execute(execCtx: ExecCtx, qryfile: String): Unit = {
    val master = execCtx.kuduCtx.value.kuduMaster
    val tableNames = Array(
      "partsupp",
      "part",
      "region",
      "supplier",
      "nation",
      "order",
      "lineitem",
      "customer"
    )
    tableNames.foreach(tableName => {
      execCtx.sqlCtx.read.options(Map("kudu.master" -> master, "kudu.table" -> tableName))
        .kudu
        .registerTempTable(tableName)
    })

    val statements = scala.io.Source.fromFile(qryfile).mkString
      .split("go")
      .map(sql => sql.split(";")(0))

    statements.indices // All queries
      .foreach(idx => {
        val stmt = statements(idx)
        println(s"Starting Query Index $idx at ${new java.util.Date}")
        val start = System.currentTimeMillis()
        try {
          val df = execCtx.sqlCtx.sql(stmt)
          val count = df.count()
          val end = System.currentTimeMillis()
          println(s"Got $count records in ${end - start}ms")
          df.show()
        } catch {
          case e: Exception =>
            println(s"$stmt \n")
            e.printStackTrace
        }

      })
  }

}
