package tpch

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.kududb.client._

import scala.collection.JavaConverters._

class Populate(execCtx: ExecCtx, inputDir: String) {

  val sc = execCtx.sparkCtx
  val sqlContext = execCtx.sqlCtx
  val customer = sqlContext.createDataFrame(sc.textFile(inputDir + "/customer.tbl").map(_.split('|')).map(p => Customer(
    p(0).trim.toInt,
    p(1).trim,
    p(2).trim,
    p(3).trim.toInt,
    p(4).trim,
    p(5).trim.toDouble,
    p(6).trim,
    p(7).trim
  )))
  val lineitem = sqlContext.createDataFrame(sc.textFile(inputDir + "/lineitem.tbl").map(_.split('|')).map(p => Lineitem(
    p(0).trim.toInt,
    p(1).trim.toInt,
    p(2).trim.toInt,
    p(3).trim.toInt,
    p(4).trim.toDouble,
    p(5).trim.toDouble,
    p(6).trim.toDouble,
    p(7).trim.toDouble,
    p(8).trim,
    p(9).trim,
    p(10).trim,
    p(11).trim,
    p(12).trim,
    p(13).trim,
    p(14).trim,
    p(15).trim
  )))
  val nation = sqlContext.createDataFrame(sc.textFile(inputDir + "/nation.tbl").map(_.split('|')).map(p => Nation(
    p(0).trim.toInt,
    p(1).trim,
    p(2).trim.toInt,
    p(3).trim
  )))
  val region = sqlContext.createDataFrame(sc.textFile(inputDir + "/region.tbl").map(_.split('|')).map(p => Region(
    p(0).trim.toInt,
    p(1).trim,
    p(1).trim
  )))
  val order = sqlContext.createDataFrame(sc.textFile(inputDir + "/orders.tbl").map(_.split('|')).map(p => Order(
    p(0).trim.toInt,
    p(1).trim.toInt,
    p(2).trim,
    p(3).trim.toDouble,
    p(4).trim,
    p(5).trim,
    p(6).trim,
    p(7).trim.toInt,
    p(8).trim
  )))
  val part = sqlContext.createDataFrame(sc.textFile(inputDir + "/part.tbl").map(_.split('|')).map(p => Part(
    p(0).trim.toInt,
    p(1).trim,
    p(2).trim,
    p(3).trim,
    p(4).trim,
    p(5).trim.toInt,
    p(6).trim,
    p(7).trim.toDouble,
    p(8).trim
  )))
  val partsupp = sqlContext.createDataFrame(sc.textFile(inputDir + "/partsupp.tbl").map(_.split('|')).map(p => Partsupp(
    p(0).trim.toInt,
    p(1).trim.toInt,
    p(2).trim.toInt,
    p(3).trim.toDouble,
    p(4).trim
  )))
  val supplier = sqlContext.createDataFrame(sc.textFile(inputDir + "/supplier.tbl").map(_.split('|')).map(p => Supplier(
    p(0).trim.toInt,
    p(1).trim,
    p(2).trim,
    p(3).trim.toInt,
    p(4).trim,
    p(5).trim.toDouble,
    p(6).trim
  )))

  def executeImport(): Unit = {
    importCsvToKudu(execCtx.kuduCtx, inputDir)
  }

  def importCsvToKudu(kuduContext: Broadcast[ExtendedKuduContext], inputDir: String) {
    writeToKudu(kuduContext, customer, "customer", List("c_custkey"))
    writeToKudu(kuduContext, lineitem, "lineitem", List("l_orderkey", "l_linenumber"))
    writeToKudu(kuduContext, nation, "nation", List("n_nationkey"))
    writeToKudu(kuduContext, region, "region", List("r_regionkey"))
    writeToKudu(kuduContext, order, "order", List("o_orderkey"))
    writeToKudu(kuduContext, part, "part", List("p_partkey"))
    writeToKudu(kuduContext, partsupp, "partsupp", List("ps_partkey", "ps_suppkey"))
    writeToKudu(kuduContext, supplier, "supplier", List("s_suppkey"))
  }

  def splitCsv() {
    writeToCsv(customer, "customer")
    writeToCsv(lineitem, "lineitem")
    writeToCsv(nation, "nation")
    writeToCsv(region, "region")
    writeToCsv(order, "order")
    writeToCsv(part, "part")
    writeToCsv(partsupp, "partsupp")
    writeToCsv(supplier, "supplier")
  }

  def writeToKudu(kuduContext: Broadcast[ExtendedKuduContext], df: DataFrame, tableName: String, pk: Seq[String]): Unit = {
    println(s"Importing ${df.count()} rows from $tableName with schema ${df.schema}")
    //df.show(10)
    val kc: ExtendedKuduContext = kuduContext.value
    if (kc.tableExists(tableName)) {
      kc.deleteTable(tableName)
    }
    val tableOptions = new CreateTableOptions()
      .setRangePartitionColumns(pk.asJava)
      .setNumReplicas(1) // TODO: Parameterize
    kc.createTable(tableName, df.schema, pk, tableOptions)
    kc.save(df, tableName)
  }

  def writeToCsv(df: DataFrame, tableName: String) {
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"s3n://brent-emr-test/s3test/$tableName.csv.gz")
  }

}

case class Customer(
                     c_custkey: Int,
                     c_name: String,
                     c_address: String,
                     c_nationkey: Int,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Lineitem(
                     l_orderkey: Int,
                     l_partkey: Int,
                     l_suppkey: Int,
                     l_linenumber: Int,
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     l_shipdate: String,
                     l_commitdate: String,
                     l_receiptdate: String,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Nation(
                   n_nationkey: Int,
                   n_name: String,
                   n_regionkey: Int,
                   n_comment: String)

case class Order(
                  o_orderkey: Int,
                  o_custkey: Int,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: Int,
                  o_comment: String)

case class Part(
                 p_partkey: Int,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Int,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Int,
                     ps_suppkey: Int,
                     ps_availqty: Int,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Int,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Int,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Int,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)
