package tpch

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
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

  def executeIngest(scaleFactor: Int): Unit = {
    importGzToKudu(execCtx.kuduCtx, scaleFactor)
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

  def importGzToKudu(kuduContext: Broadcast[ExtendedKuduContext], scaleFactor: Int) {
    val s3Root = s"s3n://brent-emr-test/tpch/${scaleFactor}x"

    val customerSchema = StructType(Array(
      StructField("c_custkey", IntegerType, false),
      StructField("c_name", StringType),
      StructField("c_address", StringType),
      StructField("c_nationkey", IntegerType),
      StructField("c_phone", StringType),
      StructField("c_acctbal", DoubleType),
      StructField("c_mktsegment", StringType),
      StructField("c_comment", StringType)
    ))
    val lineItemSchema = StructType(Array(
      StructField("l_orderkey", IntegerType, false),
      StructField("l_partkey", IntegerType, false),
      StructField("l_suppkey", IntegerType),
      StructField("l_linenumber", IntegerType),
      StructField("l_quantity", DoubleType),
      StructField("l_extendedprice", DoubleType),
      StructField("l_discount", DoubleType),
      StructField("l_tax", DoubleType),
      StructField("l_returnflag", StringType),
      StructField("l_linestatus", StringType),
      StructField("l_shipdate", StringType),
      StructField("l_commitdate", StringType),
      StructField("l_receiptdate", StringType),
      StructField("l_shipinstruct", StringType),
      StructField("l_shipmode", StringType),
      StructField("l_comment", StringType)
    ))
    val nationSchema = StructType(Array(
      StructField("n_nationkey", IntegerType, false),
      StructField("n_name", StringType),
      StructField("n_regionkey", IntegerType),
      StructField("n_comment", StringType)
    ))
    val regionSchema = StructType(Array(
      StructField("r_regionkey", IntegerType, false),
      StructField("r_name", StringType),
      StructField("r_comment", StringType)
    ))
    val orderSchema = StructType(Array(
      StructField("o_orderkey", IntegerType, false),
      StructField("o_custkey", IntegerType),
      StructField("o_orderstatus", StringType),
      StructField("o_totalprice", DoubleType),
      StructField("o_orderdate", StringType),
      StructField("o_orderpriority", StringType),
      StructField("o_clerk", StringType),
      StructField("o_shippriority", IntegerType),
      StructField("o_comment", StringType)
    ))
    val partSchema = StructType(Array(
      StructField("p_partkey", IntegerType, false),
      StructField("p_name", StringType),
      StructField("p_mfgr", StringType),
      StructField("p_brand", StringType),
      StructField("p_type", StringType),
      StructField("p_size", IntegerType),
      StructField("p_container", StringType),
      StructField("p_retailprice", DoubleType),
      StructField("p_comment", StringType)
    ))
    val partSuppSchema = StructType(Array(
      StructField("ps_partkey", IntegerType, false),
      StructField("ps_suppkey", IntegerType, false),
      StructField("ps_availqty", IntegerType),
      StructField("ps_supplycost", DoubleType),
      StructField("ps_comment", StringType)
    ))
    val supplierSchema = StructType(Array(
      StructField("s_suppkey", IntegerType, false),
      StructField("s_name", StringType),
      StructField("s_address", StringType),
      StructField("s_nationkey", IntegerType),
      StructField("s_phone", StringType),
      StructField("s_acctbal", DoubleType),
      StructField("s_comment", StringType)
    ))

    val customer = sqlContext.read.format("com.databricks.spark.csv").schema(customerSchema).option("header", "true").load(s"$s3Root/customer.csv.gz")
    val lineitem = sqlContext.read.format("com.databricks.spark.csv").schema(lineItemSchema).option("header", "true").load(s"$s3Root/lineitem.csv.gz")
    val nation = sqlContext.read.format("com.databricks.spark.csv").schema(nationSchema).option("header", "true").load(s"$s3Root/nation.csv.gz")
    val region = sqlContext.read.format("com.databricks.spark.csv").schema(regionSchema).option("header", "true").load(s"$s3Root/region.csv.gz")
    val order = sqlContext.read.format("com.databricks.spark.csv").schema(orderSchema).option("header", "true").load(s"$s3Root/order.csv.gz")
    val part = sqlContext.read.format("com.databricks.spark.csv").schema(partSchema).option("header", "true").load(s"$s3Root/part.csv.gz")
    val partsupp = sqlContext.read.format("com.databricks.spark.csv").schema(partSuppSchema).option("header", "true").load(s"$s3Root/partsupp.csv.gz")
    val supplier = sqlContext.read.format("com.databricks.spark.csv").schema(supplierSchema).option("header", "true").load(s"$s3Root/supplier.csv.gz")

    writeToKudu(kuduContext, customer, "customer", List("c_custkey"))
    writeToKudu(kuduContext, lineitem, "lineitem", List("l_orderkey", "l_linenumber"))
    writeToKudu(kuduContext, nation, "nation", List("n_nationkey"))
    writeToKudu(kuduContext, region, "region", List("r_regionkey"))
    writeToKudu(kuduContext, order, "order", List("o_orderkey"))
    writeToKudu(kuduContext, part, "part", List("p_partkey"))
    writeToKudu(kuduContext, partsupp, "partsupp", List("ps_partkey", "ps_suppkey"))
    writeToKudu(kuduContext, supplier, "supplier", List("s_suppkey"))
  }

  def splitCsv(scaleFactor: Int) {
    writeToCsv(customer, "customer", scaleFactor)
    writeToCsv(lineitem, "lineitem", scaleFactor)
    writeToCsv(nation, "nation", scaleFactor)
    writeToCsv(region, "region", scaleFactor)
    writeToCsv(order, "order", scaleFactor)
    writeToCsv(part, "part", scaleFactor)
    writeToCsv(partsupp, "partsupp", scaleFactor)
    writeToCsv(supplier, "supplier", scaleFactor)
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

  def writeToCsv(df: DataFrame, tableName: String, scaleFactor: Int) {
    df.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"s3n://brent-emr-test/tpch/${scaleFactor}x/$tableName.csv.gz")
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
