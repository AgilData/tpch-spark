package tpch

import java.io.{BufferedReader, File, FileReader}
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.kududb.spark.kudu._

/** Executes TPC-H analytics queries */
class TpchQuery(execCtx: ExecCtx) {
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

  private val sqlCtx: SQLContext = execCtx.sqlCtx
  import sqlCtx.implicits._

  tableNames.foreach(tableName =>
    sqlCtx.read.options(Map("kudu.master" -> master, "kudu.table" -> tableName)).kudu.registerTempTable(tableName))

  val customer: DataFrame = sqlCtx.table("customer")
  val region: DataFrame = sqlCtx.table("region")
  val nation: DataFrame = sqlCtx.table("nation")
  val supplier: DataFrame = sqlCtx.table("supplier")
  val partsupp: DataFrame = sqlCtx.table("partsupp")
  val part: DataFrame = sqlCtx.table("part")
  val order: DataFrame = sqlCtx.table("`order`")
  val lineitem: DataFrame = sqlCtx.table("lineitem")

  def executeQueries(file: File): Unit = {
    println(s"file=" + file.getAbsolutePath)
    val r = new BufferedReader(new FileReader(file))
    var l = r.readLine()
    while (l != null) {
      try {
        val df = execute(l)
        df.show(10)

        //TODO: collect full results from df
      } catch {
        case e: Exception => e.printStackTrace()
      }

      l = r.readLine()
    }
    r.close()
  }

  def execute(l: String): DataFrame = {
    val data: Seq[String] = l.split(",").toSeq
    val q = QueryParams(data(0).substring(1).toInt, data(1).toInt, data.slice(2, 99))
    println(s"Executing: Query ${q.query} with limit ${q.limit} and params: ${q.params}")

    q.query match {
      case 1 => q01(q)
      case 2 => q02(q)
      case 3 => q03(q)
      case 4 => q04(q)
      case _ => throw new RuntimeException(s"Query ${q.query} not implemented!")
    }
  }

  def q01(q: QueryParams): DataFrame = {
    sqlCtx.sql(
      s"""select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
    from
            lineitem
    where
            l_shipdate <= cast('1998-12-01' as date) - interval ${q.param(1)} day
    group by
            l_returnflag,
            l_linestatus
    order by
            l_returnflag,
            l_linestatus""")
  }

  def q02(q: QueryParams): DataFrame = {
    val europe = region.filter($"r_name" === q.param(3))
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === q.param(1) && part("p_type").endsWith(q.param(2)))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    val res = brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(q.limit)

    res
  }

  /** Q03,10,FURNITURE,1995-03-11 */
  def q03(q: QueryParams): DataFrame = {

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val fcust = customer.filter($"c_mktsegment" === q.param(1))
    val forders = order.filter($"o_orderdate" < q.param(2))
    val flineitems = lineitem.filter($"l_shipdate" > q.param(2))

    val res = fcust.join(forders, $"c_custkey" === forders("o_custkey"))
      .select($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .join(flineitems, $"o_orderkey" === flineitems("l_orderkey"))
      .select($"l_orderkey",
        decrease($"l_extendedprice", $"l_discount").as("volume"),
        $"o_orderdate", $"o_shippriority")
      .groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum($"volume").as("revenue"))
      .sort($"revenue".desc, $"o_orderdate")
      .limit(q.limit)

    res
  }

  def q04(q: QueryParams): DataFrame = {

    val forders = order.filter($"o_orderdate" >= q.param(1) && $"o_orderdate" < add(q.param(1), Calendar.MONTH, 3))
    val flineitems = lineitem.filter($"l_commitdate" < $"l_receiptdate")
      .select($"l_orderkey")
      .distinct

    val res = flineitems.join(forders, $"l_orderkey" === forders("o_orderkey"))
      .groupBy($"o_orderpriority")
      .agg(count($"o_orderpriority"))
      .sort($"o_orderpriority")

    res
  }

  def q05(q: QueryParams): Unit = {
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = order.filter($"o_orderdate" < "1995-01-01" && $"o_orderdate" >= "1994-01-01")

    val res = region.filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem, $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders, $"l_orderkey" === forders("o_orderkey"))
      .join(customer, $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select($"n_name", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
  }

  def add(d: String, unit: Int, n: Int): String = {
    val df = new SimpleDateFormat("yyyy-mm-dd")
    val d1 = df.parse(d)
    val c = Calendar.getInstance()
    c.setTime(d1)
    c.add(unit, n)
    val d2 = c.getTime
    df.format(d2)
  }

  case class QueryParams(query: Int, limit: Int, params: Seq[String]) {
    def param(p: Int) = params(p-1)
  }
}
