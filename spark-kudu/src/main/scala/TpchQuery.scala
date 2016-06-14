package tpch

import java.io.{BufferedReader, File, FileReader}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.kududb.spark.kudu._

import scala.io.Source

/** Executes TPC-H analytics queries */
class TpchQuery(execCtx: ExecCtx, result: Result) {
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

  def executeQueries(file: File, queryIdx: String, mode: ResultHelper.Mode.Value, threadNo: Int = 0): Unit = {
    val lines = Source.fromFile(file).getLines().toList

    lines.indices.foreach(idx => {
      val line = lines(idx)
      if (!line.trim.startsWith("--")) {
        val t1 = System.currentTimeMillis()

        println("------------ Running query $idx")
        val q = getQuery(line)
        var cnt: Long = 0

        ResultHelper.timeAndRecord(result, q.query, mode, threadNo) {
          val df = execute(q, mode)
          df.show()
          cnt = df.count()
        }

        val t2 = System.currentTimeMillis()

        println(s"Query $idx took ${t2 - t1} ms to return $cnt rows")
      }
    })
  }

  def getQuery(l: String): QueryParams = {
    val data: Seq[String] = l.split(",").toSeq
    QueryParams(data(0).substring(1).toInt, data(1).toInt, data.slice(2, 99))
  }

  def execute(q: QueryParams, mode: ResultHelper.Mode.Value): DataFrame = {
//    val data: Seq[String] = l.split(",").toSeq
//    val q = QueryParams(data(0).substring(1).toInt, data(1).toInt, data.slice(2, 99))
    println(s"Executing: Query ${q.query} with limit ${q.limit} and params: ${q.params}")

    val res = q.query match {
      case 1 => q01(q)
      case 2 => q02(q)
      case 3 => q03(q)
      case 4 => q04(q)
      case 5 => q05(q)
      case 6 => q06(q)
      case 7 => q07(q)
      case 8 => q08(q)
      case 9 => q09(q)
      case 10 => q10(q)
      case 11 => q11(q)
      case 12 => q12(q)
      case 13 => q13(q)
      case 14 => q14(q)
      case 15 => q15(q)
      case 16 => q16(q)
      case 17 => q17(q)
      case 18 => q18(q)
      case 19 => q19(q)
      case 20 => q20(q)
      case 21 => q21(q)
      case 22 => q22(q)
      case _ => throw new RuntimeException(s"Query ${q.query} not implemented!")
    }

    q.limit match {
      case -1 => res
      case _ => res.limit(q.limit)
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
    val partSize = q.param(1)
    val partType = q.param(2)
    val regionCode = q.param(3)

    val europe = region.filter($"r_name" === regionCode)
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val brass = part.filter(part("p_size") === partSize && part("p_type").endsWith(partType))
      .join(europe, europe("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = brass.groupBy(brass("ps_partkey"))
      .agg(min("ps_supplycost").as("min"))

    val res = brass.join(minCost, brass("ps_partkey") === minCost("ps_partkey"))
      .filter(brass("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")

    res
  }

  /** Q03,10,FURNITURE,1995-03-11 */
  def q03(q: QueryParams): DataFrame = {
    val mktSegment = q.param(1)
    val date = q.param(2)

    sqlCtx.sql(
      s"""select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	`order`,
	lineitem
where
	c_mktsegment = '$mktSegment'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < cast('$date' as date)
	and l_shipdate > cast('$date' as date)
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
""")
  }

  def q04(q: QueryParams): DataFrame = {
    val startDate = q.param(1)

    sqlCtx.sql(
      s"""select
	o_orderpriority,
	count(*) as order_count
from
	`order`
where
	o_orderdate >= cast( '$startDate' as date)
	and o_orderdate < cast('$startDate' as date) + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority
"""
    )
  }

  def q05(q: QueryParams): DataFrame = {
    val regionCode = q.param(1)

    sqlCtx.sql(
      s"""select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	`order`,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = '$regionCode'
	and o_orderdate >= cast('1996-01-01' as date)
	and o_orderdate < cast('1996-01-01' as date) + interval '1' year
group by
	n_name
order by
	revenue desc"""
    )
  }

  def q06(q:QueryParams):DataFrame = {
    val startDate = q.param(1)
    sqlCtx.sql(
      s"""select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= cast('$startDate' as date)
	and l_shipdate < cast('$startDate' as date) + interval '1' year
	and l_discount between 0.08 - 0.01 and 0.08 + 0.01
	and l_quantity < 25"""
    )
  }

  def q07(q:QueryParams):DataFrame = {
    val supplyNation = q.param(1)
    val customerNation = "VIETNAM"
    // TODO: Shouldn't there be another country parameter?
    // TODO: And maybe some dates?

    val getYear = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // cache fnation

    val fnation = nation.filter($"n_name" === supplyNation || $"n_name" === customerNation)
    val fline = lineitem.filter($"l_shipdate" >= "1995-01-01" && $"l_shipdate" <= "1996-12-31")

    val supNation = fnation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(fline, $"s_suppkey" === fline("l_suppkey"))
      .select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")

    val res = fnation.join(customer, $"n_nationkey" === customer("c_nationkey"))
      .join(order, $"c_custkey" === order("o_custkey"))
      .select($"n_name".as("cust_nation"), $"o_orderkey")
      .join(supNation, $"o_orderkey" === supNation("l_orderkey"))
      .filter($"supp_nation" === supplyNation && $"cust_nation" === customerNation
        || $"supp_nation" === customerNation && $"cust_nation" === supplyNation)
      .select($"supp_nation", $"cust_nation",
        getYear($"l_shipdate").as("l_year"),
        decrease($"l_extendedprice", $"l_discount").as("volume"))
      .groupBy($"supp_nation", $"cust_nation", $"l_year")
      .agg(sum($"volume").as("revenue"))
      .sort($"supp_nation", $"cust_nation", $"l_year")

    res
  }

  def q08(q:QueryParams):DataFrame = {
    val country = q.param(1)
    val regionCode = q.param(2)
    val partType = q.param(3)

    val getYear = udf { (date: String) => date.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val countryMatch = udf { (nation: String, volume: Double) => if (nation == country) volume else 0 }

    val fregion = region.filter($"r_name" === regionCode)
    val forder = order.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === partType)

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem.select(
      $"l_partkey", $"l_suppkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume")
    )
      .join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_suppkey" === nat("s_suppkey"))

    val res = nation.join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(
        getYear($"o_orderdate").as("o_year"), $"volume",
        countryMatch($"n_name", $"volume").as("case_volume")
      )
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume")).as("mkt_share")
      .sort($"o_year")

    res
  }

  def q09(q:QueryParams):DataFrame = {
    val getYear = udf { (x: String) => x.substring(0, 4) }
    val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }
    val p1 = q.param(1)

    val linePart = part.filter($"p_name".contains(p1))
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"))

    val natSup = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val res = linePart.join(natSup, $"l_suppkey" === natSup("s_suppkey"))
      .join(partsupp, $"l_suppkey" === partsupp("ps_suppkey")
        && $"l_partkey" === partsupp("ps_partkey"))
      .join(order, $"l_orderkey" === order("o_orderkey"))
      .select($"n_name", getYear($"o_orderdate").as("o_year"),
        expr($"l_extendedprice", $"l_discount", $"ps_supplycost", $"l_quantity").as("amount"))
      .groupBy($"n_name", $"o_year")
      .agg(sum($"amount"))
      .sort($"n_name", $"o_year".desc)

    res
  }

  def q10(q:QueryParams): DataFrame = {
    sqlCtx.sql(
      s"""select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	`order`,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= cast( '${q.param(1)}' as date)
	and o_orderdate < cast('${q.param(1)}' as date) + interval '3' month
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc"""
    )
  }

  def q11(q:QueryParams): DataFrame = {
    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }

    val tmp = nation.filter($"n_name" === q.param(1))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    val res = tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)

    res
  }

  def q12(q:QueryParams): DataFrame = {
    sqlCtx.sql(
      s"""select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	`order`,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('${q.param(1)}', '${q.param(2)}')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= cast('${q.param(3)}' as date)
	and l_receiptdate < cast( '${q.param(3)}' as date) + interval '1' year
group by
	l_shipmode
order by
	l_shipmode"""
    )
  }

  def q13(q:QueryParams): DataFrame = {
    val word1 = q.param(1)
    val word2 = q.param(2)

    val regex = s".*$word1.*$word2.*"
    val special = udf { (x: String) => x.matches(regex) }

    val res = customer.join(order,
      $"c_custkey" === order("o_custkey") && !special(order("o_comment")), "leftouter"
    )
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)

    res
  }

  def q14(q:QueryParams): DataFrame = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val startDate = format.parse(q.param(1))
    val cal = Calendar.getInstance()
    cal.setTime(startDate)
    cal.add(Calendar.MONTH, 1)
    val endDate = cal.getTime
    val startText = format.format(startDate)
    val endText = format.format(endDate)

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    val res = part.join(lineitem, $"l_partkey" === $"p_partkey" &&
      $"l_shipdate" >= startText && $"l_shipdate" < endText)
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum(promo($"p_type", $"value")) * 100 / sum($"value"))

    res
  }

  def q15(q: QueryParams): DataFrame = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val startDate = format.parse(q.param(1))
    val cal = Calendar.getInstance()
    cal.setTime(startDate)
    cal.add(Calendar.MONTH, 3)
    val endDate = cal.getTime
    val startText = format.format(startDate)
    val endText = format.format(endDate)

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val revenue = lineitem.filter($"l_shipdate" >= startText &&
      $"l_shipdate" < endText)
      .select($"l_suppkey", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"l_suppkey")
      .agg(sum($"value").as("total"))
    // .cache

    val res = revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_suppkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")

    res
  }

  def q16(q: QueryParams): DataFrame = {
    val q2 = q.param(2)
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith(q2) }
    val numbers = udf { (x: Int) => x.toString().matches("19|48|41|5|13|38|15|10") }

    val fparts = part.filter(($"p_brand" !== q.param(1)) && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    val res = supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")

    res
  }

  def q17(q: QueryParams): DataFrame = {
    val mul02 = udf { (x: Double) => x * 0.2 }

    val flineitem = lineitem.select($"l_partkey", $"l_quantity", $"l_extendedprice")

    val fpart = part.filter($"p_brand" === q.param(1) && $"p_container" === q.param(2))
      .select($"p_partkey")
      .join(lineitem, $"p_partkey" === lineitem("l_partkey"), "left_outer")
    // select

    val res = fpart.groupBy("p_partkey")
      .agg(mul02(avg($"l_quantity")).as("avg_quantity"))
      .select($"p_partkey".as("key"), $"avg_quantity")
      .join(fpart, $"key" === fpart("p_partkey"))
      .filter($"l_quantity" < $"avg_quantity")
      .agg(sum($"l_extendedprice") / 7.0)

    res
  }

  def q18(q: QueryParams): DataFrame = {
    val res = lineitem.groupBy($"l_orderkey")
      .agg(sum($"l_quantity").as("sum_quantity"))
      .filter($"sum_quantity" > q.param(1))
      .select($"l_orderkey".as("key"), $"sum_quantity")
      .join(order, order("o_orderkey") === $"key")
      .join(lineitem, $"o_orderkey" === lineitem("l_orderkey"))
      .join(customer, customer("c_custkey") === $"o_custkey")
      .select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice")
      .agg(sum("l_quantity"))
      .sort($"o_totalprice".desc, $"o_orderdate")
      .limit(100)

    res
  }

  def q19(q: QueryParams): DataFrame = {
    sqlCtx.sql(
      s"""select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = '${q.param(1)}'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= ${q.param(4)} and l_quantity <= ${q.param(4)} + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = '${q.param(2)}'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= ${q.param(5)} and l_quantity <= ${q.param(5)} + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = '${q.param(3)}'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= ${q.param(6)} and l_quantity <= ${q.param(6)} + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
""")
  }

  def q20(q: QueryParams): DataFrame = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val startDate = format.parse(q.param(2))
    val cal = Calendar.getInstance()
    cal.setTime(startDate)
    cal.add(Calendar.YEAR, 1)
    val endDate = cal.getTime
    val startText = format.format(startDate)
    val endText = format.format(endDate)
    val filterWord = q.param(1)

    val forest = udf { (x: String) => x.startsWith(filterWord) }
    val flineitem = lineitem.filter($"l_shipdate" >= startText && $"l_shipdate" < endText)
      .groupBy($"l_partkey", $"l_suppkey")
      .agg((sum($"l_quantity") * 0.5).as("sum_quantity"))

    val fnation = nation.filter($"n_name" === q.param(3))
    val nat_supp = supplier.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address")
      .join(fnation, $"s_nationkey" === fnation("n_nationkey"))

    val res = part.filter(forest($"p_name"))
      .select($"p_partkey").distinct
      .join(partsupp, $"p_partkey" === partsupp("ps_partkey"))
      .join(flineitem, $"ps_suppkey" === flineitem("l_suppkey") && $"ps_partkey" === flineitem("l_partkey"))
      .filter($"ps_availqty" > $"sum_quantity")
      .select($"ps_suppkey").distinct
      .join(nat_supp, $"ps_suppkey" === nat_supp("s_suppkey"))
      .select($"s_name", $"s_address")
      .sort($"s_name")

    res
  }

  def q21(q: QueryParams): DataFrame = {
    val fsupplier = supplier.select($"s_suppkey", $"s_nationkey", $"s_name")

    val plineitem = lineitem.select($"l_suppkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")
    //cache

    val flineitem = plineitem.filter($"l_receiptdate" > $"l_commitdate")
    // cache

    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = order.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    val res = nation.filter($"n_name" === q.param(1))
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_suppkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      .join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_suppkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")
      .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
      .groupBy($"s_name")
      .agg(count($"l_suppkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .limit(100)

    res
  }

  def q22(q: QueryParams) : DataFrame = {
    val numbers = s"${q.param(1)}|${q.param(2)}|${q.param(3)}|${q.param(4)}|${q.param(5)}|${q.param(6)}|${q.param(7)}"
    val sub2 = udf { (x: String) => x.substring(0, 2) }
    val phone = udf { (x: String) => x.matches(numbers) }
    val isNull = udf { (x: Any) => println(x); true }

    val fcustomer = customer.select($"c_acctbal", $"c_custkey", sub2($"c_phone").as("cntrycode"))
      .filter(phone($"cntrycode"))

    val avg_customer = fcustomer.filter($"c_acctbal" > 0.0)
      .agg(avg($"c_acctbal").as("avg_acctbal"))

    val res = order.groupBy($"o_custkey")
      .agg($"o_custkey").select($"o_custkey")
      .join(fcustomer, $"o_custkey" === fcustomer("c_custkey"), "right_outer")
      //.filter("o_custkey is null")
      .filter($"o_custkey".isNull)
      .join(avg_customer)
      .filter($"c_acctbal" > $"avg_acctbal")
      .groupBy($"cntrycode")
      .agg(count($"c_acctbal"), sum($"c_acctbal"))
      .sort($"cntrycode")

    res
  }

  def add(d: String, unit: Int, n: Int): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
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

