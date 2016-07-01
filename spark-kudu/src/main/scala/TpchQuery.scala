package tpch

import java.io.{BufferedReader, File, FileReader}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.kududb.spark.kudu._

import scala.collection.mutable
import scala.io.Source

/** Executes TPC-H analytics queries */
class TpchQuery(execCtx: ExecCtx, result: Result, dbGenInputDir: String) {
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
    sqlCtx.read.options(Map("kudu.master" -> master, "kudu.table" -> tableName))
      .kudu
      .cache
      .registerTempTable(tableName)
  )

  val customer: DataFrame = sqlCtx.table("customer")
  val region: DataFrame = sqlCtx.table("region")
  val nation: DataFrame = sqlCtx.table("nation")
  val supplier: DataFrame = sqlCtx.table("supplier")
  val partsupp: DataFrame = sqlCtx.table("partsupp")
  val part: DataFrame = sqlCtx.table("part")
  val order: DataFrame = sqlCtx.table("`order`")
  val lineitem: DataFrame = sqlCtx.table("lineitem")

  def executeRFStream(users: Int, incrementor: Option[AtomicInteger]): Unit = {
    println(s"Executing Throughput RF thread with $users iterations")

    for (i <- 1 to users) {

      if (incrementor.isDefined) {
        // Schedule 1 RF pair for each 22 queries executed on query threads
        // Even distribution of RF pairs
        // TODO until the performance issue of RF2 is resolved, this spacing logic is largely useless
        while (incrementor.get.get() < ((i - 1) *22)) {
          Thread.sleep(1000)
        }
      } else {
        Thread.sleep(1000)
      }


      try {
        ResultHelper.timeAndRecord(result, 1, ResultHelper.Mode.ThroughputRF, i) {
          Refresh.executeRF1(dbGenInputDir, i, execCtx)
        }

        ResultHelper.timeAndRecord(result, 2, ResultHelper.Mode.ThroughputRF, i) {
          Refresh.executeRF2(dbGenInputDir, i, execCtx, result.sf)
        }

      } catch {
        case e: Exception =>
          println("Throughput RF Thread FAILED")
          e.printStackTrace()
      }

    }

    println(s"Completed Throughput RF thread with $users iterations")

  }

  def executeQueries(file: File,
                     queryIdx: String,
                     mode: ResultHelper.Mode.Value,
                     skipRF: Boolean,
                     threadNo: Int = 0,
                     incrementor: Option[AtomicInteger] = None): Unit = {
    val lines = Source.fromFile(file).getLines().toList

    println(s"executeQueries() threadNo=$threadNo")
    if (mode == ResultHelper.Mode.Power && !skipRF) {
      ResultHelper.timeAndRecord(result, 1, ResultHelper.Mode.PowerRF) { Refresh.executeRF1(dbGenInputDir, threadNo + 1, execCtx)}
    }

    val queries: mutable.Map[Int, QueryParams] = mutable.Map[Int, QueryParams]()
    lines.indices.foreach(idx => {
      val line = lines(idx)
      if (!line.trim.startsWith("--")) {
        val q = getQuery(line)
        queries += (q.query -> q)
      }
    })

    val ordinal = {
      if (threadNo > 41) {
        threadNo - 41
      } else {
        threadNo
      }
    }
    val it = orderedSets(ordinal).iterator
    println(s"Using query set in threadNo $threadNo : ${orderedSets(ordinal)}")

    while (it.hasNext) {
      val index = it.next()
      if(queryIdx.eq("*") || Integer.parseInt(queryIdx) == index) {
        val t1 = System.currentTimeMillis()

        println(s"------------ Running query $index")

        val q = queries(index)
        ResultHelper.timeAndRecord(result, q.query, mode, threadNo) {
          val df = execute(q, mode, incrementor)
          //df.show() Don't include show, this doubles our execution time
          index match {
            case 1 => q1(df)
            case 2 => q2(df)
            case 3 => q3(df)
            case 4 => q4(df)
            case 5 => q5(df)
            case 6 => q6(df)
            case 7 => q7(df)
            case 8 => q8(df)
            case 9 => q9(df)
            case 10 => q10(df)
            case 11 => q11(df)
            case 12 => q12(df)
            case 13 => q13(df)
            case 14 => q14(df)
            case 15 => q15(df)
            case 16 => q16(df)
            case 17 => q17(df)
            case 18 => q18(df)
            case 19 => q19(df)
            case 20 => q20(df)
            case 21 => q21(df)
            case 22 => q22(df)
          }
        }

        val t2 = System.currentTimeMillis()

        println(s"Query $index took ${t2 - t1} ms to return some rows")
      }
    }

    if (mode == ResultHelper.Mode.Power && !skipRF) {
      ResultHelper.timeAndRecord(result, 2, ResultHelper.Mode.PowerRF) { Refresh.executeRF2(dbGenInputDir, threadNo + 1, execCtx, result.sf) }
    }
  }

  def q1(df: DataFrame): Unit = {
    df.show()
  }

  def q2(df: DataFrame): Unit = {
    df.show()
  }

  def q3(df: DataFrame): Unit = {
    df.show()
  }

  def q4(df: DataFrame): Unit = {
    df.show()
  }

  def q5(df: DataFrame): Unit = {
    df.show()
  }

  def q6(df: DataFrame): Unit = {
    df.show()
  }

  def q7(df: DataFrame): Unit = {
    df.show()
  }

  def q8(df: DataFrame): Unit = {
    df.show()
  }

  def q9(df: DataFrame): Unit = {
    df.show()
  }

  def q10(df: DataFrame): Unit = {
    df.show()
  }

  def q11(df: DataFrame): Unit = {
    df.show()
  }

  def q12(df: DataFrame): Unit = {
    df.show()
  }

  def q13(df: DataFrame): Unit = {
    df.show()
  }

  def q14(df: DataFrame): Unit = {
    df.show()
  }

  def q15(df: DataFrame): Unit = {
    df.show()
  }

  def q16(df: DataFrame): Unit = {
    df.show()
  }

  def q17(df: DataFrame): Unit = {
    df.show()
  }

  def q18(df: DataFrame): Unit = {
    df.show()
  }

  def q19(df: DataFrame): Unit = {
    df.show()
  }

  def q20(df: DataFrame): Unit = {
    df.show()
  }

  def q21(df: DataFrame): Unit = {
    df.show()
  }

  def q22(df: DataFrame): Unit = {
    df.show()
  }

  def getQuery(l: String): QueryParams = {
    val data: Seq[String] = l.split(",").toSeq
    QueryParams(data(0).substring(1).toInt, data(1).toInt, data.slice(2, 99))
  }

  def execute(q: QueryParams, mode: ResultHelper.Mode.Value, incrementor: Option[AtomicInteger]): DataFrame = {
//    val data: Seq[String] = l.split(",").toSeq
//    val q = QueryParams(data(0).substring(1).toInt, data(1).toInt, data.slice(2, 99))
    println(s"Executing: Query ${q.query} with limit ${q.limit} and params: ${q.params}")

    if (incrementor.isDefined) {
      val inc = incrementor.get.getAndIncrement()
      println(s"Progressing counter to $inc")
    }

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

  // See http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
  // Appendix: A: Ordered Sets
  // 0 is power
  // 1 - 40 is throughput
  // users > 40 loop back to 0
  val orderedSets: Map[Int, mutable.LinkedHashSet[Int]] = Map(
    0 -> mutable.LinkedHashSet(14, 2, 9, 20, 6, 17, 18, 8, 21, 13, 3, 22, 16, 4, 11, 15, 1, 10, 19, 5, 7, 12),
    1 -> mutable.LinkedHashSet(21, 3, 18, 5, 11, 7, 6, 20, 17, 12, 16, 15, 13, 10, 2, 8, 14, 19, 9, 22, 1, 4),
    2 -> mutable.LinkedHashSet(6, 17, 14, 16, 19, 10, 9, 2, 15, 8, 5, 22, 12, 7, 13, 18, 1, 4, 20, 3, 11, 21),
    3 -> mutable.LinkedHashSet(8, 5, 4, 6, 17, 7, 1, 18, 22, 14, 9, 10, 15, 11, 20, 2, 21, 19, 13, 16, 12, 3),
    4 -> mutable.LinkedHashSet(5, 21, 14, 19, 15, 17, 12, 6, 4, 9, 8, 16, 11, 2, 10, 18, 1, 13, 7, 22, 3, 20),
    5 -> mutable.LinkedHashSet(21, 15, 4, 6, 7, 16, 19, 18, 14, 22, 11, 13, 3, 1, 2, 5, 8, 20, 12, 17, 10, 9),
    6 -> mutable.LinkedHashSet(10, 3, 15, 13, 6, 8, 9, 7, 4, 11, 22, 18, 12, 1, 5, 16, 2, 14, 19, 20, 17, 21),
    7 -> mutable.LinkedHashSet(18, 8, 20, 21, 2, 4, 22, 17, 1, 11, 9, 19, 3, 13, 5, 7, 10, 16, 6, 14, 15, 12),
    8 -> mutable.LinkedHashSet(19, 1, 15, 17, 5, 8, 9, 12, 14, 7, 4, 3, 20, 16, 6, 22, 10, 13, 2, 21, 18, 11),
    9 -> mutable.LinkedHashSet(8, 13, 2, 20, 17, 3, 6, 21, 18, 11, 19, 10, 15, 4, 22, 1, 7, 12, 9, 14, 5, 16),
    10 -> mutable.LinkedHashSet(6, 15, 18, 17, 12, 1, 7, 2, 22, 13, 21, 10, 14, 9, 3, 16, 20, 19, 11, 4, 8, 5),
    11 -> mutable.LinkedHashSet(15, 14, 18, 17, 10, 20, 16, 11, 1, 8, 4, 22, 5, 12, 3, 9, 21, 2, 13, 6, 19, 7),
    12 -> mutable.LinkedHashSet(1, 7, 16, 17, 18, 22, 12, 6, 8, 9, 11, 4, 2, 5, 20, 21, 13, 10, 19, 3, 14, 15),
    13 -> mutable.LinkedHashSet(21, 17, 7, 3, 1, 10, 12, 22, 9, 16, 6, 11, 2, 4, 5, 14, 8, 20, 13, 18, 15, 19),
    14 -> mutable.LinkedHashSet(2, 9, 5, 4, 18, 1, 20, 15, 16, 17, 7, 21, 13, 14, 19, 8, 22, 11, 10, 3, 12, 6),
    15 -> mutable.LinkedHashSet(16, 9, 17, 8, 14, 11, 10, 12, 6, 21, 7, 3, 15, 5, 22, 20, 1, 13, 19, 2, 4, 18),
    16 -> mutable.LinkedHashSet(1, 3, 6, 5, 2, 16, 14, 22, 17, 20, 4, 9, 10, 11, 15, 8, 12, 19, 18, 13, 7, 21),
    17 -> mutable.LinkedHashSet(3, 16, 5, 11, 21, 9, 2, 15, 10, 18, 17, 7, 8, 19, 14, 13, 1, 4, 22, 20, 6, 12),
    18 -> mutable.LinkedHashSet(14, 4, 13, 5, 21, 11, 8, 6, 3, 17, 2, 20, 1, 19, 10, 9, 12, 18, 15, 7, 22, 16),
    19 -> mutable.LinkedHashSet(4, 12, 22, 14, 5, 15, 16, 2, 8, 10, 17, 9, 21, 7, 3, 6, 12, 18, 11, 20, 19, 1),
    20 -> mutable.LinkedHashSet(16, 15, 14, 13, 4, 22, 18, 19, 7, 1, 12, 17, 5, 10, 20, 3, 9, 21, 11, 2, 6, 8),
    21 -> mutable.LinkedHashSet(20, 14, 21, 12, 15, 17, 4, 19, 13, 10, 11, 1, 16, 5, 18, 7, 8, 22, 9, 6, 3, 2),
    22 -> mutable.LinkedHashSet(16, 14, 13, 2, 21, 10, 11, 4, 1, 22, 18, 12, 19, 5, 7, 8, 6, 3, 15, 20, 9, 17),
    23 -> mutable.LinkedHashSet(18, 15, 9, 14, 12, 2, 8, 11, 22, 21, 16, 1, 6, 17, 5, 10, 19, 4, 20, 13, 3, 7),
    24 -> mutable.LinkedHashSet(7, 3, 10, 14, 13, 21, 18, 6, 20, 4, 9, 8, 22, 15, 2, 1, 5, 12, 19, 17, 11, 16),
    25 -> mutable.LinkedHashSet(18, 1, 13, 7, 16, 10, 14, 2, 19, 5, 21, 11, 22, 15, 8, 17, 20, 3, 4, 12, 6, 9),
    26 -> mutable.LinkedHashSet(13, 2, 22, 5, 11, 21, 20, 14, 7, 10, 4, 9, 19, 18, 6, 3, 1, 8, 15, 12, 17, 16),
    27 -> mutable.LinkedHashSet(14, 17, 21, 8, 2, 9, 6, 4, 5, 13, 22, 7, 15, 3, 1, 18, 16, 11, 10, 12, 20, 19),
    28 -> mutable.LinkedHashSet(10, 22, 1, 12, 13, 18, 21, 20, 2, 14, 16, 7, 15, 3, 4, 17, 5, 19, 6, 8, 9, 11),
    29 -> mutable.LinkedHashSet(10, 8, 9, 18, 12, 6, 1, 5, 20, 11, 17, 22, 16, 3, 13, 2, 15, 21, 14, 19, 7, 4),
    30 -> mutable.LinkedHashSet(7, 17, 22, 5, 3, 10, 13, 18, 9, 1, 14, 15, 21, 19, 16, 12, 8, 6, 11, 20, 4, 2),
    31 -> mutable.LinkedHashSet(2, 9, 21, 3, 4, 7, 1, 11, 16, 5, 20, 19, 18, 8, 17, 13, 10, 12, 15, 6, 14, 22),
    32 -> mutable.LinkedHashSet(15, 12, 8, 4, 22, 13, 16, 17, 18, 3, 7, 5, 6, 1, 9, 11, 21, 10, 14, 20, 19, 2),
    33 -> mutable.LinkedHashSet(15, 16, 2, 11, 17, 7, 5, 14, 20, 4, 21, 3, 10, 9, 12, 8, 13, 6, 18, 19, 22, 1),
    34 -> mutable.LinkedHashSet(1, 13, 11, 3, 4, 21, 6, 14, 15, 22, 18, 9, 7, 5, 10, 20, 12, 16, 17, 8, 19, 2),
    35 -> mutable.LinkedHashSet(14, 17, 22, 20, 8, 16, 5, 10, 1, 13, 2, 21, 12, 9, 4, 18, 3, 7, 6, 19, 15, 11),
    36 -> mutable.LinkedHashSet(9, 17, 7, 4, 5, 13, 21, 18, 11, 3, 22, 1, 6, 16, 20, 14, 15, 10, 8, 2, 12, 19),
    37 -> mutable.LinkedHashSet(13, 14, 5, 22, 19, 11, 9, 6, 18, 15, 8, 10, 7, 4, 17, 16, 3, 1, 12, 2, 21, 20),
    38 -> mutable.LinkedHashSet(20, 5, 4, 14, 11, 1, 6, 16, 8, 22, 7, 3, 2, 12, 21, 19, 17, 13, 10, 15, 18, 9),
    39 -> mutable.LinkedHashSet(3, 7, 14, 15, 6, 5, 21, 20, 18, 10, 4, 16, 19, 1, 13, 9, 8, 17, 11, 12, 22, 2),
    40 -> mutable.LinkedHashSet(13, 15, 17, 1, 22, 11, 3, 4, 7, 20, 14, 21, 9, 8, 2, 18, 16, 6, 10, 12, 5, 19)
  )
}

