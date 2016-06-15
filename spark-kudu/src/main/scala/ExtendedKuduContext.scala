package tpch

import java.util

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import org.kududb.client.SessionConfiguration.FlushMode
import org.kududb.{ColumnSchema, Schema, Type}
import org.kududb.client._
import org.kududb.spark.kudu.KuduContext

case class ExtendedKuduContext(kuduMaster: String) extends KuduContext(kuduMaster) {

  override def tableExists(tableName: String) = syncClient.tableExists(tableName)

  override def deleteTable(tableName: String) = syncClient.deleteTable(tableName)

  def createTable(tableName: String, schema: StructType, key: Seq[Int], options: CreateTableOptions): Unit = {
    val kuduCols = new util.ArrayList[ColumnSchema]()
    // add the key columns first, in the order specified
    for (idx <- key) {
      val f = schema.fields(idx)
      kuduCols.add(new ColumnSchema.ColumnSchemaBuilder(f.name, kuduType(f.dataType)).key(true).build())
    }
    // now add the non-key columns
    for (idx <- schema.fields.indices.filter(i => !key.contains(i))) {
      val f = schema.fields(idx)
      kuduCols.add(new ColumnSchema.ColumnSchemaBuilder(f.name, kuduType(f.dataType)).key(false).build())
    }

    syncClient.createTable(tableName, new Schema(kuduCols), options)
  }

  /** Save the contents of a DataFrame into an existing Kudu table. */
  def save(df: DataFrame, tableName: String): Unit = {

    // Populate
    df.foreachPartition((rows: Iterator[Row]) => {
      val session: KuduSession = syncClient.newSession
      try {
        session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)
        session.setFlushInterval(100)
        val table: KuduTable = syncClient.openTable(tableName)
        rows.foreach((dfRow: Row) => {
          val insert: Insert = table.newInsert
          val kuduRow: PartialRow = insert.getRow
          for (idx <- dfRow.schema.indices) {
            if (dfRow.isNullAt(idx)) {
              kuduRow.setNull(idx)
            } else {
              val dt: DataType = dfRow.schema.fields(idx).dataType
              dt match {
                case DataTypes.StringType => kuduRow.addString(idx, dfRow.getAs[String](idx))
                case DataTypes.BinaryType => kuduRow.addBinary(idx, dfRow.getAs[Array[Byte]](idx))
                case DataTypes.BooleanType => kuduRow.addBoolean(idx, dfRow.getAs[Boolean](idx))
                case DataTypes.ByteType => kuduRow.addInt(idx, dfRow.getAs[Byte](idx))
                case DataTypes.ShortType => kuduRow.addInt(idx, dfRow.getAs[Short](idx))
                case DataTypes.IntegerType => kuduRow.addInt(idx, dfRow.getAs[Int](idx))
                case DataTypes.LongType => kuduRow.addLong(idx, dfRow.getAs[Long](idx))
                case DataTypes.FloatType => kuduRow.addFloat(idx, dfRow.getAs[Float](idx))
                case DataTypes.DoubleType => kuduRow.addDouble(idx, dfRow.getAs[Double](idx))
                case _ => throw new RuntimeException(s"No support for Spark SQL type $dt")
              }
            }
          }
          session.apply(insert)
        })
      } finally {
        session.close()
      }
    })
  }

  def getSession(): KuduSession = {
    syncClient.newSession()
  }
  def insert(row: Row, tableName: String, session: KuduSession): Unit = {
    val table: KuduTable = syncClient.openTable(tableName)
    val insert: Insert = table.newInsert
    val kuduRow: PartialRow = insert.getRow
    for (idx <- row.schema.indices) {
      if (row.isNullAt(idx)) {
        kuduRow.setNull(idx)
      } else {
        val dt: DataType = row.schema.fields(idx).dataType
        dt match {
          case DataTypes.StringType => kuduRow.addString(idx, row.getAs[String](idx))
          case DataTypes.BinaryType => kuduRow.addBinary(idx, row.getAs[Array[Byte]](idx))
          case DataTypes.BooleanType => kuduRow.addBoolean(idx, row.getAs[Boolean](idx))
          case DataTypes.ByteType => kuduRow.addInt(idx, row.getAs[Byte](idx))
          case DataTypes.ShortType => kuduRow.addInt(idx, row.getAs[Short](idx))
          case DataTypes.IntegerType => kuduRow.addInt(idx, row.getAs[Int](idx))
          case DataTypes.LongType => kuduRow.addLong(idx, row.getAs[Long](idx))
          case DataTypes.FloatType => kuduRow.addFloat(idx, row.getAs[Float](idx))
          case DataTypes.DoubleType => kuduRow.addDouble(idx, row.getAs[Double](idx))
          case _ => throw new RuntimeException(s"No support for Spark SQL type $dt")
        }
      }
    }
    session.apply(insert)
  }

  def delete(): Unit = {

  }

  /** Map Spark SQL type to Kudu type */
  override def kuduType(dt: DataType) : Type = dt match {
    case DataTypes.BinaryType => Type.BINARY
    case DataTypes.BooleanType => Type.BOOL
    case DataTypes.StringType => Type.STRING
    case DataTypes.TimestampType => Type.TIMESTAMP
    case DataTypes.ByteType => Type.INT8
    case DataTypes.ShortType => Type.INT16
    case DataTypes.IntegerType => Type.INT32
    case DataTypes.LongType => Type.INT64
    case DataTypes.FloatType => Type.FLOAT
    case DataTypes.DoubleType => Type.DOUBLE
    case _ => throw new RuntimeException(s"No support for Spark SQL type $dt")
  }


}
