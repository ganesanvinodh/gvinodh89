import GridMapper.getCellId
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, desc, expr, window}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.row_number

object SparkApp {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkLocalUDF")
      .getOrCreate()


    val df1 = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 8787)
      .load()

    df1.isStreaming

    df1.printSchema()

//df2 to convert "value" col to required input data schema

    val df2 = df1.select(expr("(split(value, ','))[0]").cast("string").as("medallion"),expr("(split(value, ','))[1]").cast("string").as("hack_license"),expr("(split(value, ','))[2]").cast("Timestamp").as("pickup_datetime"),expr("(split(value, ','))[3]").cast("Timestamp").as("dropoff_datetime"),expr("(split(value, ','))[4]").cast("long").as("trip_time_in_secs"),expr("(split(value, ','))[5]").cast("Double").as("trip_distance"),expr("(split(value, ','))[6]").cast("Double").as("pickup_longitude"),expr("(split(value, ','))[7]").cast("Double").as("pickup_latitude"),expr("(split(value, ','))[8]").cast("Double").as("dropoff_longitude"),expr("(split(value, ','))[9]").cast("double").as("dropoff_latitude"),expr("(split(value, ','))[10]").cast("string").as("payment_type"),expr("(split(value, ','))[11]").cast("double").as("fare_amount"),expr("(split(value, ','))[12]").cast("double").as("surcharge"),expr("(split(value, ','))[13]").cast("double").as("mta_tax"),expr("(split(value, ','))[14]").cast("double").as("tip_amount"),expr("(split(value, ','))[15]").cast("double").as("tolls_amount"),expr("(split(value, ','))[16]").cast("double").as("total_amount"))


    df2.createOrReplaceTempView("input_data")

    val cell_func = (lat:Double, lon: Double) => {
      GridMapper.getCellId(lat,lon)
    }

    spark.udf.register("findCellId",cell_func)

    //df3 to create pickup_cell_id and dropoff_cell_id
    val df3 = spark.sql("select cast(findCellId(pickup_latitude,pickup_longitude) as Double) as pickup_cell_id, cast(findCellId(dropoff_latitude,dropoff_longitude) as Double) as dropoff_cell_id, * from input_data")

//df4 to filter out bad records and set the window for aggregation
    val df4 = df3.filter(col("pickup_cell_id").notEqual(0.0).and(col("dropoff_cell_id").notEqual(0.0))).groupBy(window(col("dropoff_datetime"),"30 minutes","1 second"),col("pickup_cell_id"),col("dropoff_cell_id")).count()

	df4.createOrReplaceTempView("df4");
	val df5=spark.sql("select pickup_cell_id, dropoff_cell_id, count, row_number() OVER (order by count desc) AS rank from df4").limit(10)

	df5.createOrReplaceTempView("df5");
	val df6=spark.sql("select 
					   CASE WHEN rank=1 then pickup_cell_id ELSE null END as start_cell_id_1,
					   CASE WHEN rank=1 then dropoff_cell_id ELSE null END as end_cell_id_1,
					   CASE WHEN rank=2 then pickup_cell_id ELSE null END as start_cell_id_2,
					   CASE WHEN rank=2 then dropoff_cell_id ELSE null END as end_cell_id_2,
					   CASE WHEN rank=3 then pickup_cell_id ELSE null END as start_cell_id_3,
					   CASE WHEN rank=3 then dropoff_cell_id ELSE null END as end_cell_id_3,
					   CASE WHEN rank=4 then pickup_cell_id ELSE null END as start_cell_id_4,
					   CASE WHEN rank=4 then dropoff_cell_id ELSE null END as end_cell_id_4,
					   CASE WHEN rank=5 then pickup_cell_id ELSE null END as start_cell_id_5,
					   CASE WHEN rank=5 then dropoff_cell_id ELSE null END as end_cell_id_5,
					   CASE WHEN rank=6 then pickup_cell_id ELSE null END as start_cell_id_6,
					   CASE WHEN rank=6 then dropoff_cell_id ELSE null END as end_cell_id_6,
					   CASE WHEN rank=7 then pickup_cell_id ELSE null END as start_cell_id_7,
					   CASE WHEN rank=7 then dropoff_cell_id ELSE null END as end_cell_id_7,
					   CASE WHEN rank=8 then pickup_cell_id ELSE null END as start_cell_id_8,
					   CASE WHEN rank=8 then dropoff_cell_id ELSE null END as end_cell_id_8,
					   CASE WHEN rank=9 then pickup_cell_id ELSE null END as start_cell_id_9,
					   CASE WHEN rank=9 then dropoff_cell_id ELSE null END as end_cell_id_9,
					   CASE WHEN rank=10 then pickup_cell_id ELSE null END as start_cell_id_10,
					   CASE WHEN rank=10 then dropoff_cell_id ELSE null END as end_cell_id_10
					   from df5")

	df6.createOrReplaceTempView("df6");
	val df7=spark.sql("select 
					   max(start_cell_id_1),
					   max(end_cell_id_1),
					   max(start_cell_id_2),
					   max(end_cell_id_2),
					   max(start_cell_id_3),
					   max(end_cell_id_3),
					   max(start_cell_id_4),
					   max(end_cell_id_4),
					   max(start_cell_id_5),
					   max(end_cell_id_5),
					   max(start_cell_id_6),
					   max(end_cell_id_6),
					   max(start_cell_id_7),
					   max(end_cell_id_7),
					   max(start_cell_id_8),
					   max(end_cell_id_8),
					   max(start_cell_id_9),
					   max(end_cell_id_9),
					   max(start_cell_id_10),
					   max(end_cell_id_10)
					   from df6")

    val query = df7.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate",false)
      .start()

    query.awaitTermination()

    spark.stop()
  }
}