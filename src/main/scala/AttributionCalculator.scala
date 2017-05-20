import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

case class EventRecord(eTimestamp: String, eventId: String, eAdvertiserId: String, eUserId: String, eventType: String)
case class ImpressionRecord(impTimestamp: String, impAdvertiserId: String, creativeId: String, impUserId: String)

class Event(path: String)(implicit spark: SparkSession){
  private val eventsCSV = spark.read.format("com.databricks.spark.csv").load(path).rdd

  private val eventsSchema = Encoders.product[EventRecord].schema
  val eventsDF = spark.createDataFrame(eventsCSV, eventsSchema)
  private val toInt = udf((i: String) => i.toInt)
  val eventsWithMinDF = eventsDF
    .withColumn("eventMillies", toInt(col("eTimestamp")))
  val w2 = Window.partitionBy("eAdvertiserId", "eUserId", "eventType").orderBy("eventMillies")
  val withLagsDF2 = eventsWithMinDF
    .withColumn( "lag", lag("eventMillies", 1).over(w2))
    .withColumn( "diff", col("eventMillies")-col("lag") )
    .filter(col("diff").isNull or col("diff") > 60)
}

class Impression(path: String)(implicit spark: SparkSession){
  private val impCSV = spark.read.format("com.databricks.spark.csv").load(path).rdd
  private val impSchema = Encoders.product[ImpressionRecord].schema
  val impDF = spark.createDataFrame(impCSV, impSchema)
  impDF.createOrReplaceTempView("impressions")
}

class ImpressionEvent(events: Event, impressions: Impression)(implicit spark: SparkSession){
  import spark.sqlContext.implicits._
  val attributions = impressions.impDF.join(events.eventsWithMinDF,
    $"impAdvertiserId" === $"eAdvertiserId" and $"impUserId" === $"eUserId" and $"impTimestamp" < $"latestTimestampInMinute")

}
object AttributionCalculator extends App{

  implicit val spark = SparkSession.builder
    .appName("sparkexample")
    .master("local[*]")
    .getOrCreate
  import spark.sqlContext.implicits._

  val events = new Event("src/main/resources/events.csv")
  val impressions =  new Impression("src/main/resources/impressions.csv")
  val eventsImpressions = new ImpressionEvent(events, impressions)
  eventsImpressions.attributions.toDF().show(3)
  eventsImpressions.attributions.groupBy("impAdvertiserId").count().show(100)
  eventsImpressions.attributions.groupBy("impAdvertiserId", "impUserId", "eventType").count().show(100)
}

