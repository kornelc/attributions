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
  private val eventsDF = spark.createDataFrame(eventsCSV, eventsSchema)
  private val toInt = udf((i: String) => i.toInt)
  private val eventsWithMinDF = eventsDF
    .withColumn("eEventMillies", toInt(col("eTimestamp")))
  private val window = Window.partitionBy("eAdvertiserId", "eUserId", "eventType").orderBy("eEventMillies")
  val deduped = eventsWithMinDF
    .withColumn( "lag", lag("eEventMillies", 1).over(window))
    .withColumn( "diff", col("eEventMillies")-col("lag") )
    .filter(col("diff").isNull or col("diff") > 60000)
}

class Impression(path: String)(implicit spark: SparkSession){
  private val impCSV = spark.read.format("com.databricks.spark.csv").load(path).rdd
  private val impSchema = Encoders.product[ImpressionRecord].schema
  private val toInt = udf((i: String) => i.toInt)
  private val impDFRaw = spark.createDataFrame(impCSV, impSchema)
  val impDF = impDFRaw.withColumn("impEventMillies", toInt(col("impTimestamp")))
}

class ImpressionEvent(events: Event, impressions: Impression)(implicit spark: SparkSession){
  events.deduped.show(10)
  impressions.impDF.show(10)
  import spark.sqlContext.implicits._
  val attributions = impressions.impDF.join(events.deduped,
    $"impAdvertiserId" === $"eAdvertiserId" and $"impUserId" === $"eUserId" and  $"impEventMillies" < $"eEventMillies")

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

