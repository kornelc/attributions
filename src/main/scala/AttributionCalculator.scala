import java.io.File

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
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
  import spark.sqlContext.implicits._
  val attributions = impressions.impDF.join(events.deduped,
    $"impAdvertiserId" === $"eAdvertiserId" and $"impUserId" === $"eUserId" and  $"impEventMillies" < $"eEventMillies")
  val advertisersWithAttributedEvents = attributions
    .groupBy($"impAdvertiserId", $"eventType")
    .agg($"impAdvertiserId".as("daAdvertiserId"),
      $"eventType".as("daEventType"),
      count(col("eventId")).as("attrEventCount"))

  val distinctAdvertiserUsers = attributions
    .select($"eventType", $"impAdvertiserId", $"impUserId").distinct()
  val distinctUserCounts = distinctAdvertiserUsers
    .groupBy($"impAdvertiserId", $"eventType")
    .agg($"impAdvertiserId".as("duAdvertiserId"),
      $"eventType".as("duEventType"),
      count(col("impUserId")).as("distinctUserCount"))

  val statistics1 = impressions.impDF.select($"impAdvertiserId").distinct().alias("a")
    .join(advertisersWithAttributedEvents,
      $"a.impAdvertiserId" === $"daAdvertiserId",
      "left_outer").select($"a.impAdvertiserId".as("s1AdvertiserId"), $"attrEventCount", $"daEventType")
  val statistics2 = impressions.impDF.select($"impAdvertiserId").distinct().alias("b")
    .join(distinctUserCounts,
      $"b.impAdvertiserId" === $"duAdvertiserId",
      "left_outer").select($"b.impAdvertiserId".as("s2AdvertiserId"), $"distinctUserCount", $"duEventType")

  val statistics = statistics1.join(statistics2,
    $"s1AdvertiserId" === $"s2AdvertiserId" and ($"duEventType" === $"daEventType" or col("daEventType").isNull))
    .select($"daEventType".as("sEventType"), $"s1AdvertiserId".as("sAdvertiserId"), $"attrEventCount", $"distinctUserCount")
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
  eventsImpressions.statistics.collect.foreach( a=> println(s"Advertiser: ${a(0)} Attributed Events ${a(1)} Uqinue User: ${a(2)}"))
  saveDfToCsv(eventsImpressions.advertisersWithAttributedEvents.select($"daAdvertiserId", $"daEventType", $"attrEventCount"), "count_of_events.csv")
  saveDfToCsv(eventsImpressions.distinctUserCounts.select($"duAdvertiserId", $"duEventType", $"distinctUserCount"), "count_of_users.csv")


  def saveDfToCsv(df: DataFrame, path: String): Unit = {
    def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit)
    {
      val p = new java.io.PrintWriter(f);
      try { op(p) }
      finally { p.close() }
    }

    printToFile(new File(path)) { p => df.collect().foreach(l=>p.println(l.mkString(",")))}
  }
}

