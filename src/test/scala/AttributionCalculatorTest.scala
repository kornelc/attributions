import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class AttributionCalculatorTest extends FlatSpec with Matchers{
  case class EventRecord(eTimestamp: String, eEventId: String, eAdvertiserId: String, eUserId: String, eventType: String)
  case class ImpressionRecord(impTimestamp: String, impAdvertiserId: String, impCreativeId: String, impUserId: String)

  private val master = "local[2]"
  private val appName = "Example1Test"
  implicit private val spark = SparkSession.builder
    .appName(appName)
    .master(master)
    .enableHiveSupport
    .getOrCreate
  import spark.sqlContext.implicits._

  private def prepareForDedDupeTest(s: Dataset[Row]) = {
    s.collect.map(_.toSeq).sortWith( (_(5).toString.toInt < _(5).toString.toInt))
  }
  private def prepareForAttributionTest(s: Dataset[Row]) = {
    s.collect.map(_.toSeq).sortWith( (_(0).toString.toInt < _(0).toString.toInt))
  }
  private def prepareForStatisticsTest(s: Dataset[Row]) = {
    s.collect.map(_.toSeq).sortWith( (_.take(2).mkString < _.take(2).mkString))
  }

  "AttributionCalculator" should "should remove duplicate events for adv/user/eventtype within minute. Use last timestamp instead" in {
    val events = new Event("src/test/resources/testevents-sameu-samea-samem-samee.csv")
    val noDupes = prepareForDedDupeTest(events.deduped)
    noDupes.size shouldEqual 1
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
  }

  it should "should not remove duplicate events for when advertizer is different" in {
    val events = new Event("src/test/resources/testevents-sameu-diffa-samem-samee.csv")
    val noDupes = prepareForDedDupeTest(events.deduped)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60002", "ev2", "a2", "u1", "click", 60002, null, null)
  }

  it should "should not remove duplicate events for when user is different" in {
    val events = new Event("src/test/resources/testevents-diffu-samea-samem-samee.csv")
    val noDupes = prepareForDedDupeTest(events.deduped)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60003", "ev3", "a1", "u2", "click", 60003, null, null)
  }
  it should "should not remove duplicate events for when eventType is different" in {
    val events = new Event("src/test/resources/testevents-sameu-samea-samem-diffe.csv")
    val noDupes = prepareForDedDupeTest(events.deduped)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "purchase", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60002", "ev2", "a1", "u1", "click", 60002, null, null)
  }
  it should "should not remove duplicate events for when minute is different" in {
    val events = new Event("src/test/resources/testevents-sameu-samea-diffm-samee.csv")
    val noDupes = prepareForDedDupeTest(events.deduped)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("160003", "ev3", "a1", "u1", "click", 160003, 60002, 100001)
  }
  it should "only remove duplicates where aggregation keys are the same (complex example)" in {
    val events = new Event("src/test/resources/testevents.csv")
    val noDupes = prepareForDedDupeTest(events.deduped)
    noDupes.size shouldEqual 4
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60011", "ev4", "a2", "u1", "click", 60011, null, null)
    noDupes(2) should contain theSameElementsAs Seq("60012", "ev5", "a2", "u1", "purchase", 60012, null, null)
    noDupes(3) should contain theSameElementsAs Seq("160009", "ev7", "a1", "u1", "click", 160009, 60008, 100001)
  }
  it should "find attribution events for each impression, which are events for same advertiser and user exclusively after impression" in {
    val attributions = new ImpressionEvent(
      new Event("src/test/resources/attribution-events.csv"),
      new Impression("src/test/resources/attribution-impressions.csv"))
    val attr = prepareForAttributionTest(attributions.attributions)
    attr.size shouldEqual 4
    attr(0) should contain theSameElementsAs Seq("60002", "a1", "c1", "u1", 60002, "160009", "ev7", "a1", "u1", "click", 160009, 60008, 100001)
    attr(1) should contain theSameElementsAs Seq("60012", "a2", "c3", "u1", 60012, "120013", "ev4", "a2", "u1", "click", 120013, 60011, 60002)
    attr(2) should contain theSameElementsAs Seq("60012", "a2", "c3", "u1", 60012, "60014", "ev5", "a2", "u1", "purchase", 60014, null, null)
    attr(3) should contain theSameElementsAs Seq("60013", "a2", "c3", "u2", 60013, "60015", "ev5", "a2", "u2", "purchase", 60015, null, null)

    val stat = prepareForStatisticsTest(attributions.statistics)
    stat.size shouldEqual 4
    stat(0) should contain theSameElementsInOrderAs Seq("click", "a1", 1, 1)
    stat(1) should contain theSameElementsInOrderAs Seq("click", "a2", 1, 1)
    stat(2) should contain theSameElementsInOrderAs Seq( null, "a5", null, null)
    stat(3) should contain theSameElementsInOrderAs Seq("purchase", "a2", 2, 2)
  }

  it should "compute the count of attributed events for each advertiser, grouped by event type"

  it should "compute the count of unique users that have generated attributed events for each advertiser, grouped by event type"
}
