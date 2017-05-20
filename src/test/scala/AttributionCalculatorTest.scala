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

  def deterministic(s: Dataset[Row]) = {
    s.collect.map(_.toSeq).sortWith( (_(5).toString.toInt < _(5).toString.toInt))
  }
  "AttributionCalculator" should "should remove duplicate events for adv/user/eventtype within minute. Use last timestamp instead" in {
    val events = new Event("src/test/resources/testevents-sameu-samea-samem-samee.csv")
    val noDupes = deterministic(events.withLagsDF2)
    noDupes.size shouldEqual 1
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
  }

  it should "should not remove duplicate events for when advertizer is different" in {
    val events = new Event("src/test/resources/testevents-sameu-diffa-samem-samee.csv")
    val noDupes = deterministic(events.withLagsDF2)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60002", "ev2", "a2", "u1", "click", 60002, null, null)
  }

  it should "should not remove duplicate events for when user is different" in {
    val events = new Event("src/test/resources/testevents-diffu-samea-samem-samee.csv")
    val noDupes = deterministic(events.withLagsDF2)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60003", "ev3", "a1", "u2", "click", 60003, null, null)
  }
  it should "should not remove duplicate events for when eventType is different" in {
    val events = new Event("src/test/resources/testevents-sameu-samea-samem-diffe.csv")
    val noDupes = deterministic(events.withLagsDF2)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "purchase", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60002", "ev2", "a1", "u1", "click", 60002, null, null)
  }
  it should "should not remove duplicate events for when minute is different" in {
    val events = new Event("src/test/resources/testevents-sameu-samea-diffm-samee.csv")
    val noDupes = deterministic(events.withLagsDF2)
    noDupes.size shouldEqual 2
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("160003", "ev3", "a1", "u1", "click", 160003, 60002, 100001)
  }
  it should "only remove duplicates where aggregation keys are different (complex example)" in {
    val events = new Event("src/test/resources/testevents.csv")
    val noDupes = deterministic(events.withLagsDF2)
    noDupes.size shouldEqual 4
    noDupes(0) should contain theSameElementsAs Seq("60001", "ev1", "a1", "u1", "click", 60001, null, null)
    noDupes(1) should contain theSameElementsAs Seq("60011", "ev4", "a2", "u1", "click", 60011, null, null)
    noDupes(2) should contain theSameElementsAs Seq("60012", "ev5", "a2", "u1", "purchase", 60012, null, null)
    noDupes(3) should contain theSameElementsAs Seq("160009", "ev7", "a1", "u1", "click", 160009, 60008, 100001)
  }
  it should "find attribution events for each impression, which are events for same advertiser and user after impression"

  it should "compute the count of attributed events for each advertiser, grouped by event type"

  it should "compute the count of unique users that have generated attributed events for each advertiser, grouped by event type"
}
