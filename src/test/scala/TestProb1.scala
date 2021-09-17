import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.{be, contain}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class TestProb1 extends AnyFunSuite{
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val sc:SparkSession= new SparkSession
  .Builder()
    .appName("TestCase for UniqLocation")
    .master("local[*]")
    .getOrCreate()

  val locationRow = List("Vizag","Vizag","Andhra","Vijayanagaram")
  import sc.implicits._
  val LocationsDF:DataFrame = locationRow.toDF("Location")
  val outputDF: Dataset[Row] = Service.getuniqueLocation(LocationsDF)

  assert(outputDF.count()===3)
  assert(outputDF.collect()=== Array(Row("Vizag"),Row("Vijayanagaram"),Row("Andhra")))

  outputDF.collect() should contain allElementsOf Array(Row("Vizag"),Row("Vijayanagaram"),Row("Andhra"))

  val VizagCount: Long = outputDF.filter(col("Location")==="Vizag").count()
  VizagCount should be (1)


}


