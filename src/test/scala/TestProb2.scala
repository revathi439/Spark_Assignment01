  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.scalatest.funsuite.AnyFunSuite
  import org.scalatest.matchers.must.Matchers.{be, contain}
  import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

  class TestProb2 extends AnyFunSuite{

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("testing case2")
      .getOrCreate()

    val productByUserRow = Seq ((1001,1), (1003,3), (1001,2), (1001,4))

    import spark.implicits._
    val productByUserDF: DataFrame = productByUserRow.toDF("Product_ID","UserID")
    val outputDF: DataFrame = Service.productBought(productByUserDF)

    // TEST_CASE
    val totalRowsUser: Long = outputDF.count()
    totalRowsUser should be (4)

    outputDF.collect() should contain allElementsOf Array(Row(1,1),Row(3,1),Row(4,1),Row(2,1))


}


