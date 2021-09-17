import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

  class TestProb3 extends AnyFunSuite{

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("testing case3")
      .getOrCreate()

    val productByUserRow = Seq ((2,150,1002), (3,150,1003), (1,100,1001), (1,100,1004))

    import spark.implicits._
    val productByUserDF: DataFrame = productByUserRow.toDF("UserID","Price","Product_ID")  //select("UserID", "Price", "Product_ID")
    val outputDF: DataFrame = Service.TotalSpending(productByUserDF)

    //TEST CASE
    val totalusers: Long = outputDF.count()
    totalusers should be (4)

  }


