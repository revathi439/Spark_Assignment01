import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkQ1 extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
 implicit val spark:SparkSession =SparkSession.builder()
    .master("local[*]")
    .appName("SparkQ1")
    .getOrCreate()


  val transDF = Service.transactionFunction("src/main/resources/transaction.csv")

  val userDF = Service.transactionFunction("src/main/resources/user.csv")

  val joinedDF = Service.getJoined(transDF,userDF,"UserID")

  val resultuniqLocation = Service.getuniqueLocation(joinedDF)
  resultuniqLocation.show(false)

  val resultproductBought = Service.productBought(transDF)
  resultproductBought.show()

  val resulttotalSpending = Service.TotalSpending(transDF)
  resulttotalSpending.show()

}
