import org.apache.spark.sql._

object Service{
  // reading transaction csv data
  def transactionFunction(path:String)(implicit spark:SparkSession): DataFrame ={
    val transdata = spark.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv(path=path)
     transdata
  }


def getJoined(transDF:DataFrame, userDF:DataFrame, joinCol:String):DataFrame={
  val joinedDF = transDF.join(userDF, Seq(joinCol))
  joinedDF
}

//function for finding UniqLocation

  def getuniqueLocation(joinedDF:DataFrame):Dataset[Row]={
    joinedDF.select("Location").distinct()
  }

// Function for finding ProductBought by each user

  def productBought(transDF:DataFrame)={
    transDF.select("Product_ID","UserID")
      .groupBy("UserID")
      .count()
      .withColumnRenamed("count","total product")
  }

//Function for finding total amount spend by user

  def TotalSpending(transDF:DataFrame)={
    transDF.select("UserID","Price","Product_ID")
      .groupBy("UserID","Product_ID")
      .agg(functions.sum("Price")).withColumnRenamed("sum(price)","totalSpending")
  }
}

