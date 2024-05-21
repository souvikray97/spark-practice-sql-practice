package org.souvik.application


import org.apache.spark.sql.functions.{current_timestamp, expr}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}


object Main {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder()
    .appName("spark-practice2")
    .master("local[*]")
    .config("spark.driver.bindAddress","127.0.0.1")
    .getOrCreate()

    val df: DataFrame =spark
      .read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    val timestampFromExpression = expr("cast(current_timeStamp() as string) as timetampExpression")
    val timestampFromFucntion =  current_timestamp().cast(StringType).as("timestampFunction")

    df.select(timestampFromExpression, timestampFromFucntion).show()

    df.selectExpr("cast(Date as String)", "Open + 1.0", "current_timestamp()").show

    df.createTempView("df")

    spark.sql("Select * from df").show(100)
  }
  }
