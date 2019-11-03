package retail_db

import org.apache.spark.sql.SparkSession

object DataFrameDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.
      builder().
      appName("Data Frame Demo").
      master("local").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val ordersDF = spark.
      read.
      json("E:\\Work\\data\\data-master\\retail_db_json\\orders")

    ordersDF.show(false)

  }

}
