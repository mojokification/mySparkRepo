package retail_db

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GetDailyProductRevenueDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      master("local").
      appName("Get Daily Product Revenue Using DataFrame Operations").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    import spark.implicits._

    val orders = spark.read.json("E:\\Work\\data\\data-master\\retail_db_json\\orders")
    val orderItems  = spark.read.json("E:\\Work\\data\\data-master\\retail_db_json\\order_items")

    val dailyProductRevenue = orders.
      filter($"order_status".isin("COMPLETE", "CLOSED")).
      join(orderItems, $"order_id" === $"order_item_order_id").
      groupBy($"order_date", $"order_item_product_id").
      agg(sum("order_item_subtotal").alias("daily_product_revenue")).
      orderBy($"order_date",$"daily_product_revenue".desc)

    dailyProductRevenue.show(false)
    val outputBaseDirectory = ("E:\\Work\\data\\data-master\\")
    dailyProductRevenue.write.json(outputBaseDirectory + "get_daily_product_revenue_df")

  }

}
