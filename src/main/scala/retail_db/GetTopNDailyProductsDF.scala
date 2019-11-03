package retail_db

import com.typesafe.config.ConfigFactory //allows us to load properties mentioned in application.properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object GetTopNDailyProductsDF {

  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load() //don't need to pass any parameter as long as our path for properties is src/main/resources
    //val envProps = props.getConfig("dev") //this api requires parameter as the first part which is the group from app.properties
    val envProps = props.getConfig(args(0))
    val topN = args(1).toInt
    val spark = SparkSession.builder().
      master(envProps.getString("execution.mode")).
      appName("Get Top" + topN + "Products Per Day").
      getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    import spark.implicits._

    val inputBaseDirectory = envProps.getString("input.base.dir")
    val outputBaseDirectory = envProps.getString("output.base.dir")

    val orders = spark.read.json(inputBaseDirectory + "orders")
    val orderItems  = spark.read.json(inputBaseDirectory + "order_items")

    val dailyProductRevenue = orders.
      filter($"order_status".isin("COMPLETE", "CLOSED")).
      join(orderItems, $"order_id" === $"order_item_order_id").
      groupBy($"order_date", $"order_item_product_id").
      agg(sum("order_item_subtotal").alias("daily_product_revenue")).
      orderBy($"order_date",$"daily_product_revenue".desc)

    //val spec = Window.partitionBy("order_date")
    //val avgRevenue = avg($"daily_product_revenue").over(spec)
    //dailyProductRevenue.withColumn("avg_revenue", avgRevenue).show(false)

    val specRank = Window.partitionBy("order_date").orderBy(col("daily_product_revenue").desc)
    val ranking = rank.over(specRank)
    val topNDailyProducts = dailyProductRevenue.withColumn("rnk", ranking)
    //val topN = 5
    topNDailyProducts.
      where(col("rnk") <= topN).
      orderBy($"order_date", $"rnk").
      write.json(outputBaseDirectory+"get_top_n_daily_products_using_df")


  }

}
