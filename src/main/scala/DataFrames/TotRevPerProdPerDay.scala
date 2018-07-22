package DataFrames

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf,SparkContext}
import com.typesafe.config.ConfigFactory

object TotRevPerProdPerDay {

  def main( args: Array[String])={

    val props = ConfigFactory.load()

    val conf = new SparkConf()
      .setAppName("TotRevPerProdPerDay")
      //.setMaster(props.getConfig(args(2)).getString("executionMode"))
      .set("spark.ui.port","25467")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions","2")

    sqlContext.sql("use shane_pig_demo")

    //val ordDf = sqlContext.sql("select count(*) from orders")

    val totalRevDF = "INSERT OVERWRITE TABLE total_rev "+
      "SELECT o.order_date, p.product_name,round(sum(oi.order_item_subtotal),2) as daily_rev "+
      "FROM products p INNER JOIN order_items oi "+
      "ON p.product_id = oi.order_item_product_id "+
      "INNER JOIN orders o "+
      "ON oi.order_item_order_id = o.order_id "+
      "where o.order_status in ('COMPLETE','CLOSED') "+
      "group by o.order_date, p.product_name "+
      "order by o.order_date, daily_rev desc"

    sqlContext.sql(totalRevDF)

    //ordDf.rdd.saveAsTextFile("/user/jainshanil/ordersDF_test")

    //val totRevRdd = totalRevDF.rdd.map(rec => rec.split(",")(0)+"\t"+rec.split(",")))

  }

}
