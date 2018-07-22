package DataFrames

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._


object TotalRevenue {

  def main(args : Array[String]){

    val props = ConfigFactory.load()
    val conf = new SparkConf().
                setAppName("TotalRevenueDF").
                setMaster(props.getConfig(args(2)).getString("executionMode")).
                set("spark.ui.port","23587")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions","2")
    import sqlContext.implicits._

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val inputBasePath = args(0)
    val outputPath = args(1)

    if (!fs.exists(new Path(inputBasePath+"/orders")) ||
            !fs.exists(new Path(inputBasePath+"/order_items"))){
      println("Input path :"+inputBasePath+"/orders does not exist !!!!!!!!")
    }
    else{
      val ordersDF = sc.textFile(inputBasePath+"/orders").
        map(rec => {
          val arr = rec.split(",")
          Orders(arr(0).toInt,arr(1),arr(2).toInt,arr(3))
        }).toDF

      //ordersDF.show(30)
      //val orderItmPath = args(1)
      val orderItmDF = sc.textFile(inputBasePath+"/order_items").
        map(rec => {
          val a = rec.split(",")
          Order_items(a(0).toInt,a(1).toInt,a(2).toInt,a(3).toInt,a(4).toFloat,a(5).toFloat)
        }).toDF

      //orderItmDF.show(20)

      val ordDFfiltered = ordersDF.filter(ordersDF("order_status") === "COMPLETE")

      val ord_ordItm_join = ordDFfiltered.join(orderItmDF,
        ordDFfiltered("order_id") === orderItmDF("order_item_order_id"))

      val ord_grp = ord_ordItm_join.groupBy("order_date")

      val ord_agg = ord_grp.agg(sum("order_item_subtotal")).
        sort("order_date")

      if (fs.exists(new Path(outputPath))){
        println("Deleting "+outputPath+" !!!!!!!!!!!")
        fs.delete(new Path(outputPath),true)
      }

      ord_agg.rdd.saveAsTextFile(outputPath)


    }






  }

}
