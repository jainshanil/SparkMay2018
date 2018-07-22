package DataFrames

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory


object TotRevenueDFSprkSql {

  def main(args : Array[String]): Unit ={

    val props = ConfigFactory.load()


    val conf = new SparkConf().
                    setAppName("TotRevenueDFSprkSql").
                      setMaster(props.getConfig(args(2)).getString("executionMode")).
                    set("spark.ui.port","23548")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions","2")
    import sqlContext.implicits._

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputBasePath = args(0)
    val outputPath = args(1)

    if (!fs.exists(new Path(inputBasePath+"\\orders")) ||
      !fs.exists(new Path(inputBasePath+"/order_items"))){
      println("Input Path "+inputBasePath+"\\orders does not exist")
    }else{
      val ordersDF = sc.textFile(inputBasePath+"\\orders").map(rec => {
                              val arr = rec.split(",")
                              Orders(arr(0).toInt,arr(1),arr(2).toInt,arr(3))
      }).toDF()

      val order_itmDF = sc.textFile(inputBasePath+"\\order_items").map(rec => {
        val arr = rec.split(",")
        Order_items(arr(0).toInt,
          arr(1).toInt,
          arr(2).toInt,
          arr(3).toInt,
          arr(4).toFloat,
          arr(5).toFloat)
      }).toDF()

      ordersDF.registerTempTable("ord")
      order_itmDF.registerTempTable("ord_itm")

      val totRevPerDay = sqlContext.sql("select o.order_date,sum(oi.order_item_subtotal) as total_rev "+
                      "from ord o inner join ord_itm oi "+
                      "on o.order_id = oi.order_item_order_id "+
                       "group by o.order_date "+
                      " order by o.order_date")

      if(fs.exists(new Path(outputPath))){

        println("Deleting output path :"+outputPath)
        fs.delete(new Path(outputPath),true)
      }
      totRevPerDay.rdd.saveAsTextFile(outputPath)

    }






  }

}
