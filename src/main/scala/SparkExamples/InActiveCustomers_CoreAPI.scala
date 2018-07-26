package SparkExamples

/*
Data is available in local file system /data/retail_db
Source directories: /data/retail_db/orders and /data/retail_db/customers
Source delimiter: comma (“,”)
Source Columns - orders - order_id, order_date, order_customer_id, order_status
Source Columns - customers - customer_id, customer_fname, customer_lname and many more
Get the customers who have not placed any orders, sorted by customer_lname and then customer_fname
Target Columns: customer_lname, customer_fname
Number of files - 1
Target Directory: /user/<YOUR_USER_ID>/solutions/solutions02/inactive_customers
Target File Format: TEXT
Target Delimiter: comma (“, ”)
Compression: N/A
 */

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SQLContext
import scala.io.Source

object InActiveCustomers_CoreAPI {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("yarn").
      setAppName("InActiveCustomers_CoreAPI").
      set("spark.ui.port","23896")

    val sc = new SparkContext(conf)

    val ordRaw = Source.fromFile("/data/retail_db/orders/part-00000").getLines.toList

    val ordRDD = sc.parallelize(ordRaw).
      map(rec => (rec.split(",")(2).toInt,rec.split(",")(0).toInt))

    val custRaw = Source.fromFile("/data/retail_db/customers/part-00000").getLines.toList

    val custRDD = sc.parallelize(custRaw).
      map(rec => (rec.split(",")(0).toInt,(rec.split(",")(1),rec.split(",")(2))))

    val custOrdLeftJoin = custRDD.leftOuterJoin(ordRDD)

    val inActiveCustRDD = custOrdLeftJoin.filter(rec => rec._2._2 match {
      case None => true
      case _ => false
    })

    val inActiveCustRDDSorted = inActiveCustRDD.
      map(rec => ((rec._2._1._2,rec._2._1._1),rec)).
      sortByKey().
      map(rec => rec._1._1+", "+rec._1._2)

    inActiveCustRDDSorted.
      coalesce(1).
      saveAsTextFile("/user/jainshanil/solutions/solution02/inactive_customers_core_api")

  }
}
