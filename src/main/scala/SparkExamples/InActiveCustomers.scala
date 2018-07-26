package SparkExamples

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SQLContext
import scala.io.Source

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

object InActiveCustomers {

  def main(args: Array[String]) = {

    val conf = new SparkConf().
      setMaster("yarn").
      setAppName("InActiveCustomers").
      set("spark.ui.port","23896")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ordersRaw = Source.fromFile("/data/retail_db/orders/part-00000").getLines.toList
    val ordDF = sc.parallelize(ordersRaw).
      map(rec => (rec.split(",")(2).toInt,rec.split(",")(0).toInt)).
      toDF("order_customer_id","order_id")


    val custRaw = Source.fromFile("/data/retail_db/customers/part-00000").getLines.toList
    val custDF =sc.parallelize(custRaw).
      map(rec => (rec.split(",")(0).toInt,rec.split(",")(1),rec.split(",")(2))).
      toDF("customer_id","customer_fname","customer_lname")

    ordDF.registerTempTable("orders")

    custDF.registerTempTable("customers")

    val inactiveCust = sqlContext.sql("SELECT c.customer_lname,c.customer_fname "+
      " FROM customers c LEFT JOIN orders o"+
      " ON c.customer_id = o.order_customer_id "+
      " WHERE o.order_customer_id is NULL "+
      " ORDER BY c.customer_lname,c.customer_fname")

    inactiveCust.rdd.
      map(rec => rec.mkString(", ")).
      coalesce(1).
      saveAsTextFile("/user/jainshanil/solutions/solution02/inactive_customers_sql")

  }

}
