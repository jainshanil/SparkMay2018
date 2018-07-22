package SparkExamples

/*
Choose language of your choice Python or Scala
Data is available in HDFS file system under /public/crime/csv
You can check properties of files using hadoop fs -ls -h /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,”
Get monthly count of primary crime type, sorted by month in ascending and number of crimes per type in descending order
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution01/crimes_by_type_by_month
Output File Format: TEXT
Output Columns: Month in YYYYMM format, crime count, crime type
Output Delimiter: \t (tab delimited)
Output Compression: gzip
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TotalCrimePerMonthPerType {

  def main(args: Array[String]) = {

    val conf = new SparkConf().
      setMaster("yarn").
      setAppName("TotalCrimePerMonthPerType").
      set("spark.ui.port","23675")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val cRaw = sc.textFile("/public/crime/csv/crime_data.csv").
      filter(_.substring(0,2) != "ID").map(rec => {
      val arr = rec.split(",")
      (arr(2),arr(5))}).toDF("Date","Crime")

    cRaw.registerTempTable("Crime_table")

    val crmAggregated = sqlContext.sql("select concat(substring(Date,7,4),substring(Date,1,2)) as Month, "+
      "count(*) as crime_count , Crime from Crime_table "+
      "group by concat(substring(Date,7,4),substring(Date,1,2)),Crime "+
      "order by concat(substring(Date,7,4),substring(Date,1,2)),crime_count desc")

    crmAggregated.rdd.
      map(rec => rec.mkString("\t")).coalesce(1).
      saveAsTextFile("/user/jainshanil/solutions/solution01/crimes_by_type_by_month",
        classOf[org.apache.hadoop.io.compress.GzipCodec])

  }
}
