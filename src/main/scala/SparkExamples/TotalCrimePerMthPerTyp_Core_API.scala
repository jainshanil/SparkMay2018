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

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SQLContext

object TotalCrimePerMthPerTyp_Core_API {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("TotalCrimePerMthPerTyp_Core_API").
      setMaster("yarn").
      set("spark.ui.port","26754")

    val sc = new SparkContext()


    val crimeRaw = sc.textFile("/public/crime/csv")
    val header = crimeRaw.first
    val crimeRawWOHeader = crimeRaw.filter(rec => rec != header)
    //crimeRawWOHeader.take(5).foreach(println)

    val crimeRawMap = crimeRawWOHeader.
      map(rec => (rec.split(",")(2),rec.split(",")(5))).
      map(rec => ((rec._1.substring(6,10)+rec._1.substring(0,2),rec._2),1))

    val crime_rBK = crimeRawMap.reduceByKey(_ + _)

    val crime_count_sorted = crime_rBK.
      map(rec => ((rec._1._1,-rec._2),rec._1._1+"\t"+rec._2+"\t"+rec._1._2)).
      sortByKey().
      map(rec => rec._2)

    //crime_count_sorted.take(30).foreach(println)

    crime_count_sorted.map(rec => rec._1._1+"\t"+rec._2+"\t"+rec._1._2).
      coalesce(1,shuffle = true).
      saveAsTextFile("/user/jainshanil/solutions/solution01/crimes_by_typ_by_mth_core")


  }

}
