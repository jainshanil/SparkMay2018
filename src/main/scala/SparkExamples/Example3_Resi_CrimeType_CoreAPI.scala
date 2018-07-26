package SparkExamples
/*
Data is available in HDFS file system under /public/crime/csv
Structure of data (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)
File format - text file
Delimiter - “,” (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
Get top 3 crime types based on number of incidents in RESIDENCE area using “Location Description”
Store the result in HDFS path /user/<YOUR_USER_ID>/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA
Output Fields: Crime Type, Number of Incidents
Output File Format: JSON
Output Delimiter: N/A
Output Compression: No
 */

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SQLContext


object Example3_Resi_CrimeType_CoreAPI {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("Example3_Resi_CrimeType").
      setMaster("yarn").
      set("spark.ui.port","23896")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val crmRaw = sc.textFile("/public/crime/csv")
    val header = crmRaw.first
    val crmRawWoHdr = crmRaw.filter(rec => rec != header)

    val crmFiltered = crmRawWoHdr.map(rec => {
      val str = rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      (str(7),str(5))
    }).filter(_._1 == "RESIDENCE")

    val crimeTypeCnt = crmFiltered.
      map(rec => (rec._2,1)).
      reduceByKey(_ + _).
      map(rec => (rec._2,rec._1)).
      sortByKey(false)

    val crimeCountForRes = sc.parallelize(crimeTypeCnt.map(rec => (rec._2,rec._1)).take(3))

    crimeCountForRes.toDF("crime_type","cnt").
      coalesce(1).
      write.json("/user/jainshanil/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA")

  }

}
