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

object Example3_Resi_CrimeType {

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

    val crmDF = crmRawWoHdr.map(rec => {
      val str = rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      (str(7),str(5))
    }).toDF("loc_desc","crime_type")

    crmDF.registerTempTable("crime")

    val reslt = sqlContext.sql("select crime_type,cnt from "+
                                            "(SELECT crime_type,COUNT(crime_type) AS cnt,"+
                                                " RANK() OVER(ORDER BY COUNT(crime_type) DESC) as rnk from crime "+
                                                " WHERE loc_desc = 'RESIDENCE' "+
                                                " group by crime_type) CRM_RNK "+
                                        " where rnk < 4"
    )

    reslt.coalesce(1).save("/user/jainshanil/solutions/solution03/RESIDENCE_AREA_CRIMINAL_TYPE_DATA_sql","json")
  }

}
