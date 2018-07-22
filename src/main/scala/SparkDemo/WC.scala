package SparkDemo
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.hadoop.fs._
import com.typesafe.config.ConfigFactory

object WC {

  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))

    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(envProps.getString("executionMode")).
      set("spark.ui.port","45689")

    val sc = new SparkContext(conf)

    val inputPath = args(1)
    val outputPath = args(2)

    val fs = FileSystem.get(sc.hadoopConfiguration)

    if (!fs.exists(new Path(inputPath))){
      println("Input path does not exist")
    }else{
      if (fs.exists(new Path(outputPath))){
        println("Deleting "+outputPath)
        fs.delete(new Path(outputPath),true)
      }
      val lines = sc.textFile(inputPath)
    //lines.take(5).foreach(println)
      val word_count = lines.flatMap(line => line.split(" "))
                            .map(word => (word,1))
                            .reduceByKey( _ + _)

    //word_count.take(5).foreach(println)
      word_count.map(rec => rec._1+"\t"+rec._2).saveAsTextFile(outputPath)
    //sc.textFile("C:\\Users\\Shanil\\udacity\\data\\retail_db\\order_items\\part-00000").
      //take(5).
      //foreach(println)

    }
  }

}
