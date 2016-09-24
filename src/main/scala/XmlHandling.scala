import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import org.codehaus.jackson.`type`.TypeReference
import org.codehaus.jackson.map.ObjectMapper
import org.json4s.jackson.Json
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by vinod on 9/20/16.
 */
object XmlHandling {

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()
    val df = spark
      .read
      .format("com.databricks.spark.xml")
      .option("rowTag", "pnr")
      .load("src/main/resources/data/sample.xml")
    val df_Map = df.toJSON.rdd.map(x => jsonStrToMap(x))
    df_Map.foreach(x => show(x))
    val mapper = new ObjectMapper()
     df.toJSON.rdd.saveAsTextFile("target/json")

  }

  def show(map: Map[String, Any]): Unit = {
    println(map.get(""))
  }

}
