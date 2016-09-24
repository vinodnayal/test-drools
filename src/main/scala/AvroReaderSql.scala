//import com.databricks.spark.avro._
//import org.apache.spark.SparkConf

import java.io.File

import com.entity.Book
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.sql._
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.rdd.RDD

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Created by vinod on 9/19/16.
 */
object AvroReaderSql {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark avro reader").setMaster("local"))
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.avro").load("src/main/resources/book.avro")
    df.show()
  }
}
