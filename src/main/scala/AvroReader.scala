//import com.databricks.spark.avro._
//import org.apache.spark.SparkConf

import java.io.File

import com.entity.Book
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificDatumReader

//import org.apache.spark.sql._
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.rdd.RDD

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.apache.spark.sql.SparkSession

/**
 * Created by vinod on 9/19/16.
 */
object AvroReader {
  def main(args: Array[String]): Unit = {


    val schema = new Schema.Parser().parse(new File("src/main/resources/avro/book.avsc"))
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark
      .read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schema.toString)
      .load("src/main/resources/data/book.avro").show()

    val bookDatumReader: GenericDatumReader[Book] = new SpecificDatumReader[Book](classOf[Book])
    val bookFileReader: DataFileReader[Book] = new DataFileReader[Book](new File("target/book.avro"), bookDatumReader)
    while (bookFileReader.hasNext) {
      val b: Book = bookFileReader.next
      println("deserialized from file: " + b)
    }
  }
}
