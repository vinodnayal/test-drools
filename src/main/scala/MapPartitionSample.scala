import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by vinod on 9/20/16.
 */
object MapPartitionSample {

  def toUpperCase(x: Iterator[String]): Iterator[String] = {
    x.map {
      _.toUpperCase
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val rdd1 = sc.parallelize(List("yellow", "red", "blue"), 2)
    val rdd2 = rdd1.mapPartitions(toUpperCase(_))
    rdd2.saveAsTextFile("test")
  }


}
