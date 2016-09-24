import com.entity.EmployeeStage
import model.{Dept, Employee}
import org.apache.spark.{SparkConf, SparkContext}
import org.test.KieSessionFactory
import org.test.model.Applicant

// this is used to implicitly convert an RDD to a DataFrame.

// Import Spark SQL data types and Row.

import org.apache.spark.sql._

/**
 * Created by vinod on 9/20/16.
 */
object CsvHandling {


  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val emps = sc.textFile("src/main/resources/data/csv/emps", 3).map(_.split(",")).
      map(e => Employee(e(0), e(1).toInt, e(2).toDouble, e(3).toInt)).toDF()
    val depts = sc.textFile("src/main/resources/data/csv/dept", 3).map(_.split(",")).
      map(e => Dept(e(0), e(1).toInt)).toDF()
    emps.show()
    depts.show()
    val joinedDf = emps.as("e").join(depts.as("d"), $"e.deptId" === $"d.deptId")
    val empStages = joinedDf.select("name", "d.dep", "salary").rdd.map(p => new EmployeeStage(p(0).toString, p(1).toString, p(2).toString.toDouble, false))
    empStages.foreach(x => println(x + " " + x.dept + " " + x.salary))



    val rdd2 = empStages.mapPartitions(applicants => evalRules(applicants))
    rdd2.foreach(x => println(x.approved))
  }

  def evalRules(incomingEvents: Iterator[EmployeeStage]): Iterator[EmployeeStage] = {
    val ksession = KieSessionFactory.getKieSession("src/main/resources/drl/emp.drl")
    val patients = incomingEvents.map(patient => {
      ksession.execute(patient)
      patient
    })
    patients
  }

  def show(map: Map[String, Any]): Unit = {
    println(map.get(""))
  }

}
