import org.apache.spark.{SparkConf, SparkContext}
import org.test.KieSessionFactory
import org.test.model.Applicant

/**
 * Created by vinod on 9/20/16.
 */
object DroolsTest {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))

    val applicants = List(
      new Applicant(1, "John", "Doe", 10000, 568),
      new Applicant(2, "John", "Greg", 12000, 654),
      new Applicant(3, "Mary", "Sue", 100, 568),
      new Applicant(4, "Greg", "Darcy", 1000000, 788),
      new Applicant(5, "Jane", "Stuart", 10, 788));
    val rdd1 = sc.parallelize(applicants, 2)
    val rdd2 = rdd1.mapPartitions(applicants => evalRules(applicants))
    rdd2.foreach(x => println(x.isApproved))


  }

  def evalRules(incomingEvents: Iterator[Applicant]): Iterator[Applicant] = {
    val ksession = KieSessionFactory.getKieSession("src/main/resources/drl/approval.drl")
    val patients = incomingEvents.map(patient => {
      ksession.execute(patient)
      patient
    })
    patients
  }

}
