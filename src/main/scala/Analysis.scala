import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Analysis {

  val conf = new SparkConf().setAppName("Aadhar Data Analysis").setMaster("local")
  val sc = new SparkContext(conf)


  def main(args: Array[String]): Unit = {

    val rawData = sc.textFile(args(0))
    val csvData = rawData
                      .map {rawLine =>
                            val column = rawLine.split(",")
                            Record(column(0),column(1),column(2),column(3),column(4),
                              column(5).toInt,column(6),column(7).toInt,column(8).toInt,
                              column(9).toInt,column(10).toInt,column(11).toInt)
                          }
  }
}

case class Record(registrar: String,enrolmentAgency: String,state: String,district: String,
                  subDistrict: String,pinCode: Int,gender: String,age: Int,aadhaarGenerated: Int,
                  enrolmentRejected: Int,residentsProvidingEmail: Int,
                  residentsProvidingMobileNumber: Int
)
