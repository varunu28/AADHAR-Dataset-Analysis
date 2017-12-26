import org.apache.spark.{SparkConf, SparkContext}

object ReadingData {

  val conf = new SparkConf().setAppName("Aadhar Data Analysis").setMaster("local")
  val sc = new SparkContext(conf)


  def main(args: Array[String]): Unit = {

    val aadharData = sc.textFile("InputData/UIDAI-ENR-DETAIL-20171128.csv")

    val header = aadharData.first()

    val aadharDataWithoutHeader = aadharData.filter(rec => rec != header)

    aadharData.take(5).foreach(println)
  }
}
