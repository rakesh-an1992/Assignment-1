import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

object LogicProcessor {
  def getSparkSession: SparkSession = {
    SparkSession.builder().master("local").appName("Spark-Assignment").getOrCreate()
  }

  def readJsonFile(in_path:String, fileName:Option[String] = None): DataFrame ={
    var srcPath = ""
    fileName match{
      case Some(_) => srcPath = s"${in_path}${srcPath}.json"
      case None => srcPath = in_path
    }
    val jsonDf = getSparkSession.read.format("json").load(srcPath)
    jsonDf
  }

  def calcAvgSessionDuration(df : DataFrame):DataFrame ={
    val avgSessionDf = df.filter(col("appnameenc").isin(lit(1), lit(2)))
      .select(col("sessionid"), col("timestampist").cast(TimestampType).as("session_ts"))
      .withColumn("DiffInSeconds", current_timestamp().cast(LongType) - col("session_ts").cast(LongType))
      .groupBy(col("sessionid"))
      .agg(avg(col("DiffInSeconds")).as("avg_session_ts"))
      .withColumn("DurationInMin", round(col("avg_session_ts")/60, 2))
      .withColumn("DurationInHours", round(col("avg_session_ts")/3600, 2))
      .withColumn("DurationInDays", col("avg_session_ts")/(24*3600))
      .select(col("sessionid"), col("DurationInDays").cast(LongType), col("DurationInHours"), col("DurationInMin"))
    avgSessionDf
  }

  def getCntCntCalcUserID(df:DataFrame):DataFrame ={
    val cntCalcUserID = df.filter(col("region").isNotNull and col("region").notEqual(lit("-")))
      .groupBy(col("region")).count().as("calc_userid")
    cntCalcUserID
  }

  def deriveEventActions(df : DataFrame): DataFrame ={
    val partitioCols = Window.partitionBy(col("calc_userid"),col("eventlaenc")).orderBy(col("timestampist").desc)
    val partitioCols1 = Window.partitionBy(col("calc_userid")).orderBy(col("timestampist").desc)

    val actionsDf = df.filter(col("eventlaenc").isin(lit(126), lit(107)))
    val actionsDf_1 = actionsDf.withColumn("row_num1", row_number()over(partitioCols))
          .filter(col("row_num1").equalTo(lit(1)))

    val actionCnts = actionsDf.groupBy(col("calc_userid"), col("eventlaenc")).count()

    val actionsWithCnt = actionsDf_1.as("act").join(actionCnts.as("cnt"),
      col("act.calc_userid") === col("cnt.calc_userid")
        && col("act.eventlaenc") === col("cnt.eventlaenc"),"left_outer")
      .select(col("act.calc_userid"), col("act.eventlaenc"), col("act.timestampist"), col("cnt.count").as("action_cnt"))
      .withColumn("row_num2", row_number() over(partitioCols1))
      .withColumn("first_action", when(col("row_num2").equalTo(lit(1)), col("eventlaenc")).otherwise(""))
      .withColumn("first_action_cnt", when(col("row_num2").equalTo(lit(1)), col("action_cnt")).otherwise(""))
      .withColumn("second_action", when(col("row_num2").equalTo(lit(2)), col("eventlaenc")).otherwise(""))
      .withColumn("second_action_cnt", when(col("row_num2").equalTo(lit(2)), col("action_cnt")).otherwise(""))
        .drop("timestampist","action_cnt", "row_num2")
    actionsWithCnt
  }
}
