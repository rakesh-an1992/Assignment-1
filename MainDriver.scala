object MainDriver extends App {

  val jsonFilePath = "src/main/resources/inputs/"
  val jsonDf = LogicProcessor.readJsonFile(jsonFilePath)

  // average session duration
  val avgSessionDurationDf = LogicProcessor.calcAvgSessionDuration(jsonDf)
  //avgSessionDurationDf.show(false)

  //Count of "calc_userid" for each "region". ignore "-" and nulls
  val cntCalcUserIdDf = LogicProcessor.getCntCntCalcUserID(jsonDf)
  //cntCalcUserIdDf.show(false)

  /*
  Consider "eventlaenc" =126 or 107 as defining actions.
  Calculate first and second defining action, ordered based on time, for each "calc_userid" and also find the count of those actions.
   */
 val eventActionsDf = LogicProcessor.deriveEventActions(jsonDf)
  //eventActionsDf.show(false)

}
