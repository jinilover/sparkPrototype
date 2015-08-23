package loganalysis

import AnalyzeLogCommonTypes._

import scalaz._
import scalaz.Free._
import scalaz.Scalaz._

//start of ADT
sealed trait AnalyzeLog[NEXT]

case class StartBigDataApp[NEXT](next: NEXT)
  extends AnalyzeLog[NEXT]

case class StopBigDataApp[NEXT](next: NEXT)
  extends AnalyzeLog[NEXT]

case class ParseLogFile[NEXT](
                               fileName: String,
                               parseFunc: String => Option[LogRec],
                               onValue: PARSABLES_UNPARSABLES => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectContentSizes[NEXT](parsedRecSize: Long,
                                     onValue: SIZES => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectResponseCodes[NEXT](onValue: RES_CODE_COUNT_LIST => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectPopularHosts[NEXT](amtBeTaken: Int,
                                     onValue: RESRC_FREQ_LIST => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectPopularEndpts[NEXT](amtBeTaken: Int,
                                      onValue: RESRC_FREQ_LIST => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectErroroneousEndpts[NEXT](amtBeTaken: Int,
                                          onValue: RESRC_FREQ_LIST => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectDailyHostReqs[NEXT](onValue: DAILY_HOST_REQS => NEXT)
  extends AnalyzeLog[NEXT]

case class CollectNotFoundErrs[NEXT](amtBeTaken: Int,
                                     onValue: NOTFOUND_RESULT => NEXT)
  extends AnalyzeLog[NEXT]

//end of ADT

object AnalyzeLogCommonTypes {
  type NO_OF_PARSABLE_LOGS = Long
  type NO_OF_UNPARSABLE_LOGS = Long
  type PARSABLES_UNPARSABLES = (NO_OF_PARSABLE_LOGS, NO_OF_UNPARSABLE_LOGS)

  type AVG_SIZE = Long
  type MIN_SIZE = Long
  type MAX_SIZE = Long
  type SIZES = (AVG_SIZE, MIN_SIZE, MAX_SIZE)

  type RES_CODE = Int
  type RES_CODE_COUNT = Long
  type RES_CODE_COUNT_LIST = List[(RES_CODE, RES_CODE_COUNT)]

  type RESRC_FREQ = Int
  type RESRC_NAME = String
  type RESRC_FREQ_LIST = List[(RESRC_NAME, RESRC_FREQ)]

  type DAILY_HOST_FREQ_LIST = List[(RESRC_NAME, RESRC_FREQ)]
  type AVG_DAILY_HOST_FREQ_LIST = List[(RESRC_NAME, RESRC_FREQ)]
  type DAILY_HOST_REQS = (DAILY_HOST_FREQ_LIST, AVG_DAILY_HOST_FREQ_LIST)

  type NOTFOUND_FREQ = Long
  type NOTFOUND_ENDPT_LIST = RESRC_FREQ_LIST
  type NOTFOUND_HOSTS_LIST = RESRC_FREQ_LIST
  type NOTFOUND_DAYS_LIST = RESRC_FREQ_LIST
  type NOTFOUND_HRS_LIST = RESRC_FREQ_LIST
  type NOTFOUND_RESULT = 
  (NOTFOUND_FREQ, 
    NOTFOUND_ENDPT_LIST, 
    NOTFOUND_HOSTS_LIST, 
    NOTFOUND_DAYS_LIST, 
    NOTFOUND_HRS_LIST)
}

object FreeMonadTypes {
  type AnalyzeLogFC[NEXT] = Coyoneda[AnalyzeLog, NEXT]

  type FreeAnalyzeLog[NEXT] = Free[AnalyzeLogFC, NEXT]
}


case class DateTime(year: Int, month: Int, day: Int, hr: Int, min: Int, sec: Int) {
  private def prependZero(n: Int) = if (n < 10) s"0${n.toString}" else n.toString

  lazy val dateStr = s"$year-${prependZero(month)}-${prependZero(day)}"

}

case class LogRec(host: String,
                  clientId: String,
                  userId: String,
                  dateTime: DateTime,
                  method: String,
                  endpoint: String,
                  protocol: String,
                  responseCode: Int,
                  contentSize: Long)