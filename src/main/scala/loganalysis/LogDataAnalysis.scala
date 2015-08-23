package loganalysis

import FreeMonadTypes._
import scalaz.Free._
import AnalyzeLogCommonTypes._

object LogDataAnalysis {
  private def startBigDataApp(): FreeAnalyzeLog[Unit] =
    liftFC(StartBigDataApp(()))

  private def parseLogFile(fileName: String, parseFunc: String => Option[LogRec]): FreeAnalyzeLog[PARSABLES_UNPARSABLES] =
    liftFC(ParseLogFile(fileName, parseFunc, identity))

  private def collectContentSizes(parsedRecSize: Long): FreeAnalyzeLog[SIZES] =
    liftFC(CollectContentSizes(parsedRecSize, identity))

  private def collectResponseCode(): FreeAnalyzeLog[RES_CODE_COUNT_LIST] =
    liftFC(CollectResponseCodes(identity))

  private def collectPopularHosts(amtBeTaken: Int): FreeAnalyzeLog[RESRC_FREQ_LIST] =
    liftFC(CollectPopularHosts(amtBeTaken, identity))

  private def collectPopularEndpts(amtBeTaken: Int): FreeAnalyzeLog[RESRC_FREQ_LIST] =
    liftFC(CollectPopularEndpts(amtBeTaken, identity))

  private def collectErroroneousEndpts(amtBeTaken: Int): FreeAnalyzeLog[RESRC_FREQ_LIST] =
    liftFC(CollectErroroneousEndpts(amtBeTaken, identity))

  private def collectDailyHostReqs(): FreeAnalyzeLog[DAILY_HOST_REQS] =
    liftFC(CollectDailyHostReqs(identity))

  private def collectNotFoundErrs(amtBeTaken: Int): FreeAnalyzeLog[NOTFOUND_RESULT] =
    liftFC(CollectNotFoundErrs(amtBeTaken, identity))

  private def stopBigDataApp(): FreeAnalyzeLog[Unit] =
    liftFC(StopBigDataApp(()))

  def parseRawString(logLine: String): Option[LogRec] = {
    val LOG_MSG_FORMAT_PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)""".r
    logLine match {
      case LOG_MSG_FORMAT_PATTERN(host, clientId, userId, dt, method, endpoint, protocol, responseCode, contentSize) =>
        Some(LogRec(host, clientId, userId, date(dt), method, endpoint, protocol, responseCode.toInt,
          if (contentSize == "-") 0 else contentSize.toLong))
      case _ => None
    }
  }

  def date(dt: String): DateTime = {
    val months = Map(
      "Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6,
      "Jul" -> 7, "Aug" -> 8, "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)
    DateTime(dt.substring(7, 11).toInt,
      months(dt.substring(3, 6)),
      dt.substring(0, 2).toInt,
      dt.substring(12, 14).toInt,
      dt.substring(15, 17).toInt,
      dt.substring(18, 20).toInt)
  }

  def script(fileName: String) = {
    val amtBeTaken = 10
    for {
      _ <- startBigDataApp()
      parsedResult <- parseLogFile(fileName, parseRawString)
      (parsableCnt, unparsableCnt) = parsedResult
      cntSizes <- collectContentSizes(parsableCnt)
      (avgCntSize, minCntSize, maxCntSize) = cntSizes
      resCodeAmts <- collectResponseCode()
      top10Hosts <- collectPopularHosts(amtBeTaken)
      top10Endpts <- collectPopularEndpts(amtBeTaken)
      top10ErrEndpts <- collectErroroneousEndpts(amtBeTaken)
      dailyHostReqs <- collectDailyHostReqs()
      (dailyHosts, avgDailyHostReqs) = dailyHostReqs
      notFoundResult <- collectNotFoundErrs(amtBeTaken)
      (noOfNotFoundErrs, notFoundEndpts, notFoundHosts, notFoundDays, notFoundHrs) = notFoundResult
      _ <- stopBigDataApp()
    } yield
      println(
        s"""
           |$parsableCnt parsable lines, $unparsableCnt unparsable lines
           |
           |average requested content size = $avgCntSize, 
           |min requested content size = $minCntSize, 
           |max requested content size = $maxCntSize
           |
           |# of requests for different response codes:
           |${resCodeAmts mkString ","}
           |
           |$amtBeTaken most popular hosts:
           |${top10Hosts mkString ","}
           |
           |$amtBeTaken most popular endpoints:
           |${top10Endpts mkString ","}
           |
           |$amtBeTaken endpoints encountering most error:
           |${top10ErrEndpts mkString ","}
           |
           |# of hosts visited in the following dates:
           |${dailyHosts mkString ","}
           |
           |Average # of daily requests per host in the following dates:
           |${avgDailyHostReqs mkString ","}
           |
           |There are $noOfNotFoundErrs 404-error endpoints
           |
           |$amtBeTaken endpoints encountering most 404 error:
           |${notFoundEndpts mkString ","}
           |
           |$amtBeTaken hosts encountering most 404 error:
           |${notFoundHosts mkString ","}
           |
           |$amtBeTaken dates encountering most 404 error:
           |${notFoundDays mkString ","}
           |
           |$amtBeTaken hours encountering most 404 error:
           |${notFoundHrs mkString ","}
         """.stripMargin)
  }
}
