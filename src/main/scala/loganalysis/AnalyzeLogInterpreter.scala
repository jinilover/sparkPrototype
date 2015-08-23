package loganalysis

import interpreter.Interpreter
import loganalysis.FreeMonadTypes.FreeAnalyzeLog
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scalaz._

object AnalyzeLogInterpreter {

  implicit object SparkAnalyzeLogInterpreter extends Interpreter[FreeAnalyzeLog] {
    type CachedRDD[NEXT] = State[MapKey Map RDD[LogRec], NEXT]

    lazy val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("LogAnalysis")
    )

    def getLargestItems(rdd: RDD[LogRec])(amtBeTaken: Int)(selectKey: LogRec => String): List[(String, Int)] =
      rdd
        .map(r => (selectKey(r), 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(amtBeTaken)
        .toList

    private def al2State = new (AnalyzeLog ~> CachedRDD) {
      def apply[NEXT](task: AnalyzeLog[NEXT]): CachedRDD[NEXT] =
        task match {
          case StartBigDataApp(next) =>
            State(m => (m, next))

          case ParseLogFile(file, parseFunc, onValue) =>
            val rawStrings: RDD[String] = sc.textFile(file)
            val parsedLogRecs: RDD[LogRec] = rawStrings
              .map(parseFunc)
              .filter(_.fold(false)(_ => true))
              .map(_.get)
              .cache()
            val returnedVals = (parsedLogRecs.count(), rawStrings.count() - parsedLogRecs.count())
            State {
              m =>
                (m + (ParsedLogRecsKey -> parsedLogRecs), onValue(returnedVals))
            }

          case CollectContentSizes(parsedRecSize, onValue) =>
            State {
              m =>
                val cntSizes: RDD[Long] = m(ParsedLogRecsKey)
                  .map(_.contentSize)
                  .cache()
                (m, onValue {
                  (cntSizes.reduce(_ + _) / parsedRecSize, cntSizes.min(), cntSizes.max())
                })
            }

          case CollectResponseCodes(onValue) =>
            State {
              m =>
                (m, onValue {
                  m(ParsedLogRecsKey)
                    .map(rec => (rec.responseCode, 1))
                    .countByKey()
                    .toList
                    .sortWith(_._1 < _._2)
                })
            }

          case CollectPopularHosts(amtBeTaken, onValue) =>
            State {
              m =>
                (m, onValue(getLargestItems(m(ParsedLogRecsKey))(amtBeTaken)(_.host)))
            }

          case CollectPopularEndpts(amtBeTaken, onValue) =>
            State {
              m =>
                (m, onValue(getLargestItems(m(ParsedLogRecsKey))(amtBeTaken)(_.endpoint)))
            }

          case CollectErroroneousEndpts(amtBeTaken, onValue) =>
            State {
              m =>
                (m, onValue {
                  getLargestItems(m(ParsedLogRecsKey).filter(_.responseCode != 200))(amtBeTaken)(_.endpoint)
                })
            }

          case CollectDailyHostReqs(onValue) =>
            State {
              m =>
                val reqAmtPerDayHost = m(ParsedLogRecsKey)
                  .map(rec => ((rec.dateTime.dateStr, rec.host), 1))
                  .reduceByKey(_ + _).cache()

                val hostAmtPerDayCollection = reqAmtPerDayHost
                  .map(tup => (tup._1._1, 1))
                  .reduceByKey(_ + _)
                  .sortByKey()
                  .collect()
                  .toList

                val reqHostAmtPerDayCollection = reqAmtPerDayHost
                  .map {
                  tup =>
                    val ((date, _), noOfReqsPerHostPerDay) = tup
                    (date, (1, noOfReqsPerHostPerDay))
                }.reduceByKey {
                  (t1, t2) =>
                    (t1._1 + t2._1, t1._2 + t2._2)
                }.mapValues(t => t._2 / t._1)
                  .sortByKey()
                  .collect()
                  .toList

                (m, onValue((hostAmtPerDayCollection, reqHostAmtPerDayCollection)))
            }

          case CollectNotFoundErrs(amtBeTaken, onValue) =>
            State {
              m =>
                val notFoundErrRecs = m(ParsedLogRecsKey).filter(_.responseCode == 404).cache()
                val topItems = getLargestItems(notFoundErrRecs)(amtBeTaken) _
                val notFoundEndpts = topItems(_.endpoint)
                val notFoundHosts = topItems(_.host)
                val notFoundDays = topItems(_.dateTime.dateStr)
                val notFoundHrs = topItems(_.dateTime.hr.toString)

                (m, onValue {
                  (notFoundErrRecs.count(), notFoundEndpts, notFoundHosts, notFoundDays, notFoundHrs)
                })
            }

          case StopBigDataApp(next) =>
            sc.stop()
            State(m => (m, next))
        }

    }

    def execute[A](script: FreeAnalyzeLog[A]): Id.Id[_] =
      Free.runFC(script)(al2State).exec(Map.empty[MapKey, RDD[LogRec]])
  }

}

sealed trait MapKey

case object ParsedLogRecsKey extends MapKey

case object NotFoundErrRecsKey extends MapKey
