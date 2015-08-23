package loganalysis

import interpreter._
import loganalysis.FreeMonadTypes._
import loganalysis.AnalyzeLogInterpreter._

object Main extends App {
  if (args.length < 1)
    println("Please provide the data file name")
  else
    implicitly[Interpreter[FreeAnalyzeLog]].execute(LogDataAnalysis script s"./${args(0)}")
}
