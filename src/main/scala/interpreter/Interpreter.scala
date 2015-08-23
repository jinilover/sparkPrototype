package interpreter

import scalaz._

trait Interpreter[FREE[_]] {
  def execute[A](script: FREE[A]): Id.Id[_]
}