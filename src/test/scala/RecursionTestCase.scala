/**
 * Issue: recursive functions behave weirdly
 */

//object RecursionTestCase {
//  def sumList(list: List[Int]): Int = {
//    list match {
//      case Nil => 0
//      case head::tail => head + sumList(tail)
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    sumList(List(1,2,3))
//  }
//
//}
