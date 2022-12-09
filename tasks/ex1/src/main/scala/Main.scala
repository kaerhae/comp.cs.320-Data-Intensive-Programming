import scala.math._

object Methods {
  /* METHODS */

  /* EXERCISE 1.1 */
  val mySum = (a: Int, b: Int) => a + b

  /* EXERCISE 1.2 */
  def pascal(row: Int, colum: Int): Int = {
    if (colum == 0 || colum == row) {
      1
    }
    else {
      pascal(row - 1, colum - 1) + pascal(row - 1, colum)
    }
  }


  /* EXERCISE 1.3 */
  def timesOpen(char: Char, isOpen: Int): Int = {
    if (char == '(') {
      isOpen + 1
    } else if (char == ')') {
      isOpen - 1
    } else {
      isOpen
    }
  }

  def balance(chars: List[Char]): Boolean = {
    def isBalanced(chars: List[Char], isOpen: Int): Boolean = {
      if (chars.isEmpty) {
        isOpen == 0
      }
      else {
        val firstChar = chars.head
        val n = timesOpen(firstChar, isOpen)
        if (n >= 0) isBalanced(chars.tail, n)
        else false
      }
    }

    isBalanced(chars, 0)
  }

  /* EXERCISE 1.4 */
  def sumOfSquares(list: Array[Int]): Int = {
    val sum: Int = list
      .map(x => x * x)
      .reduce(_ + _)
    return sum
  }

  /* EXERCISE 1.6 */
  def sqrt(x: Double): Double = {
    def newtonsMethod(x: Double, guessValue: Double): Double = {
      val method = 0.5 * (guessValue + x / guessValue)

      if (Math.abs(guessValue - method) < 0.01) {
        method
      } else {
        newtonsMethod(x, method)
      }
    }
    newtonsMethod(x, 1.0)
  }
}

object Main extends App {

  /* EXERCISE 1.1 */
  val s = Methods.mySum(20,21)
  println(s)

  /* EXERCISE 1.2 */
  println(Methods.pascal(4, 2))

  /* EXERCISE 1.3 */
  println(Methods.balance(List('a','(',')',')')))

  /* EXERCISE 1.4 */
  val a = Array(1,2,3,4,5)
  println(Methods.sumOfSquares(a))



  /* EXERCISE 1.5 */

  /*
      First ramonesString function sequences is doing following things:
       1) Splits string by " " (space)
       2) Map list of string to list of tuple
       3) Groups by tuples first item
       4) Maps items by length
   */

  val ramonesString = "sheena is a punk rocker she is a punk punk"
    .split(" ")
    .map(s => (s, 1))
    .groupBy(p => p._1)
    .mapValues(v => v.length)
  // Note, if you get mapValues is deprecated error use the version below: (might be required for Scala 2.13 and above)
  val ramonesString2 = "sheena is a punk rocker she is a punk punk"
    .split(" ")
    .map(s => (s, 1))
    .groupBy(p => p._1)
    .map({case(v1, v2) => (v1, v2.length)})


  /*

      Second ramonesString functions sequence is doing following:
      1) Splits by space char
      2) Maps list of string to list of tuples
      3) Groups by tuples first item
      4) Maps tuples to tuples second values
      5) Returns sum of values
   */

  "sheena is a punk rocker she is a punk punk"
    .split(" ")
    .map((_, 1))
    .groupBy(_._1)
    .mapValues(v => v.map(_._2)
      .reduce(_ + _))
  // Note, if you get mapValues is deprecated error use the version below: (for (might be required Scala 2.13 and above)
  // "sheena is a punk rocker she is a punk punk".split(" " ).map((_, 1)).groupBy(_._1).map({case(v1, v2) => (v1, v2.map(_._2).reduce(_+_))})



    /* EXERCISE 1.6 */
  println(Methods.sqrt(2))
  println(Methods.sqrt(100))
  println(Methods.sqrt(1e-16))
  println(Methods.sqrt(1e60))

}


