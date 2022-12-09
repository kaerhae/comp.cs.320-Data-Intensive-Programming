package ex5

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.annotation.tailrec


object Ex5Main extends App {
	val spark = SparkSession.builder()
                          .appName("ex5")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("WARN")

  // There are three scientific articles in the directory src/main/resources/articles/
  // The call sc.textFile(...) returns an RDD consisting of the lines of the articles:
  val articlesRdd: RDD[String] = sc.textFile("src/main/resources/articles/*")


  printTaskLine(1)
  // Task #1: How do you get the first 10 lines as an Array?
  val lines10: Array[String] = articlesRdd.take(10)
  lines10.foreach(println)


  printTaskLine(2)
  // Task #2: Compute how many lines there are in total in the articles.
  //          And then count the total number of words in the articles
  //          You can assume that words in each line are separated by the space character (i.e. " ")
  val nbrOfLines: Long = articlesRdd.count()
  println(s"#lines = ${nbrOfLines}")

  /* 
     ---------- AUTHORS COMMENT -----------
    In variable 'words':
      1) Flattening and splitting RDD with space
      2) Filtering out all empty string from array
  */
  val words: Long = articlesRdd
    .flatMap(a => a.split(" "))
    .filter(a => !a.isEmpty).count()
  println(s"#words = ${words}")


  printTaskLine(3)
  // Task #3: What is the count of non-white space characters? (it is enough to count the non " "-characters for this)
  //          And how many numerical characters are there in total? (i.e., 0, 1, 2, ..., 9 characters)

  /* 
   ---------- AUTHORS COMMENT -----------
  Variable 'chars' does following:
      1) Flattening every character into list
      2) Filters out all whitespace characters (also tabs linebreaks etc.)

  Variable 'numchars' flattens every character into list and filters all digits
  */
  val chars: Long = articlesRdd.flatMap(_.toList).filter(a => !a.isWhitespace).count()
  println(s"#chars = ${chars}")

  val numChars: Long = articlesRdd.flatMap(_.toList).filter(a => a.isDigit).count()
  println(s"#numChars = ${numChars}")


  printTaskLine(4)
  // Task #4: How many 5-character words that are not "DisCo" are there in the corpus?
  //          And what is the most often appearing 5-character word (that is not "DisCo") and how many times does it appear?
  
  /* 
   ---------- AUTHORS COMMENT -----------
  Variable 'words5Count' does following:
      1) Flattening and splitting every word by whitespace into list
      2) Filters out all "DisCo" and other than 5-letter words
      3) Returns count
  */
  val words5Count: Long = articlesRdd
    .flatMap(a => a.split(" "))
    .filter(a => a.length == 5)
    .filter(a => a != "DisCo")
    .count()
  println(s"5-character words: ${words5Count}")
  /* 
   ---------- AUTHORS COMMENT -----------
  Variable 'commonWord' does following:
      1) Flattening and splitting every word by whitespace into list
      2) Filters out all "DisCo" and other than 5-letter words
      3) Maps every word into tuple of (String, Int): (word, 1)
      4) Reduces list by same word and counts occurences into tuples second Int item
      5) With maxBy reduces list into most biggest second tuple item value
      6) Returns first tuple item, the string
  */
  val commonWord: String = articlesRdd
    .flatMap(a => a.split(" "))
    .filter(a => a.length == 5)
    .filter(a => a != "DisCo")
    .map(a => (a, 1))
    .reduceByKey(_+_)
    .collect
    .maxBy(_._2)
    ._1

  /* 
   ---------- AUTHORS COMMENT -----------
  Variable 'commonWordCount' does following:
      1) Flattening and splitting every word by whitespace into list
      2) Filters out all "DisCo" and other than most frequent word (string variable from previous)
      3) Maps every word into tuple of (String, Int): (word, 1)
      4) Reduces list by same word and counts occurences into tuples second Int item
      5) With maxBy reduces list into most biggest second tuple item value
      6) Returns second tuple item, integer
  */
  val commonWordCount: Int = articlesRdd
    .flatMap(a => a.split(" "))
    .filter(a => a == commonWord)
    .filter(a => a != "DisCo")
    .map(a => (a, 1))
    .reduceByKey(_+_)
    .collect
    .maxBy(_._2)
    ._2

  println(s"The most common word is '${commonWord}' and it appears ${commonWordCount} times")


  // You are given a factorization function that returns the prime factors for a given number:
  // For example, factorization(28) would return List(2, 2, 7)
  def factorization(number: Int): List[Int] = {
    @tailrec
    def checkFactor(currentNumber: Int, factor: Int, factorList: List[Int]): List[Int] = {
      if (currentNumber == 1) factorList
      else if (factor * factor > currentNumber) factorList :+ currentNumber
      else if (currentNumber % factor == 0) checkFactor(currentNumber / factor, factor, factorList :+ factor)
      else checkFactor(currentNumber, factor + 1, factorList)
    }

    if (number < 2) List(1)
    else checkFactor(number, 2, List.empty)
  }


  printTaskLine(5)
  // Task #5: You are given a sequence of integers and a factorization function.
  //          Using them create a pair RDD that contains the integers and their prime factors.
  //          Get all the distinct prime factors from the RDD.

  /* 
   ---------- AUTHORS COMMENT -----------
  Variable 'factorRdd' does following:
      1) Map function for values is mapping Seq[Int] values and returning list of pair RDD where mapped Seq 'values'
        is the first item and second item is factorization function with mapped item as input
      2) Finally parallelized into RDD
  */
  val values: Seq[Int] = 12.to(17) ++ 123.to(127) ++ 1234.to(1237)
  val factorRdd: RDD[(Int, List[Int])] = sc.parallelize(values.map(a => (a, factorization(a))))
  factorRdd.collect().foreach({case (n, factors) => println(s"$n: ${factors.mkString(",")}")})

  /* 
   ---------- AUTHORS COMMENT -----------
  Variable 'distinctPrimes' does following:
      1) Flattening second item of RDD[(Int, List[Int])] into RDD[Int]
      2) Returning elements of previous operation with .collect
      3) Removing duplicates from RDD[Int] with .distinct
      4) Converting implicitly into List[Int]
      2) Finally sorted List[Int]
  */
  val distinctPrimes: List[Int] = factorRdd
    .flatMap({ case (a, b) => b})
    .collect
    .distinct
    .toList
    .sorted
  println(s"distinct primes: ${distinctPrimes.mkString(", ")}")

  printTaskLine(6)
  // Task #6: Here is a code snippet. Explain how it works.
 val lyricsRdd = sc.textFile("lyrics/*.txt")
  val lyricsCount = lyricsRdd.flatMap(line => line.split(" "))
                             .map(word => (word, 1))
                             .reduceByKey((v1, v2) => v1 + v2)

  lyricsCount.collect().foreach(println)

/* 
   ---------- AUTHORS COMMENT -----------
  Task #6 is doing the following:
      1) Is reading presumably all text files from folder "lyrics" and creating a RDD with SparkContext
      2) Flattening lines of the text file and separating all words from the text files
       (assuming that words can be separated by whitespace)
      3) Mapping words into pair RDD (String, Int)
      4) pair RDD is basicly a list of tuples, as list of (key, value) items, 
      so reduceByKey is aggregating list items and { ... => v1 + v2 } is comparing all same key values and 
      summing values. Basicly same thing as with {.reduceByKey(_+_)}
  */

  def printTaskLine(taskNumber: Int): Unit = {
    println(s"======\nTask $taskNumber\n======")
  }
}
