package assignment22


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.functions.{ udf, col } 
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{ StructField, StructType, DoubleType, StringType}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.ml.feature.MinMaxScaler
import Array.range
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.types
import org.apache.spark.storage.StorageLevel
import javax.xml.crypto.Data
import org.apache.spark.sql.Row


class Assignment {

  val spark: SparkSession = SparkSession.builder()
                          .appName("ex5")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()


      /* Defining dataschema for dataD2.csv */
      val data2Schema = StructType(
            List(
          StructField("a", DoubleType, true),
          StructField("b", DoubleType, true),
          StructField("LABEL", StringType, true),
          ))

      val data3Schema = StructType(
                  List(
                StructField("a", DoubleType, true),
                StructField("b", DoubleType, true),
                StructField("c", DoubleType, true),
                StructField("LABEL", StringType, true),
                ))

      // the data frame to be used in tasks 1 and 4
      /* 
        Passing manual schema to dataframe and
        also persisting data due to multiple calls 
      */
      val dataD2: DataFrame = spark.read
          .schema(data2Schema)
          .option("header", "true")
          .csv("./data/dataD2.csv")
          .persist(StorageLevel.MEMORY_ONLY)


      val dirtydata2: DataFrame = spark.read
          .option("inferSchema", "true")
          .option("header", "true")
          .csv("./data/dataD2_dirty.csv")

      // the data frame to be used in task 2
      val dataD3: DataFrame = spark.read
          .schema(data3Schema)
          .option("header", "true")
          .csv("./data/dataD3.csv")


      val stringIndexer = new StringIndexer()
          .setInputCol("LABEL")
          .setOutputCol("NUMERICLABEL")
      // the data frame to be used in task 3 (based on dataD2 but containing numeric labels)
      val dataD2WithLabels: DataFrame = stringIndexer.fit(dataD2).transform(dataD2)


  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {

    /* MLlib algorithms mostly take a vector as a input so initializing data with vectorassembler */
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("a","b"))
      .setOutputCol("features")

    /* Creatin scaler algoritm for normalizing vector data */
    val scaler = new MinMaxScaler()
      .setInputCol(vectorAssembler.getOutputCol)
      .setOutputCol("scaledFeatures")
      .setMin(0)
      .setMax(1)

    /* Creating K-means algorithm. 
    Presumably KMeans takes "features" as a default features column,
    so initializing it to scaler column.

    This functions parameter k is input for setK parameter.
    */
    val kmeans = new KMeans()
          .setK(k)
          .setSeed(1L)
          .setFeaturesCol(scaler.getOutputCol)

    /* Setting new pipeline for two earlier presented algorithms */
    val tpl = new Pipeline()
      .setStages(Array(vectorAssembler, scaler))

    /* Assembling data to suitable format with pipeline algorithms */
    val pipeline = tpl.fit(df)
    val transformedData = pipeline.transform(df)

    /* Fitting model (also predictions even these are not used) */
    val kmModel = kmeans.fit(transformedData)
    val predictions = kmModel.transform(transformedData)
    /* Fetching results from the model */
    val vector = kmModel.clusterCenters
    /* Casting Vector[Double, Double] as a Array[(Double, Double)] */
    val vectors: Array[(Double, Double)] = vector.map({ case v: Vector => (v(0), v(1))})
    return vectors
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    /* MLlib algorithms mostly take a vector as a input so initializing data with vectorassembler */
        val vectorAssembler = new VectorAssembler()
          .setInputCols(Array("a","b", "c"))
          .setOutputCol("features")

        /* Creatin scaler algoritm for normalizing vector data */
        val scaler = new MinMaxScaler()
          .setInputCol(vectorAssembler.getOutputCol)
          .setOutputCol("scaledFeatures")
          .setMin(0)
          .setMax(1)

        /* Creating K-means algorithm. 
        Presumably KMeans takes "features" as a default features column,
        so initializing it to scaler column.

        This functions parameter k is input for setK parameter.
        */
        val kmeans = new KMeans()
              .setK(k)
              .setSeed(1L)
              .setFeaturesCol(scaler.getOutputCol)

        /* Setting new pipeline for two earlier presented algorithms */
        val tpl = new Pipeline()
          .setStages(Array(vectorAssembler, scaler))

        /* Assembling data to suitable format with pipeline algorithms */
        val pipeline = tpl.fit(df)
        val transformedData = pipeline.transform(df)

        /* Fitting model (also predictions even these are not used) */
        val kmModel = kmeans.fit(transformedData)
        val predictions = kmModel.transform(transformedData)
        /* Fetching results from the model */
        val vector = kmModel.clusterCenters
        /* Casting Vector[Double, Double, Double] as a Array[(Double, Double, Double)] */
        val vectors: Array[(Double, Double, Double)] = vector.map({ case v: Vector => (v(0), v(1), v(2))})
        return vectors
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    /* MLlib algorithms mostly take a vector as a input so initializing data with vectorassembler */
            val vectorAssembler = new VectorAssembler()
              .setInputCols(Array("a","b", "NUMERICLABEL"))
              .setOutputCol("features")

            /* Creatin scaler algoritm for normalizing vector data */
            val scaler = new MinMaxScaler()
              .setInputCol(vectorAssembler.getOutputCol)
              .setOutputCol("scaledFeatures")
              .setMin(0)
              .setMax(1)

            /* Creating K-means algorithm. 
            Presumably KMeans takes "features" as a default features column,
            so initializing it to scaler column.

            This functions parameter k is input for setK parameter.
            */
            val kmeans = new KMeans()
                  .setK(k)
                  .setSeed(1L)
                  .setFeaturesCol(scaler.getOutputCol)

            /* Setting new pipeline for two earlier presented algorithms */
            val tpl = new Pipeline()
              .setStages(Array(vectorAssembler, scaler))

            /* Assembling data to suitable format with pipeline algorithms */
            val pipeline = tpl.fit(df)
            val transformedData = pipeline.transform(df)

            /* Fitting model (also predictions even these are not used) */
            val kmModel = kmeans.fit(transformedData)
            val predictions = kmModel.transform(transformedData)
            /* Fetching results from the model */
            val vector = kmModel.clusterCenters
            /* Casting Vector[Double, Double, Double] as a Array[(Double, Double, Double)] */
            val vectors: Array[(Double, Double)] = vector
              .map({ case v: Vector => (v(0), v(1), v(2))})
              .filter(_._3 != 0)
              .map(a => (a._1, a._2))
            return vectors
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    val kList: Array[Int] = range(low, high + 1)

    def silhouetteEvaluator(df: DataFrame, k: Int): (Int, Double) = {
      /* MLlib algorithms mostly take a vector as a input so initializing data with vectorassembler */
          val vectorAssembler = new VectorAssembler()
            .setInputCols(Array("a","b"))
            .setOutputCol("features")

          /* Creatin scaler algoritm for normalizing vector data */
          val scaler = new MinMaxScaler()
            .setInputCol(vectorAssembler.getOutputCol)
            .setOutputCol("scaledFeatures")
            .setMin(0)
            .setMax(1)

          /* Creating K-means algorithm. 
          Presumably KMeans takes "features" as a default features column,
          so initializing it to scaler column.

          This functions parameter k is input for setK parameter.
          */
          val kmeans = new KMeans()
                .setK(k)
                .setSeed(1)
                .setFeaturesCol(scaler.getOutputCol)

          /* Setting new pipeline for two earlier presented algorithms */
          val tpl = new Pipeline()
            .setStages(Array(vectorAssembler, scaler, kmeans))

          /* Assembling data to suitable format with pipeline algorithms */
          val pipeline = tpl.fit(df)
          val transformedData = pipeline.transform(df)
          val evaluator = new ClusteringEvaluator().setFeaturesCol(scaler.getOutputCol)
          val silhouette = evaluator.evaluate(transformedData)
          /* Fetching results from the model */
          val vector = pipeline.stages(2).asInstanceOf[KMeansModel].clusterCenters
          /* Casting Vector[Double, Double] as a Array[(Double, Double)] */
          val points: (Int, Double) = (k, silhouette)
          return points
    }

    val scores: Array[(Int, Double)] = kList.map(k => silhouetteEvaluator(df, k))

    return scores
  }
}
