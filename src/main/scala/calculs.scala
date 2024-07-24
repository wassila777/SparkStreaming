import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window




object calculs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Static CSV Example")
      .master("local[*]")
      .getOrCreate()


    println("Spark session created")


    val csvFilePath = "/Users/wassilaelfarh/IdeaProjects/TSapp/input/Disruptions_Data.csv"


    val schema = new StructType()
      .add("Disruption ID", StringType, true)
      .add("Begin", StringType, true)
      .add("End", StringType, true)
      .add("Last Update", StringType, true)
      .add("Cause", StringType, true)
      .add("Severity", StringType, true)
      .add("Title", StringType, true)
      .add("Message", StringType, true)
      .add("Tags", StringType, true)



    val df = spark.read
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ",")
      .csv(csvFilePath)



    df.printSchema()



    val dfWithTimestamp = df
      .withColumn("Begin", to_timestamp(col("Begin"), "yyyyMMdd'T'HHmmss"))
      .withColumn("End", to_timestamp(col("End"), "yyyyMMdd'T'HHmmss"))
      .withColumn("LastUpdate", to_timestamp(col("Last Update"), "yyyyMMdd'T'HHmmss"))


    val disruptionsByCause = df.groupBy("Cause").count()
    disruptionsByCause.show()


    val disruptionsBySeverity = df.groupBy("Severity").count()
    disruptionsBySeverity.show()


    val dfWithDuration = dfWithTimestamp.withColumn("Duration", col("End").cast("long") - col("Begin").cast("long"))


    val durationStats = dfWithDuration.agg(
      avg("Duration").alias("AverageDuration"),
      max("Duration").alias("MaxDuration"),
      min("Duration").alias("MinDuration")
    )
    durationStats.show()


    val disruptionsByMonth = dfWithTimestamp.groupBy(month(col("Begin")).alias("Month")).count()
    disruptionsByMonth.show()


    val disruptionsByDayOfWeek = dfWithTimestamp.groupBy(date_format(col("Begin"), "EEEE")).count()
    disruptionsByDayOfWeek.show()


    val disruptionsByHour = dfWithTimestamp.groupBy(hour(col("Begin")).alias("Hour")).count()
    disruptionsByHour.show()


    val currentDate = current_timestamp()
    val activeDisruptions = dfWithTimestamp.filter(col("End") > currentDate)
    activeDisruptions.show()


    val prolongedDisruptions = dfWithTimestamp.filter(col("LastUpdate") > col("Begin"))
    prolongedDisruptions.show()


    val disruptionsByTitle = df.groupBy("Title").count()
    disruptionsByTitle.show()


    val windowSpec = Window.orderBy("Begin")
    val dfWithLag = dfWithTimestamp.withColumn("PreviousEnd", lag("End", 1).over(windowSpec))
    val dfWithTimeBetween = dfWithLag.withColumn("TimeBetween", col("Begin").cast("long") - col("PreviousEnd").cast("long"))


    val positiveTimeBetween = dfWithTimeBetween.filter(col("TimeBetween") > 0)
    val averageTimeBetween = positiveTimeBetween.agg(avg("TimeBetween")).first().getDouble(0)
    println(s"Average Time Between Disruptions (seconds): $averageTimeBetween")


    val activeDisruptionsByCause = activeDisruptions.groupBy("Cause").count().withColumnRenamed("count", "active_count")
    val totalDisruptionsByCause = df.groupBy("Cause").count().withColumnRenamed("count", "total_count")
    val activePercentageByCause = activeDisruptionsByCause.join(totalDisruptionsByCause, "Cause")
      .withColumn("ActivePercentage", col("active_count") / col("total_count") * 100)
    activePercentageByCause.show()


    val words = df.withColumn("word", explode(split(col("Title"), " ")))
    val keywordCounts = words.groupBy("word").count().orderBy(desc("count"))
    keywordCounts.show()

    spark.stop()
    println("Spark session stopped")
  }
}