import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.sql.expressions.Window

object app {
        def main(args: Array[String]): Unit = {
                val checkpointDirectory = "/Users/wassilaelfarh/IdeaProjects/TSapp/checkpoint"
                val csvFilePath = "/Users/wassilaelfarh/IdeaProjects/TSapp/input-split"
                val outputPath = "/Users/wassilaelfarh/IdeaProjects/TSapp/output"


                val spark = SparkSession.builder
                  .appName("Spark Streaming")
                  .master("local[*]")
                  .getOrCreate()

                import spark.implicits._


                val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => {
                        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
                        ssc.checkpoint(checkpointDirectory)

                        val csvDStream = ssc.textFileStream(csvFilePath)

                        csvDStream.foreachRDD { rdd =>
                                if (!rdd.isEmpty()) {

                                        val ds = spark.createDataset(rdd)


                                        val df = spark.read.option("header", "true").csv(ds)
                                        println("Original DataFrame:")
                                        df.show()


                                        val dfWithTimestamp = df
                                          .withColumn("Begin", to_timestamp(col("Begin"), "yyyyMMdd'T'HHmmss"))
                                          .withColumn("End", to_timestamp(col("End"), "yyyyMMdd'T'HHmmss"))
                                          .withColumn("LastUpdate", to_timestamp(col("Last Update"), "yyyyMMdd'T'HHmmss"))
                                        println("DataFrame with Timestamps:")
                                        dfWithTimestamp.show()


                                        val disruptionsByCause = df.groupBy("Cause").count()
                                        println("Disruptions by Cause:")
                                        disruptionsByCause.show()
                                        disruptionsByCause.write.csv(s"$outputPath/disruptionsByCause")


                                        val disruptionsBySeverity = df.groupBy("Severity").count()
                                        println("Disruptions by Severity:")
                                        disruptionsBySeverity.show()
                                        disruptionsBySeverity.write.csv(s"$outputPath/disruptionsBySeverity")

                                        val dfWithDuration = dfWithTimestamp.withColumn("Duration", col("End").cast("long") - col("Begin").cast("long"))
                                        println("DataFrame with Duration:")
                                        dfWithDuration.show()
                                        dfWithDuration.write.csv(s"$outputPath/duration")


                                        val durationStats = dfWithDuration.agg(
                                                avg("Duration").alias("AverageDuration"),
                                                max("Duration").alias("MaxDuration"),
                                                min("Duration").alias("MinDuration")
                                        )
                                        println("Duration Statistics:")
                                        durationStats.show()
                                        durationStats.write.csv(s"$outputPath/durationStats")


                                        val disruptionsByMonth = dfWithTimestamp.groupBy(month(col("Begin")).alias("Month")).count()
                                        println("Disruptions by Month:")
                                        disruptionsByMonth.show()
                                        disruptionsByMonth.write.csv(s"$outputPath/disruptionsByMonth")


                                        val disruptionsByDayOfWeek = dfWithTimestamp.groupBy(date_format(col("Begin"), "EEEE")).count()
                                        println("Disruptions by Day of Week:")
                                        disruptionsByDayOfWeek.show()
                                        disruptionsByDayOfWeek.write.csv(s"$outputPath/disruptionsByDayOfWeek")


                                        val disruptionsByHour = dfWithTimestamp.groupBy(hour(col("Begin")).alias("Hour")).count()
                                        println("Disruptions by Hour:")
                                        disruptionsByHour.show()
                                        disruptionsByHour.write.csv(s"$outputPath/disruptionsByHour")


                                        val currentDate = current_timestamp()
                                        val activeDisruptions = dfWithTimestamp.filter(col("End") > currentDate)
                                        println("Active Disruptions:")
                                        activeDisruptions.show()
                                        activeDisruptions.write.csv(s"$outputPath/activeDisruptions")


                                        val prolongedDisruptions = dfWithTimestamp.filter(col("LastUpdate") > col("Begin"))
                                        println("Prolonged Disruptions:")
                                        prolongedDisruptions.show()
                                        prolongedDisruptions.write.csv(s"$outputPath/prolongedDisruptions")


                                        val disruptionsByTitle = df.groupBy("Title").count()
                                        println("Disruptions by Title:")
                                        disruptionsByTitle.show()
                                        disruptionsByTitle.write.csv(s"$outputPath/disruptionsByTitle")


                                        val windowSpec = Window.orderBy("Begin")
                                        val dfWithLag = dfWithTimestamp.withColumn("PreviousEnd", lag("End", 1).over(windowSpec))
                                        val dfWithTimeBetween = dfWithLag.withColumn("TimeBetween", col("Begin").cast("long") - col("PreviousEnd").cast("long"))
                                        println("DataFrame with Time Between Disruptions:")
                                        dfWithTimeBetween.show()
                                        dfWithTimeBetween.write.csv(s"$outputPath/dfWithTimeBetween")


                                        val positiveTimeBetween = dfWithTimeBetween.filter(col("TimeBetween") > 0)
                                        println("Positive Time Between Disruptions:")
                                        positiveTimeBetween.show()
                                        positiveTimeBetween.write.csv(s"$outputPath/positiveTimeBetween")

                                        val averageTimeBetween = positiveTimeBetween.agg(avg("TimeBetween")).first().getDouble(0)
                                        println(s"Average Time Between Disruptions (seconds): $averageTimeBetween")


                                        val activeDisruptionsByCause = activeDisruptions.groupBy("Cause").count().withColumnRenamed("count", "active_count")
                                        val totalDisruptionsByCause = df.groupBy("Cause").count().withColumnRenamed("count", "total_count")
                                        val activePercentageByCause = activeDisruptionsByCause.join(totalDisruptionsByCause, "Cause")
                                          .withColumn("ActivePercentage", col("active_count") / col("total_count") * 100)
                                        println("Active Percentage by Cause:")
                                        activePercentageByCause.show()
                                        activePercentageByCause.write.csv(s"$outputPath/activePercentageByCause")

                                        val words = df.withColumn("word", explode(split(col("Title"), " ")))
                                        val keywordCounts = words.groupBy("word").count().orderBy(desc("count"))
                                        println("Keyword Counts in Titles:")
                                        keywordCounts.show()
                                        keywordCounts.write.csv(s"$outputPath/keywordCountsMessages")


                                        val titleWords = df.withColumn("titleWord", explode(split(col("Title"), "\\W+")))
                                        val titleKeywordCounts = titleWords.groupBy("titleWord").count().orderBy(desc("count"))
                                        println("Keyword Counts in Titles:")
                                        titleKeywordCounts.show()
                                        titleKeywordCounts.write.csv(s"$outputPath/titleKeywordCounts")

                                        val tagsAnalysis = df.groupBy("Tags").count()
                                        println("Tags Analysis:")
                                        tagsAnalysis.show()
                                        tagsAnalysis.write.csv(s"$outputPath/tagsAnalysis")

                                } else {
                                        println("Pas de nouvelles données.")
                                }
                        }

                        ssc
                })

                ssc.start()
                ssc.awaitTermination()
        }
}
