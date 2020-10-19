package xke.local

import org.apache.spark.sql._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}

import scala.annotation.tailrec

object HelloWorld {
  def main(args: Array[String]): Unit = {
    /**
     * L'abus de join est mauvais pour la santé
     * Je me dis que je pourrais recycler ce repo pour en faire un slot sur l'optimisation des traitements sparks :)
     */
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val windowKed = Window.orderBy(col("year"), col("month"))
      .rowsBetween(-3, 0)
    val windowSpeaker = Window.partitionBy("speaker")
      .orderBy(col("year"), col("month"))
      .rowsBetween(-3, 0)
    val percentileUdf = udf(percentile(_, _, _))

    val df = spark.read.json("src/main/resources/data")

    val nbSlotsStats = nbSlot(df, windowKed).cache
    val speakersStatsPerKed = nbSlotPerSpeakerPerKED(df).cache
    val speakers = speakersStatsPerKed.select("speaker").distinct()

    val nbSlotPerSpeakerOver4MonthsWithNbSlots =
      nbSlotPerSpeakerOver4Months(df, speakersStatsPerKed, speakers, windowSpeaker)
        .join(nbSlotsStats.drop("nb_speaker", "nb_slots"), List("year", "month"), "left")
    val percentileOver4Month = percentileSpeakerOver4Month(nbSlotPerSpeakerOver4MonthsWithNbSlots, percentileUdf, 80)

    val nbUniqueSpeaker = uniqueSpeaker(speakersStatsPerKed)
    val speakerOver4MonthsDf = speakersOver4Months(df, windowKed)

    val uniqueSpeakerOver4MonthsDf = uniqueSpeakerOver4Months(speakersStatsPerKed, windowKed)
    val speakersStatsPerKedWithNbSlots = speakersStatsPerKed
      .join(nbSlotsStats.drop("nb_speaker", "nb_slots_over_4_months"), List("year", "month"), "left")
    val percentileSpeakers = percentileSpeaker(speakersStatsPerKedWithNbSlots, percentileUdf, 80)

    val toHoursMin = udf(formatHoursMin(_))
    val toSecond = udf((x: CalendarInterval) => x.microseconds / 1000000)
    val duration = durationPerKED(df, toHoursMin, toSecond)

    val res = nbSlotsStats
      .join(nbUniqueSpeaker, List("year", "month"), "left")
      .join(speakerOver4MonthsDf, List("year", "month"), "left")
      .join(uniqueSpeakerOver4MonthsDf, List("year", "month"), "left")
      .join(percentileSpeakers.drop("nb_slots").drop("nb_slot_per_person"), List("year", "month"), "left")
      .join(duration, List("year", "month"), "left")
      .join(percentileOver4Month, List("year", "month"), "left")
      .withColumn("date", concat(col("year"), lit("-"), col("month")))
      .sort("year", "month")

    res.coalesce(1)
      .write
      .option("header", true)
      .mode("OverWrite")
      .csv("data_ked")
  }

  def formatHoursMin(seconds: Long): String = {
    val minuts = seconds / 60
    val hours = seconds / 3600
    val rest = minuts - hours * 60
    s"${hours}h$rest"
  }

  def nbSlotPerSpeakerOver4Months(dfRaw: DataFrame, speakersStatsPerKed: DataFrame, speakers: DataFrame, window: WindowSpec): DataFrame = {
    val allSpeakerEveryKed = dfRaw
      .select("year", "month")
      .distinct()
      .crossJoin(speakers)
    val allSpeakerStatsPerKed = speakersStatsPerKed
      .join(allSpeakerEveryKed, List("year", "month", "speaker"), "outer")
      .na.fill(0)

    allSpeakerStatsPerKed
      .withColumn("nb_slot_per_speaker_over_4_months", sum("count") over window)
      .filter(col("nb_slot_per_speaker_over_4_months") > 0)
      .orderBy("year", "month", "nb_slot_per_speaker_over_4_months")
  }

  def durationPerKED(df: DataFrame, toHoursMin: UserDefinedFunction, toSecond: UserDefinedFunction): DataFrame = {
    df.filter(size(col("attendees")) > 0)
      .withColumn(
        "duration",
        col("endTime").cast("timestamp")
          -
          col("startTime").cast("timestamp")
      )
      .na.drop
      .withColumn("duration_in_second", toSecond(col("duration")))
      .select("year", "month", "duration_in_second")
      .groupBy("year", "month").sum("duration_in_second")
      .withColumn("duration_formatted", toHoursMin(col("sum(duration_in_second)")))
      .withColumn("duration_in_hours", col("sum(duration_in_second)") / 3600)
      .drop("sum(duration_in_second)")
  }

  def percentile(list: Seq[Int], max: Int, percentile: Int): Int = {
    @tailrec
    def percentileAcc(acc: Float, i: Int, list: Seq[Int]): Int = {
      if (list.isEmpty) i
      else if (acc < max.toFloat * percentile.toFloat / 100f) percentileAcc(acc + list.head, i + 1, list.tail)
      else i
    }
    percentileAcc(0f, 0, list)
  }

  def percentileSpeaker(df: DataFrame, percentileUdf: UserDefinedFunction, percentile: Int): Dataset[Row] = {
    df.groupBy("year", "month")
      .agg(
        sort_array(collect_list("count"), false).as("nb_slot_per_person"),
        max("nb_slots").as("nb_slots")
      )
      .withColumn(
        s"speaker_percentile_$percentile",
        percentileUdf(col("nb_slot_per_person"), col("nb_slots"), lit(percentile))
      )
      .sort("year", "month")
  }

  def percentileSpeakerOver4Month(df: DataFrame, percentileUdf: UserDefinedFunction, percentile: Int): Dataset[Row] = {
    df.groupBy("year", "month")
      .agg(
        sort_array(collect_list("nb_slot_per_speaker_over_4_months"), false).as("nb_slot_per_speaker_over_4_months"),
        max("nb_slot_over_4_months").as("nb_slot_over_4_months")
      )
      .withColumn(
        s"speaker_over_4_month_percentile_$percentile",
        percentileUdf(col("nb_slot_per_speaker_over_4_months"), col("nb_slot_over_4_months"), lit(percentile))
      )
      .sort("year", "month")
      .drop("nb_slot_per_speaker_over_4_months", "nb_slot_over_4_months")
  }

  def nbSlotPerSpeakerPerKED(df: DataFrame): DataFrame = {
    df.withColumn("speaker", explode(col("attendees")))
      .groupBy("year", "month", "speaker").count
  }

  def uniqueSpeakerOver4Months(df: DataFrame, window: WindowSpec): DataFrame = {
    df.groupBy("year", "month").count
      .withColumn("nb_speaker_unique_over_4_month",
        sum(col("count")) over window
      )
      .orderBy("year", "month")
      .drop("count")
  }

  def speakersOver4Months(df: DataFrame, window: WindowSpec): DataFrame = {
    df.withColumn("speaker", explode(col("attendees")))
      .groupBy("year", "month").count
      .withColumn(
        "nb_speaker_over_4_months",
        sum(col("count")) over window
      )
      .orderBy("year", "month")
      .drop("count")
  }

  def uniqueSpeaker(df: DataFrame): DataFrame = {
    df.groupBy("year", "month").count
      .sort("count")
      .withColumnRenamed("count", "nbUniqueSpeaker")
  }

  def nbSlot(df: DataFrame, window: WindowSpec): DataFrame = {
    df.withColumn("nb_speaker", size(col("attendees")))
      .groupBy("year", "month")
      .agg(
        sum("nb_speaker").as("nb_speaker"),
        count("summary").as("nb_slots")
      )
      .withColumn(
        "nb_slot_over_4_months",
        sum(col("nb_slots")) over window
      )
      .sort("year", "month")
  }
}
