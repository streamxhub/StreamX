package com.streamxhub.flink.core.util

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time


object WatermarkUtils {

  def boundedOutOfOrdernessWatermark[T](fun: T => Long)(implicit maxOutOfOrderness: Time): AssignerWithPeriodicWatermarks[T] = {
    new BoundedOutOfOrdernessTimestampExtractor[T](maxOutOfOrderness) {
      override def extractTimestamp(element: T): Long =  fun(element)
    }
  }

  def timeLagWatermark[T](fun: T => Long)(implicit maxTimeLag: Long): AssignerWithPeriodicWatermarks[T] = {
    new AssignerWithPeriodicWatermarks[T] {
      override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = fun(element)

      override def getCurrentWatermark: Watermark = new Watermark(System.currentTimeMillis() - maxTimeLag)
    }
  }

  def punctuatedWatermark[T](maxTimeLag: Long)(implicit fun: T => Long, f1: T => Boolean): AssignerWithPunctuatedWatermarks[T] = {
    new AssignerWithPunctuatedWatermarks[T] {
      override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = fun(element)

      override def checkAndGetNextWatermark(lastElement: T, extractedTimestamp: Long): Watermark = {
        if (f1(lastElement)) new Watermark(extractedTimestamp) else null
      }
    }
  }

}