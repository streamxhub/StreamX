package com.streamxhub.flink.core.source

import com.streamxhub.common.conf.ConfigConst
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import com.streamxhub.flink.core.StreamingContext
import com.streamxhub.flink.core.util.FlinkConfigUtils

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.language.postfixOps

class KafkaSource(@transient val ctx: StreamingContext, specialKafkaParams: Map[String, String] = Map.empty[String, String]) {

  /**
   * 获取DStream 流
   *
   * @return
   */
  def getDataStream(topic:String = "",instance: String = ""): DataStream[String] = {
    val prop = FlinkConfigUtils.getKafkaSource(ctx.parameter, topic,instance)
    specialKafkaParams.foreach(x=>prop.put(x._1,x._2))
    val consumer = new FlinkKafkaConsumer011[String](prop.remove(ConfigConst.KEY_KAFKA_TOPIC).toString, new SimpleStringSchema(), prop)
    ctx.addSource(consumer)
  }
}