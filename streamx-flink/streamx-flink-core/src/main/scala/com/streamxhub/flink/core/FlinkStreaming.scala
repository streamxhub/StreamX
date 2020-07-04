/**
 * Copyright (c) 2019 The StreamX Project
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.streamxhub.flink.core

import com.streamxhub.common.conf.ConfigConst._
import com.streamxhub.common.util.{Logger, SystemPropertyUtils}
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import com.streamxhub.flink.core.ext.{DataStreamExt, ProcessFuncContextExt}
import org.apache.flink.streaming.api.functions.ProcessFunction


/**
 * @author benjobs
 * @param parameter
 * @param environment
 */
class StreamingContext(val parameter: ParameterTool, val environment: StreamExecutionEnvironment) extends StreamExecutionEnvironment(environment.getJavaEnv) {

  def this(flinkInitializer: FlinkInitializer) = {
    this(flinkInitializer.parameter, flinkInitializer.initStreamEnv())
  }

  override def execute(): JobExecutionResult = {
    val appName = parameter.get(KEY_FLINK_APP_NAME, "")
    execute(appName)
  }

  override def execute(jobName: String): JobExecutionResult = {
    println(s"\033[95;1m$LOGO\033[1m\n")
    println(s"[StreamX] FlinkStreaming $jobName Starting...")
    super.execute(jobName)
  }
}


trait FlinkStreaming extends Logger {

  final implicit def streamExt[T: TypeInformation](dataStream: DataStream[T]): DataStreamExt[T] = new DataStreamExt(dataStream)

  final implicit def procFuncExt[IN: TypeInformation, OUT: TypeInformation](ctx: ProcessFunction[IN, OUT]#Context): ProcessFuncContextExt[IN, OUT] = new ProcessFuncContextExt[IN, OUT](ctx)

  @transient
  private var env: StreamExecutionEnvironment = _

  private var parameter: ParameterTool = _

  private var context: StreamingContext = _

  final def main(args: Array[String]): Unit = {
    SystemPropertyUtils.setAppHome(KEY_APP_HOME, classOf[FlinkStreaming])
    //init......
    val initializer = new FlinkInitializer(args, config)
    parameter = initializer.parameter
    env = initializer.initStreamEnv()
    context = new StreamingContext(parameter, env)
    //
    beforeStart(context)
    handler(context)
    context.execute()
  }

  /**
   * 用户可覆盖次方法...
   *
   */
  def beforeStart(context: StreamingContext): Unit = {}

  def config(env: StreamExecutionEnvironment, parameter: ParameterTool): Unit = {}

  def handler(context: StreamingContext): Unit

}



