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
package com.streamxhub.repl.flink.interpreter

import java.util.Properties

import com.streamxhub.common.util.{ClassLoaderUtils, Logger}
import com.streamxhub.repl.flink.shims.FlinkShims
import org.apache.zeppelin.interpreter.Interpreter.FormType
import org.apache.zeppelin.interpreter.{Interpreter, InterpreterContext, InterpreterException, InterpreterResult}
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion


/**
 * Interpreter for flink scala. It delegates all the function to FlinkScalaInterpreter.
 */
class FlinkInterpreter(properties: Properties) extends Interpreter(properties) with Logger {

  private var interpreter: FlinkScalaInterpreter = _
  private var replContext: FlinkReplContext = _

  @throws[InterpreterException] private def checkScalaVersion(): Unit = {
    val scalaVersionString = scala.util.Properties.versionString
    logInfo("Using Scala: " + scalaVersionString)

    /*if (!scalaVersionString.contains("version 2")) {
      throw new InterpreterException("Unsupported scala version: " + scalaVersionString + ", Only scala 2.11 is supported")
    }*/
  }

  @throws[InterpreterException] override def open(): Unit = {
    checkScalaVersion()
    this.interpreter = new FlinkScalaInterpreter(getProperties)
    this.interpreter.open()
    this.replContext = this.interpreter.getReplContext
  }

  @throws[InterpreterException] override def close(): Unit = if (this.interpreter != null) this.interpreter.close()

  @throws[InterpreterException] override def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    logInfo(s"Interpret code: \n$code\n")
    this.replContext.setInterpreterContext(context)
    this.replContext.setGui(context.getGui)
    this.replContext.setNoteGui(context.getNoteGui)
    // set ClassLoader of current Thread to be the ClassLoader of Flink scala-shell,
    // otherwise codegen will fail to find classes defined in scala-shell
    ClassLoaderUtils.runAsClassLoader(getFlinkScalaShellLoader, () => {
      createPlannerAgain()
      setParallelismIfNecessary(context)
      setSavepointIfNecessary(context)
      interpreter.interpret(code, context)
    })
  }

  @throws[InterpreterException] override def cancel(context: InterpreterContext): Unit = {
    this.interpreter.cancel(context)
  }

  @throws[InterpreterException] override def getFormType = FormType.SIMPLE

  @throws[InterpreterException] override def getProgress(context: InterpreterContext): Int = this.interpreter.getProgress(context)

  @throws[InterpreterException] override def completion(buf: String, cursor: Int, interpreterContext: InterpreterContext): java.util.List[InterpreterCompletion] = interpreter.completion(buf, cursor, interpreterContext)

  private[flink] def getExecutionEnvironment = this.interpreter.getExecutionEnvironment()

  private[flink] def getStreamExecutionEnvironment = this.interpreter.getStreamExecutionEnvironment()

  private[flink] def getStreamTableEnvironment = this.interpreter.getStreamTableEnvironment("blink")

  private[flink] def getJavaBatchTableEnvironment(planner: String) = this.interpreter.getJavaBatchTableEnvironment(planner)

  private[flink] def getJavaStreamTableEnvironment(planner: String) = this.interpreter.getJavaStreamTableEnvironment(planner)

  private[flink] def getBatchTableEnvironment = this.interpreter.getBatchTableEnvironment("blink")

  private[flink] def getJobManager = this.interpreter.getJobManager

  private[flink] def getDefaultParallelism:Int = this.interpreter.defaultParallelism

  private[flink] def getDefaultSqlParallelism:Int = this.interpreter.defaultSqlParallelism

  /**
   * Workaround for issue of FLINK-16936.
   */
  def createPlannerAgain(): Unit = {
    this.interpreter.createPlannerAgain()
  }

  def getFlinkScalaShellLoader: ClassLoader = interpreter.getFlinkScalaShellLoader

  private[flink] def getReplContext = this.replContext

  private[flink] def getFlinkConfiguration = this.interpreter.getConfiguration

  def getInnerIntp: FlinkScalaInterpreter = this.interpreter

  def getFlinkShims: FlinkShims = this.interpreter.getFlinkShims

  def setSavepointIfNecessary(context: InterpreterContext): Unit = {
    this.interpreter.setSavepointPathIfNecessary(context)
  }

  def setParallelismIfNecessary(context: InterpreterContext): Unit = {
    this.interpreter.setParallelismIfNecessary(context)
  }

}
