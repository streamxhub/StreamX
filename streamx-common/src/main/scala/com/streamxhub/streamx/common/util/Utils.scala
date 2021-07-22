/*
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
package com.streamxhub.streamx.common.util

import org.apache.commons.lang.StringUtils

import java.io.{BufferedInputStream, File, FileInputStream, IOException}
import java.net.URL
import java.util.jar.{JarFile, JarInputStream}
import java.util.{Properties, UUID, jar, Collection => JavaCollection, Map => JavaMap}
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object Utils {

  private[this] val OS = System.getProperty("os.name").toLowerCase

  def notEmpty(elem: Any): Boolean = {
    elem match {
      case null => false
      case x if x.isInstanceOf[CharSequence] => elem.toString.trim.nonEmpty
      case x if x.isInstanceOf[Traversable[_]] => x.asInstanceOf[Traversable[_]].nonEmpty
      case x if x.isInstanceOf[Iterable[_]] => x.asInstanceOf[Iterable[_]].nonEmpty
      case x if x.isInstanceOf[JavaCollection[_]] => !x.asInstanceOf[JavaCollection[_]].isEmpty
      case x if x.isInstanceOf[JavaMap[_, _]] => !x.asInstanceOf[JavaMap[_, _]].isEmpty
      case _ => true
    }
  }

  def isEmpty(elem: Any): Boolean = !notEmpty(elem)

  def uuid(): String = UUID.randomUUID().toString.replaceAll("-", "")

  def require(requirement: Boolean, message: String) {
    if (!requirement)
      throw new IllegalArgumentException(s"requirement failed: $message")
  }

  @throws[IOException] def checkJarFile(jar: URL): Unit = {
    val jarFile: File = Try(new File(jar.toURI)) match {
      case Success(x) => x
      case Failure(_) => throw new IOException(s"JAR file path is invalid $jar")
    }
    if (!jarFile.exists) {
      throw new IOException(s"JAR file does not exist '${jarFile.getAbsolutePath}'")
    }
    if (!jarFile.canRead) {
      throw new IOException(s"JAR file can't be read '${jarFile.getAbsolutePath}'")
    }
    Try(new JarFile(jarFile)) match {
      case Failure(e) => throw new IOException(s"Error while opening jar file '${jarFile.getAbsolutePath}'", e)
      case Success(x) => x.close()
    }
  }

  def getJarManifest(jarFile: File): jar.Manifest = {
    checkJarFile(jarFile.toURL)
    new JarInputStream(new BufferedInputStream(new FileInputStream(jarFile))).getManifest
  }

  def copyProperties(original: Properties, target: Properties): Unit = original.foreach(x => target.put(x._1, x._2))

  //获取系统名字
  def getOsName: String = OS

  def isLinux: Boolean = OS.indexOf("linux") >= 0

  def isWindows: Boolean = OS.indexOf("windows") >= 0

  /**
   * if any blank strings exist
   */
  def isAnyBank(strs: String*): Boolean = strs == null || strs.exists(path => StringUtils.isBlank(path))

}
