/*
 * Copyright (c) 2021 The StreamX Project
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

package com.streamxhub.streamx.common.fs

trait FsOperator {

  def exists(path: String): Boolean

  def mkdirs(path: String): Unit

  def delete(path: String): Unit

  def upload(srcPath: String, dstPath: String): Unit = upload(srcPath, dstPath, false, true)

  def copy(srcPath: String, dstPath: String): Unit = copy(srcPath, dstPath, false, true)

  def copyDir(srcPath: String, dstPath: String): Unit = copyDir(srcPath, dstPath, false, true)

  def upload(srcPath: String, dstPath: String, delSrc: Boolean = false, overwrite: Boolean = true): Unit

  def copy(srcPath: String, dstPath: String, delSrc: Boolean = false, overwrite: Boolean = true): Unit

  def copyDir(srcPath: String, dstPath: String, delSrc: Boolean = false, overwrite: Boolean = true): Unit

  def move(srcPath: String, dstPath: String): Unit

  def fileMd5(path: String): String


}
