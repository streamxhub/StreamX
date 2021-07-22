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

import com.streamxhub.streamx.common.util.{HdfsUtils, Logger}

/**
 * Hadoop File System (aka HDFS) Operator
 */
object HdfsOperator extends FsOperator with Logger {

  override def exists(path: String): Boolean = HdfsUtils.exists(toHdfsPath(path))

  override def mkdirs(path: String): Unit = HdfsUtils.mkdirs(toHdfsPath(path))

  override def delete(path: String): Unit = HdfsUtils.delete(toHdfsPath(path))

  override def move(srcPath: String, dstPath: String): Unit = HdfsUtils.move(toHdfsPath(srcPath), toHdfsPath(dstPath))

  override def upload(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit =
    HdfsUtils.upload(toHdfsPath(srcPath), toHdfsPath(dstPath), delSrc = delSrc, overwrite = overwrite)

  override def copy(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit =
    HdfsUtils.copyHdfs(toHdfsPath(srcPath), toHdfsPath(dstPath), delSrc = delSrc, overwrite = overwrite)

  override def copyDir(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit =
    HdfsUtils.copyHdfsDir(toHdfsPath(srcPath), toHdfsPath(dstPath), delSrc = delSrc, overwrite = overwrite)

  override def fileMd5(path: String): String = HdfsUtils.fileMd5(toHdfsPath(path))

  private def toHdfsPath(path: String): String = {
    if (path.startsWith("hdfs://")) {
      return path
    }
    HdfsUtils.getDefaultFS.concat(path)
  }

}


