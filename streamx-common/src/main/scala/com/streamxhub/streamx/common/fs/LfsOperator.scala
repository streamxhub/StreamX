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

import com.streamxhub.streamx.common.util.Logger
import com.streamxhub.streamx.common.util.Utils.isAnyBank
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.lang.StringUtils

import java.io.{File, FileInputStream}

/**
 * Local File System (aka LFS) Operator
 */
//noinspection DuplicatedCode
object LfsOperator extends FsOperator with Logger {

  override def exists(path: String): Boolean = {
    StringUtils.isNotBlank(path) && new File(path).exists()
  }

  override def mkdirs(path: String): Unit = {
    if (isAnyBank(path))
      return
    FileUtils.forceMkdir(new File(path))
  }

  override def delete(path: String): Unit = {
    if (StringUtils.isBlank(path))
      return
    val file = new File(path)
    if (!file.exists()) {
      logWarn(s"delete file: file is no exists, ${path}")
      return
    }
    FileUtils.forceDelete(file)
  }

  override def move(srcPath: String, dstPath: String): Unit = {
    if (isAnyBank(srcPath, dstPath)) {
      return
    }
    val srcFile = new File(srcPath)
    var dstFile = new File(dstPath)
    if (dstFile.isDirectory) {
      dstFile = new File(dstFile.getAbsolutePath.concat("/").concat(srcFile.getName))
    }
    if (srcFile.getCanonicalPath == dstFile.getCanonicalPath)
      return
    FileUtils.moveFile(srcFile, dstFile)
  }

  override def upload(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit = {
    if (new File(srcPath).isDirectory){
      copyDir(srcPath, dstPath, delSrc, overwrite)
    }else{
      copy(srcPath, dstPath, delSrc, overwrite)
    }
  }

  override def copy(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit = {
    if (isAnyBank(srcPath, dstPath))
      return
    val srcFile = new File(srcPath)
    var dstFile = new File(dstPath)
    if (dstFile.isDirectory) {
      dstFile = new File(dstFile.getAbsolutePath.concat("/").concat(srcFile.getName))
    }
    if (srcFile.getCanonicalPath == dstFile.getCanonicalPath)
      return
    if (!overwrite && dstFile.exists())
      return
    FileUtils.copyFile(srcFile, dstFile)
  }

  override def copyDir(srcPath: String, dstPath: String, delSrc: Boolean, overwrite: Boolean): Unit = {
    if (isAnyBank(srcPath, dstPath)) {
      return
    }
    val srcFile = new File(srcPath)
    val dstFile = new File(dstPath)
    if (srcFile.getCanonicalPath == dstFile.getCanonicalPath)
      return
    if (!overwrite && dstFile.exists())
      return
    FileUtils.copyDirectory(new File(srcPath), new File(dstPath))
  }

  override def fileMd5(path: String): String = {
    DigestUtils.md5Hex(IOUtils.toByteArray(new FileInputStream(path)))
  }
}



