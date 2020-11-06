package com.streamxhub.repl.flink.interpreter

import java.io.{ByteArrayOutputStream, IOException, OutputStream}

import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

class InterpreterOutput(flushListener: FlushListener) extends OutputStream {

  private val buffer = new ByteArrayOutputStream
  private val lineBuffer = new ArrayBuffer[Byte]()
  private val NEW_LINE_CHAR = '\n'

  def clear(): Unit = buffer.reset()

  override def write(b: Int): Unit = {
    buffer.write(b)
    lineBuffer += b.toByte
    if (b == NEW_LINE_CHAR) {
      flushLine()
    }
  }

  def flushLine(): Unit = {
    val line = new String(lineBuffer.toArray)
    lineBuffer.clear()
    if (flushListener != null && line.nonEmpty) {
      flushListener.onLine(line)
    }
  }

  @throws[IOException] override def write(b: Array[Byte]): Unit = write(b, 0, b.length)

  override def write(b: Array[Byte], off: Int, len: Int): Unit = for (i <- off until len) write(b(i))

  private[this] def toByteArray(): Array[Byte] = buffer.toByteArray

  @throws[IOException] override def close(): Unit = {
    flush()
    flushLine()
  }

  override def toString() = new String(toByteArray)

}


private[interpreter] class InterpreterOutputStream(logger: Logger) extends LogOutputStream {

  private[this] var interpreterOutput: InterpreterOutput = null
  private[this] var ignore: Boolean = false

  def getInterpreterOutput: InterpreterOutput = interpreterOutput

  def setInterpreterOutput(interpreterOutput: InterpreterOutput): Unit = {
    this.interpreterOutput = interpreterOutput
  }

  @throws[IOException] override def write(b: Int): Unit = {
    if (ignore && b == '\n') {
      val error = Thread.currentThread.getStackTrace.filter(x => x.getClassName == "scala.tools.nsc.interpreter.ReplReporter" && x.getMethodName == "error")
      if (error.nonEmpty) {
        return
      }
    } else {
      ignore = false
    }
    super.write(b)
    if (interpreterOutput != null) {
      interpreterOutput.write(b)
    }
  }

  @throws[IOException] override def write(b: Array[Byte]): Unit = write(b, 0, b.length)

  @throws[IOException] override def write(b: Array[Byte], off: Int, len: Int): Unit = for (i <- off until len) write(b(i))

  @Override def processLine(s: String, i: Int): Unit = {
    logger.debug("Interpreter output:" + s)
  }

  @throws[IOException] override def close(): Unit = {
    super.close()
    if (interpreterOutput != null) interpreterOutput.close()
  }

  @throws[IOException] override def flush(): Unit = {
    super.flush()
    if (interpreterOutput != null) interpreterOutput.flush()
  }

  def ignoreLeadingNewLinesFromScalaReporter(): Unit = ignore = true

}

private[this] abstract class LogOutputStream extends OutputStream {
  private val buffer: ByteArrayOutputStream = new ByteArrayOutputStream(132)
  private var skip = false
  private var level = 999

  def this(level: Int) {
    this()
    this.level = level
  }

  @throws[IOException] override def write(cc: Int): Unit = {
    val c = cc.toByte
    if (c != 10 && c != 13) {
      buffer.write(cc)
    } else if (!this.skip) {
      this.processBuffer()
    }
    this.skip = c == 13
  }

  @throws[IOException] override def flush(): Unit = {
    if (this.buffer.size > 0) this.processBuffer()
  }

  @throws[IOException] override def close(): Unit = {
    if (this.buffer.size > 0) this.processBuffer()
    super.close()
  }

  @throws[IOException] override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    var offset = off
    var blockStartOffset = off
    var remaining = len
    while (remaining > 0) {
      while (remaining > 0 && b(offset) != 10 && b(offset) != 13) {
        offset += 1
        remaining -= 1
      }
      val blockLength = offset - blockStartOffset
      if (blockLength > 0) {
        this.buffer.write(b, blockStartOffset, blockLength)
      }
      while (remaining > 0 && (b(offset) == 10 || b(offset) == 13)) {
        this.write(b(offset))
        offset += 1
        remaining -= 1
      }
      blockStartOffset = offset
    }
  }

  protected def processBuffer(): Unit = {
    this.processLine(this.buffer.toString)
    this.buffer.reset()
  }

  def getMessageLevel: Int = this.level

  protected def processLine(line: String): Unit = this.processLine(line, this.level)

  protected def processLine(var1: String, var2: Int): Unit
}

trait FlushListener {
  /**
   * 注意:返回的数据,千万不能调用println打印,因为此时的Console.out,System.out,System.err已经被interpreter占用,
   * 专门用来输出执行flink代码的信息
   * 如果显示的调用println,会导致递归...
   */
  def onLine(line: String)
}

