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
package com.streamxhub.streamx.common.conf

import com.streamxhub.streamx.common.enums.StorageType

object ConfigConst {
  /**
   *
   * about parameter...
   */

  val KEY_APP_HOME = "app.home"

  val KEY_HOST = "host"

  val KEY_PORT = "port"

  val KEY_DB = "db"

  val KEY_USER = "user"

  val KEY_PASSWORD = "password"

  val KEY_TIMEOUT = "timeout"

  val KEY_JOB_ID = "jobId"

  val KEY_SEMANTIC = "semantic"

  val KEY_STREAMX_CONSOLE_URL = "streamx.console.url"

  /**
   * sign....
   */
  val SIGN_COLON = ":"

  val SIGN_SEMICOLON = ";"

  val SIGN_COMMA = ","

  val SIGN_EMPTY = ""

  /**
   * kerberos
   */
  val KEY_KERBEROS = "kerberos"

  /**
   * hadoop.security.authentication
   */
  val KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication"

  val KEY_SECURITY_KERBEROS_ENABLE = "security.kerberos.login.enable"

  val KEY_SECURITY_KERBEROS_KEYTAB = "security.kerberos.login.keytab"

  val KEY_SECURITY_KERBEROS_PRINCIPAL = "security.kerberos.login.principal"

  val KEY_SECURITY_KERBEROS_KRB5_CONF = "security.kerberos.login.krb5"

  val KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf"

  //spark

  val KEY_SPARK_USER_ARGS = "spark.user.args"

  val KEY_SPARK_CONF = "spark.conf"

  val KEY_SPARK_DEBUG_CONF = "spark.debug.conf"

  val KEY_SPARK_MAIN_CLASS = "spark.main.class"

  val KEY_SPARK_APP_NAME = "spark.app.name"

  val KEY_SPARK_BATCH_DURATION = "spark.batch.duration"

  // flink
  def KEY_APP_CONF(prefix: String = null): String = if (prefix == null) "conf" else s"${prefix}conf"

  def KEY_FLINK_CONF(prefix: String = null): String = if (prefix == null) "flink.conf" else s"${prefix}flink.conf"

  def KEY_APP_NAME(prefix: String = null): String = if (prefix == null) "app.name" else s"${prefix}app.name"

  def KEY_FLINK_SQL(prefix: String = null): String = if (prefix == null) "sql" else s"${prefix}sql"

  def KEY_FLINK_PARALLELISM(prefix: String = null): String = if (prefix == null) "parallelism.default" else s"${prefix}parallelism.default"

  val KEY_FLINK_DEPLOYMENT_PROPERTY_PREFIX = "flink.deployment.property."

  val KEY_FLINK_DEPLOYMENT_OPTION_PREFIX = "flink.deployment.option."

  val KEY_FLINK_APP_NAME = "yarn.application.name"

  // --checkpoints--
  val KEY_FLINK_CHECKPOINTS_ENABLE = "flink.checkpoints.enable"

  val KEY_FLINK_CHECKPOINTS_UNALIGNED = "flink.checkpoints.unaligned"

  val KEY_FLINK_CHECKPOINTS_INTERVAL = "flink.checkpoints.interval"

  val KEY_FLINK_CHECKPOINTS_MODE = "flink.checkpoints.mode"

  val KEY_FLINK_CHECKPOINTS_CLEANUP = "flink.checkpoints.cleanup"

  val KEY_FLINK_CHECKPOINTS_TIMEOUT = "flink.checkpoints.timeout"

  val KEY_FLINK_CHECKPOINTS_MAX_CONCURRENT = "flink.checkpoints.maxConcurrent"

  val KEY_FLINK_CHECKPOINTS_MIN_PAUSEBETWEEN = "flink.checkpoints.minPauseBetween"

  //---state---

  val KEY_FLINK_STATE_CHECKPOINTS_DIR = "flink.state.checkpoints.dir"

  val KEY_FLINK_STATE_CHECKPOINT_STORAGE = "flink.state.checkpoint-storage"

  val KEY_FLINK_STATE_BACKEND = "flink.state.backend.value"

  val KEY_FLINK_STATE_BACKEND_ASYNC = "flink.state.backend.async"

  val KEY_FLINK_STATE_BACKEND_INCREMENTAL = "flink.state.backend.incremental"

  val KEY_FLINK_STATE_BACKEND_MEMORY = "flink.state.backend.memory"

  val KEY_FLINK_STATE_ROCKSDB = "flink.state.backend.rocksdb"

  //---restart-strategy---

  val KEY_FLINK_RESTART_STRATEGY = "flink.restart-strategy.value"

  val KEY_FLINK_RESTART_STRATEGY_FAILURE_RATE_PER_INTERVAL = "flink.restart-strategy.failure-rate.max-failures-per-interval"

  val KEY_FLINK_RESTART_STRATEGY_FAILURE_RATE_RATE_INTERVAL = "flink.restart-strategy.failure-rate.failure-rate-interval"

  val KEY_FLINK_RESTART_STRATEGY_FAILURE_RATE_DELAY = "flink.restart-strategy.failure-rate.delay"

  val KEY_FLINK_RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS = "flink.restart-strategy.fixed-delay.attempts"

  val KEY_FLINK_RESTART_STRATEGY_FIXED_DELAY_DELAY = "flink.restart-strategy.fixed-delay.delay"

  val KEY_EXECUTION_RUNTIME_MODE = "flink.execution.runtime-mode"

  val KEY_FLINK_WATERMARK_INTERVAL = "flink.watermark.interval"

  // ---watermark---
  val KEY_FLINK_WATERMARK_TIME_CHARACTERISTIC = "flink.watermark.time.characteristic"

  // ---table---
  val KEY_FLINK_TABLE_PLANNER = "flink.table.planner"

  val KEY_FLINK_TABLE_MODE = "flink.table.mode"

  val KEY_FLINK_TABLE_CATALOG = "flink.table.catalog"

  val KEY_FLINK_TABLE_DATABASE = "flink.table.database"

  /**
   * about config Kafka
   */

  val KAFKA_SINK_PREFIX = "kafka.sink."

  val KAFKA_SOURCE_PREFIX = "kafka.source."

  val KEY_KAFKA_TOPIC = "topic"

  val KEY_KAFKA_SEMANTIC = "semantic"

  val KEY_KAFKA_PATTERN = "pattern"

  val KEY_KAFKA_START_FROM = "start.from"

  val KEY_KAFKA_START_FROM_OFFSET = "offset"

  val KEY_KAFKA_START_FROM_TIMESTAMP = "timestamp"

  val REDIS_PREFIX = "redis."

  val KEY_ALIAS = "alias"

  /**
   * about config jdbc...
   */
  val KEY_JDBC_PREFIX = "jdbc."
  val KEY_JDBC_DRIVER = "driverClassName"
  val KEY_JDBC_DATABASE = "database"
  val KEY_JDBC_URL = "jdbcUrl"
  val KEY_JDBC_USER = "username"
  val KEY_JDBC_PASSWORD = "password"
  val KEY_JDBC_INSERT_BATCH = "batch.size"
  val DEFAULT_JDBC_INSERT_BATCH = 1

  val MONGO_PREFIX = "mongodb."
  /**
   * about config HBase
   */
  val HBASE_PREFIX = "hbase."

  val KEY_HBASE_COMMIT_BATCH = "hbase.commit.batch"

  val KEY_HBASE_WRITE_SIZE = "hbase.client.write.size"

  val DEFAULT_HBASE_COMMIT_BATCH = 1000

  val KEY_HBASE_AUTH_USER = "hbase.auth.user"

  val DEFAULT_HBASE_WRITE_SIZE = 1024 * 1024 * 10

  /**
   * about influx
   */
  val INFLUX_PREFIX = "influx."
  val KEY_INFLUX_ACTIONS = "actions"
  val KEY_INFLUX_FLUSH_DURATION = "flush.duration"
  /**
   * about clickhouse
   */
  val CLICKHOUSE_SINK_PREFIX = "clickhouse.sink"

  val HTTP_SINK_PREFIX = "http.sink"

  /**
   * sink threshold and failover...
   */
  val KEY_SINK_THRESHOLD_BUFFER_SIZE: String = "threshold.bufferSize"
  val KEY_SINK_THRESHOLD_NUM_WRITERS: String = "threshold.numWriters"
  val KEY_SINK_THRESHOLD_QUEUE_CAPACITY: String = "threshold.queueCapacity"
  val KEY_SINK_THRESHOLD_DELAY_TIME: String = "threshold.delayTime"
  val KEY_SINK_THRESHOLD_REQ_TIMEOUT: String = "threshold.requestTimeout"
  val KEY_SINK_THRESHOLD_RETRIES: String = "threshold.retries"
  val KEY_SINK_THRESHOLD_SUCCESS_CODE: String = "threshold.successCode"

  val KEY_SINK_FAILOVER_TABLE: String = "failover.table"
  val KEY_SINK_FAILOVER_STORAGE: String = "failover.storage"

  val DEFAULT_SINK_REQUEST_TIMEOUT = 2000
  val DEFAULT_HTTP_SUCCESS_CODE = 200
  val DEFAULT_SINK_THRESHOLD_QUEUE_CAPACITY = 10000
  val DEFAULT_SINK_THRESHOLD_DELAY_TIME = 1000L
  val DEFAULT_SINK_THRESHOLD_BUFFER_SIZE = 1000
  val DEFAULT_SINK_THRESHOLD_RETRIES = 3
  val DEFAULT_SINK_THRESHOLD_NUM_WRITERS: Int = Runtime.getRuntime.availableProcessors()

  /**
   * about config es
   */
  val ES_PREFIX = "es.sink."

  val KEY_ES_AUTH_USER = "es.auth.user"

  val KEY_ES_AUTH_PASSWORD = "es.auth.password"

  val KEY_ES_REST_MAX_RETRY = "es.rest.max.retry.timeout"

  val KEY_ES_REST_CONTENT_TYPE = "es.rest.content.type"

  val KEY_ES_CONN_REQ_TIME_OUT = "es.connect.request.timeout"

  val KEY_ES_CONN_TIME_OUT = "es.connect.timeout"

  val KEY_ES_CLUSTER_NAME = "es.cluster.name"

  val KEY_ES_BULK_PREFIX = "bulk.flush."

  val KEY_ES_CLIENT_TRANSPORT_SNIFF = "client.transport.sniff"

  val KEY_STREAMX_WORKSPACE = "streamx.workspace.path"

  val STREAMX_WORKSPACE_DEFAULT = "/streamx"

  val KEY_STREAMX_WORKSPACE_TYPE = "streamx.workspace.type"

  /**
   * optional value from {@link StorageType}
   */
  val STREAMX_WORKSPACE_TYPE_DEFAULT = "lfs"

  lazy val WORKSPACE: String = System.getProperties.getProperty(KEY_STREAMX_WORKSPACE, STREAMX_WORKSPACE_DEFAULT)

  lazy val WORKSPACE_TYPE: StorageType =
    StorageType.of(System.getProperties.getProperty(KEY_STREAMX_WORKSPACE_TYPE, STREAMX_WORKSPACE_TYPE_DEFAULT))

  lazy val APP_PLUGINS = s"$WORKSPACE/plugins"

  /**
   * 存放不同版本flink相关的jar
   */
  lazy val APP_SHIMS = s"$WORKSPACE/shims"

  lazy val APP_UPLOADS = s"$WORKSPACE/uploads"

  lazy val APP_WORKSPACE = s"$WORKSPACE/workspace"

  lazy val APP_FLINK = s"$WORKSPACE/flink"

  lazy val APP_BACKUPS = s"$WORKSPACE/backups"

  lazy val APP_SAVEPOINTS = s"$WORKSPACE/savepoints"

  /**
   * 存放全局公共的jar
   */
  lazy val APP_JARS = s"$WORKSPACE/jars"

  val LOGO =
    """
      |
      |                         ▒▓██▓██▒
      |                     ▓████▒▒█▓▒▓███▓▒
      |                  ▓███▓░░        ▒▒▒▓██▒  ▒
      |                ░██▒   ▒▒▓▓█▓▓▒░      ▒████
      |                ██▒         ░▒▓███▒    ▒█▒█▒
      |                  ░▓█            ███   ▓░▒██
      |                    ▓█       ▒▒▒▒▒▓██▓░▒░▓▓█
      |                  █░ █   ▒▒░       ███▓▓█ ▒█▒▒▒
      |                  ████░   ▒▓█▓      ██▒▒▒ ▓███▒
      |               ░▒█▓▓██       ▓█▒    ▓█▒▓██▓ ░█░
      |         ▓░▒▓████▒ ██         ▒█    █▓░▒█▒░▒█▒
      |        ███▓░██▓  ▓█           █   █▓ ▒▓█▓▓█▒
      |      ░██▓  ░█░            █  █▒ ▒█████▓▒ ██▓░▒
      |     ███░ ░ █░          ▓ ░█ █████▒░░    ░█░▓  ▓░
      |    ██▓█ ▒▒▓▒          ▓███████▓░       ▒█▒ ▒▓ ▓██▓
      | ▒██▓ ▓█ █▓█       ░▒█████▓▓▒░         ██▒▒  █ ▒  ▓█▒
      | ▓█▓  ▓█ ██▓ ░▓▓▓▓▓▓▓▒              ▒██▓           ░█▒
      | ▓█    █ ▓███▓▒░              ░▓▓▓███▓          ░▒░ ▓█
      | ██▓    ██▒    ░▒▓▓███▓▓▓▓▓██████▓▒            ▓███  █
      |▓███▒ ███   ░▓▓▒░░   ░▓████▓░                  ░▒▓▒  █▓
      |█▓▒▒▓▓██  ░▒▒░░░▒▒▒▒▓██▓░                            █▓
      |██ ▓░▒█   ▓▓▓▓▒░░  ▒█▓       ▒▓▓██▓    ▓▒          ▒▒▓
      |▓█▓ ▓▒█  █▓░  ░▒▓▓██▒            ░▓█▒   ▒▒▒░▒▒▓█████▒
      | ██░ ▓█▒█▒  ▒▓▓▒  ▓█                █░      ░░░░   ░█▒
      | ▓█   ▒█▓   ░     █░                ▒█              █▓
      |  █▓   ██         █░                 ▓▓        ▒█▓▓▓▒█░
      |   █▓ ░▓██░       ▓▒                  ▓█▓▒░░░▒▓█░    ▒█
      |    ██   ▓█▓░      ▒                    ░▒█▒██▒      ▓▓
      |     ▓█▒   ▒█▓▒░                         ▒▒ █▒█▓▒▒░░▒██
      |      ░██▒    ▒▓▓▒                     ▓██▓▒█▒ ░▓▓▓▓▒█▓
      |        ░▓██▒                          ▓░  ▒█▓█  ░░▒▒▒
      |            ▒▓▓▓▓▓▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒░░▓▓  ▓░▒█░
      |
      |
      |            ____ __                          _  __
      |           / __// /_ ____ ___  ___ _ __ _   | |/_/
      |          _\ \ / __// __// -_)/ _ `//  ' \ _>  <
      |         /___/ \__//_/   \__/ \_,_//_/_/_//_/|_|
      |
      |
      |          [StreamX] Make Flink|Spark easier ô‿ô!
      |
      |""".stripMargin


}



