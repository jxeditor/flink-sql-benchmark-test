package com.test.benchmark

import org.apache.commons.cli.{CommandLine, DefaultParser, Option, Options}
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.hive.common.util.HiveVersionInfo

/**
  * @author XiaShuai on 2020/4/24.
  */
object GameBusiness {

  private val HIVE_CONF = new Option("c", "hive_conf", true, "conf of hive.")

  private val DATABASE = new Option("d", "database", true, "database of hive.")

  private val LOCATION = new Option("l", "location", true, "sql query path.")

  private val QUERIES = new Option("q", "queries", true, "sql query names. If the value is 'all', all queries will be executed.")

  private val ITERATIONS = new Option("i", "iterations", true, "The number of iterations that will be run per case, default is 1.")

  private val PARALLELISM = new Option("p", "parallelism", true, "The parallelism, default is 800.")

  def main(args: Array[String]): Unit = {

    val options = getOptions
    val parser = new DefaultParser
    val line = parser.parse(options, args, true)

    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.inBatchMode.build
    val tEnv = TableEnvironment.create(settings)

    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    tEnv.getConfig.getConfiguration.setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    tEnv.getConfig.getConfiguration.setLong(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10485760L)
    tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Integer.parseInt(line.getOptionValue(PARALLELISM.getOpt, "800")))
    tEnv.getConfig.getConfiguration.setInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, 200)

    tEnv.getConfig.addConfiguration(GlobalConfiguration.loadConfiguration)

    val catalog = new HiveCatalog("hive", line.getOptionValue(DATABASE.getOpt), line.getOptionValue(HIVE_CONF.getOpt), HiveVersionInfo.getVersion)
    tEnv.registerCatalog("hive", catalog)
    tEnv.useCatalog("hive")

    val table = tEnv.sqlQuery(
      s"""
         |SELECT rid, count(1) as total, sum(cast(p['t_t4_d'] as int)) as pay
         |FROM  game_ods.event
         |WHERE app   = 'game_skuld_01'
         |  AND dt    = '2019-08-16'
         |  AND event = 'event_role.pay_3_d'
         |GROUP BY rid
         |""".stripMargin)

    tEnv.sqlUpdate(
      s"""
         |insert overwrite test
         |select rid
         |from $table
         |""".stripMargin)

    tEnv.execute("test")
  }

  private def getOptions = {
    val options = new Options
    options.addOption(HIVE_CONF)
    options.addOption(DATABASE)
    options.addOption(LOCATION)
    options.addOption(QUERIES)
    options.addOption(ITERATIONS)
    options.addOption(PARALLELISM)
    options
  }
}
