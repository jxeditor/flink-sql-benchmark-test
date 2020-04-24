package com.ververica.flink.benchmark;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableUtils;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.hive.common.util.HiveVersionInfo;

import java.util.List;

/**
 * @author XiaShuai on 2020/4/24.
 */
public class TestMain {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig().getConfiguration().setBoolean(
                OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tEnv.getConfig().getConfiguration().setBoolean(
                OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false);
        tEnv.getConfig().getConfiguration().setLong(
                OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10485760L);
        tEnv.getConfig().getConfiguration().setInteger(
                ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        tEnv.getConfig().getConfiguration().setInteger(
                ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, 200);

        tEnv.getConfig().addConfiguration(GlobalConfiguration.loadConfiguration());

        HiveCatalog catalog = new HiveCatalog("hive", "default", "F:\\test\\flink-sql-benchmark\\flink-tpcds\\src\\main\\resources\\hive_conf", "2.1.1");
        catalog.getHiveConf().set("dfs.client.use.datanode.hostname", "true");
        tEnv.registerCatalog("hive", catalog);
        tEnv.useCatalog("hive");


        Table table = tEnv.sqlQuery("select * from test");

        List<Row> rows = TableUtils.collectToList(table);

        tEnv.execute("");
    }
}
