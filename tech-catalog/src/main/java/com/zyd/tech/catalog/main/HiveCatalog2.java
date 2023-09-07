package com.zyd.tech.catalog.main;

import com.zyd.tech.catalog.utils.FlinkUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * mysql binlog 数据实时写到adb
 */
public class HiveCatalog2 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔60000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(1000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        EnvironmentSettings Settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, Settings);
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setSqlDialect(SqlDialect.DEFAULT);

        Configuration flinkConf = FlinkUtil.getConfiguration(env);
        String appName = flinkConf.getString("yarn.application.name", "flink-cdc-test");
        tableConfig.getConfiguration().setString("pipeline.name",appName);

        System.setProperty("HADOOP_USER_NAME", "hive");
        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site1";
        String version         = "2.1.1";


        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);

        hiveCatalog.open();

//
        //获取funciton信息
        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "funEvl1"));

        System.out.println(elv2);



        System.err.printf("clName:%s\n" , Thread.currentThread().getContextClassLoader().getClass().getName() );

        tableEnv.registerCatalog(name,hiveCatalog);
        tableEnv.useCatalog(name);

        // 选择 database
        tableEnv.useDatabase(defaultDatabase);






        String selectSqlB = "select funEvl1(id,type) local_func1 from hive_catalog_7.catalog_test.flink_source_table_c2";

        tableEnv.sqlQuery(selectSqlB).execute().print();

    }
}
