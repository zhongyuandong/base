package com.zyd.tech.catalog.main;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class HiveCatalogQuery {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔60000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(6000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(6000);
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

        tableConfig.getConfiguration().setString("pipeline.name","flink-cdc-test");

        System.setProperty("HADOOP_USER_NAME", "hive");
        String name            = "hive_catalog_7";
        String defaultDatabase = "test0513_7";
        String hiveConfDir     = "D:/tmp/hive-site/";
        String version         = "2.1.1";

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);

        tableEnv.registerCatalog(name,hiveCatalog);
        tableEnv.useCatalog(name);

        // 选择 database
        tableEnv.useDatabase(defaultDatabase);

        tableEnv.getConfig().addJobParameter("hashcode_factor", "31");

        String selectSqlB = "select db8_Func0525_1(id,type),db8_Func0525_1(id,type,type),db8_Func0525_1(id,type,type,type) local_func1 from test0513_8.source_table1_8_1";

        selectSqlB = "select ap9f(id,type) from test0513_7.source_table1_7";
        selectSqlB = "select id,type from test0513_7.source_table1_7";


        tableEnv.sqlQuery(selectSqlB).execute().print();

    }
}
