package com.zyd.tech.catalog.main;

import com.alibaba.fastjson.JSON;
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
public class HiveCatalog3 {

    public static void main(String[] args) throws Exception {


        try {



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
        String hiveConfDir     = "D:\\tmp\\hive-site";
//        String hiveConfDir     = "/opt/flink/conf/hive";
        String version         = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);
        hiveCatalog.open();

//
        //获取function信息
        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "funEvl1"));

        System.out.println("获取function信息>>>>" + JSON.toJSONString(elv2));



        System.err.printf("clName:%s\n" , Thread.currentThread().getContextClassLoader().getClass().getName() );

        tableEnv.registerCatalog(name,hiveCatalog);
        tableEnv.useCatalog(name);

        long time = System.currentTimeMillis();

        // 选择 database
        tableEnv.useDatabase(defaultDatabase);

        String tableName = "hive_catalog_7.catalog_test.flink_source_table_" + time;

            String cdcTableC="CREATE TABLE " + tableName + " ( "
                    +"        id int NOT NULL, "
                    +"        num1 int, "
                    +"        num2 int, "
                    +"        str1 STRING, "
                    +"        str2 STRING, "
                    +"        name STRING, "
                    +"        type STRING, "
                    +"        primary key (id) not enforced"
                    +"    ) WITH ( "
                    +"       'connector' = 'mysql-cdc', "
                    +"       'hostname' = 'qa-cdh-002', "
                    +"       'port' = '3306', "
                    +"       'username' = 'root', "
                    +"       'password' = 'dsf!G13#dsd', "
//                +"       'is_generic' = 'false', "
                    +"       'database-name' = 'hive_catalog_source', "
                    +"       'table-name' = 'source_table3' "
                    +" )";

        tableEnv.executeSql(cdcTableC);





//        String selectSqlB = "select funEvl1(id,type) local_func1 from " + tableName;
//
//        tableEnv.sqlQuery(selectSqlB).execute().print();


            tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS catalog_test");
            tableEnv.executeSql("DROP TABLE IF EXISTS catalog_test.log_hive");

//            tableEnv.executeSql(" CREATE TABLE hive_tmp.log_hive (\n" +
//                    "                     user_id STRING,\n" +
//                    "                     name1 STRING \n" +
//                    "           ) PARTITIONED BY (\n" +
//                    "                     dt STRING,\n" +
//                    "                     hr STRING\n" +
//                    "           ) STORED AS PARQUET\n" +
//                    "             TBLPROPERTIES (\n" +
//                    "                    'sink.partition-commit.trigger' = 'partition-time',\n" +
//                    "                    'sink.partition-commit.delay' = '1 min',\n" +
//                    "                    'format' = 'json',\n" +
//                    "                    'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" +
//                    "                    'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'" +
//                    "           )");
            tableEnv.executeSql(" CREATE TABLE catalog_test.log_hive (user_id STRING,name1 STRING)");
            tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
//            tableEnv.executeSql("" +
//                    "        INSERT INTO hive_tmp.log_hive\n" +
//                    "        SELECT\n" +
//                    "               str1,\n" +
//                    "               str2,\n" +
//                    "               DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH')\n" +
//                    "               FROM " + tableName);
            tableEnv.executeSql("INSERT INTO catalog_test.log_hive SELECT str1, str2 FROM " + tableName);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("捕获到异常消息：" + e.getMessage());
        }

    }
}
