package com.zyd.tech.catalog.main;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

public class KafkaToHive {

    public static void main(String[] args) {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        senv.setParallelism(3);
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(senv, tableEnvSettings);

        //
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));

        String catalogName            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";
//        String hiveConfDir     = "/opt/flink/conf/hive";
        String version         = "2.1.1";
        HiveCatalog catalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir,version);

        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS stream_tmp");
        tableEnv.executeSql("DROP TABLE IF EXISTS stream_tmp.log_kafka");

//        tableEnv.executeSql("create table stream_tmp.log_kafka(" +
//                "user_id String,\n" +
//                "order_amount Double,\n" +
//                "log_ts Timestamp(3),\n" +
//                "WATERMARK FOR log_ts AS log_ts -INTERVAL '5' SECOND" +
//                ")WITH(" +
//                " 'connector' = 'kafka',\n" +
//                "'topic' = 'test',\n" +
//                " 'properties.bootstrap.servers' = 'qa-kafka-cluster01:9093,qa-kafka-cluster02:9093,qa-kafka-cluster03:9093',\n" +
//                "'properties.group.id' = 'flink1',\n" +
//                "'scan.startup.mode' = 'earliest-offset',\n" +
//                "'format' = 'json',\n" +
//                "'json.fail-on-missing-field' = 'false',\n" +
//                "'json.ignore-parse-errors' = 'true'" +
//                ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        long time = System.currentTimeMillis();

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

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS hive_tmp");
        tableEnv.executeSql("DROP TABLE IF EXISTS hive_tmp.log_hive");

        tableEnv.executeSql(" CREATE TABLE hive_tmp.log_hive (\n" +
                "                     user_id STRING,\n" +
                "                     order_amount STRING\n" +
                "           ) PARTITIONED BY (\n" +
                "                     dt STRING,\n" +
                "                     hr STRING\n" +
                "           ) STORED AS PARQUET\n" +
                "             TBLPROPERTIES (\n" +
                "                    'sink.partition-commit.trigger' = 'partition-time',\n" +
                "                    'sink.partition-commit.delay' = '1 min',\n" +
                "                    'format' = 'json',\n" +
                "                    'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" +
                "                    'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'" +
                "           )");
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("" +
                "        INSERT INTO hive_tmp.log_hive\n" +
                "        SELECT\n" +
                "               str1,\n" +
                "               str2,\n" +
                "               '20230914', '09'\n" +
                "               FROM " + tableName);
    }

}
