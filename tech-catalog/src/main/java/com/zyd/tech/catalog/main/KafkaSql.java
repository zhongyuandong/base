package com.zyd.tech.catalog.main;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.catalog.hive.HiveCatalog;

public class KafkaSql {

    public static void main(String[] args) throws Exception {

        Map<String, String> map = new HashMap<String, String>();
        map.put("table.exec.state.ttl", "1d");

        runFlinkSql("over_aggregation.sql", null, map);
        // 创建流式执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
//
//        // 注册Kafka连接器
////        tableEnv.executeSql("CREATE TABLE kafka_table (id INT, name STRING, age INT) " +
////                "WITH ('connector' = 'kafka', " +
////                "'topic' = 'k8s_test02', " +
////                "'properties.bootstrap.servers' = 'qa-kafka-cluster01:9093,qa-kafka-cluster02:9093,qa-kafka-cluster03:9093', " +
////                "'properties.group.id' = 'flink_consumer_group', " +
////                "'format' = 'json')");
//
//        tableEnv.executeSql("CREATE TABLE kafka_table (id INT, name STRING, age INT)\n" +
//                " with (\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'k8s_test01',\n" +
//                "  'format' = 'json',\n" +
//                "  'properties.bootstrap.servers' = 'qa-kafka-cluster01:9093,qa-kafka-cluster02:9093,qa-kafka-cluster03:9093',\n" +
//                "  'properties.group.id' = 'k8s_test0_202309121',\n" +
//                "  'scan.startup.mode' = 'group-offsets',\n" +
//                "  'scan.topic-partition-discovery.interval' = '1000',\n" +
//                "  'json.ignore-parse-errors' = 'true'\n" +
//                ")");
//
//        // 执行SQL查询并打印结果
//        tableEnv.executeSql("SELECT * FROM kafka_table").print();
//
//        // 执行任务
//        env.execute("Flink SQL Kafka Consumer Example");
    }

    public static void runFlinkSql(String sqlFile, Map<String, String> envConf, Map<String, String> tableEnvConf) throws Exception {
        Configuration envConfig = new Configuration();
        if (MapUtils.isNotEmpty(envConf)) {
            envConfig.addAll(Configuration.fromMap(envConf));
        }
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration tableEnvConfig = tEnv.getConfig().getConfiguration();
        if (MapUtils.isNotEmpty(tableEnvConf)) {
            tableEnvConfig.addAll(Configuration.fromMap(tableEnvConf));
        }

        String tableName = "default_catalog.default_database.source_connector_hermes_test";
        String allSql = "create table " + tableName + " (\n" +
                "    mdata row(\n" +
                "        device row(\n" +
                "            name varchar\n" +
                "        )\n" +
                "    ),\n" +
                "    sdata row(\n" +
                "        fault row(\n" +
                "            mcStatus varchar,\n" +
                "            mcStatusPre varchar,\n" +
                "            ctime int,\n" +
                "            timeDiff int\n" +
                "        )\n" +
                "    ),\n" +
                "    deviceNum as mdata.device.name,\n" +
                "    mcStatus as sdata.fault.mcStatus,\n" +
                "    mcStatusPre as sdata.fault.mcStatusPre,\n" +
                "    deviceTime as sdata.fault.ctime,\n" +
                "    timeDiff as sdata.fault.timeDiff,\n" +
                "    rowDay AS CAST(FROM_UNIXTIME(sdata.fault.ctime, 'yyyyMMdd') AS STRING),\n" +
                "    rowTime  AS TO_TIMESTAMP(FROM_UNIXTIME(sdata.fault.ctime, 'yyyy-MM-dd HH:mm:ss')),\n" +
                "    WATERMARK FOR rowTime AS rowTime\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'hermes_test',\n" +
                "  'format' = 'json',\n" +
                "  'properties.bootstrap.servers' = 'qa-cdh-001:9093,qa-cdh-002:9093,qa-cdh-003:9093',\n" +
                "  'properties.group.id' = 'hermes_grp_2',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'scan.topic-partition-discovery.interval' = '6s',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")";


        tEnv.executeSql(allSql).print();


        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";
//          String hiveConfDir     = "/opt/flink/conf/hive";
//        String hiveConfDir     = "/opt/bd-pb-data/flink_data/conf/hive";
        String version         = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);
        tEnv.registerCatalog(name,hiveCatalog);
        tEnv.useCatalog(name);
        String selectSqlB = "select funEvl1(deviceTime,mcStatus) aa, rowTime ,'20230915' a1, '09' a2 from " + tableName;
        tEnv.sqlQuery(selectSqlB).execute().print();
//        tEnv.executeSql("CREATE TABLE  default_catalog.default_database.ods_iot_test_di  (\n" +
//                " device_num string,\n" +
//                " cloud_time bigint,\n" +
//                " bdata string\n" +
//                ") PARTITIONED BY (namespace STRING, hh STRING) STORED AS orc TBLPROPERTIES  (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$namespace $hh:00:00',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai',\n" +
//                "  'sink.partition-commit.delay'='1 min',\n" +
//                "  'sink.partition-commit.policy.kind'='success-file'\n" +
//                ")");
//        tEnv.executeSql("SET table.sql-dialect=hive");
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//        String hiveTable = String.format("%s.%s.%s", name, defaultDatabase, "hive_table");
//        tEnv.executeSql("CREATE TABLE " + hiveTable + " (\n" +
//                "  user_id STRING,\n" +
//                "  order_amount INT\n" +
//                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.delay'='1 h',\n" +
//                "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
//                ")");
//        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
////        tEnv.executeSql("SET table.sql-dialect=default");
//        tEnv.executeSql("INSERT INTO " + hiveTable + " \n" +
//                "select funEvl1(deviceTime,mcStatus) aa, ctime ,'20230915' a1, '09' a2 \n" +
//                "FROM " + tableName).print();
//        tEnv.sqlQuery("select * from " + hiveTable).execute().print();
//        System.out.println("运行完成...");


    }
}
