package com.zyd.tech.catalog.main;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.HashMap;
import java.util.Map;

public class Kafka2Hive {

    public static void main(String[] args) throws Exception {

        Map<String, String> map = new HashMap<String, String>();
        map.put("table.exec.state.ttl", "1d");

        runFlinkSql("over_aggregation.sql", null, map);
    }

    public static void runFlinkSql(String sqlFile, Map<String, String> envConf, Map<String, String> tableEnvConf) throws Exception {
        Configuration envConfig = new Configuration();
        if (MapUtils.isNotEmpty(envConf)) {
            envConfig.addAll(Configuration.fromMap(envConf));
        }
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
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

//        String sqlSet = "SET table.sql-dialect=hive";
//
//        String createHive = "CREATE TABLE hive_table (\n" +
//                "  user_id STRING,\n" +
//                "  order_amount DOUBLE\n" +
//                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
//                "  'sink.partition-commit.trigger'='partition-time',\n" +
//                "  'sink.partition-commit.delay'='1 h',\n" +
//                "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" +
//                ")";
//        tEnv.executeSql(sqlSet).print();
//        tEnv.executeSql(createHive).print();


        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";
//          String hiveConfDir     = "/opt/flink/conf/hive";
//        String hiveConfDir     = "/opt/bd-pb-data/flink_data/conf/hive";
        String version         = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);
        hiveCatalog.open();

        //获取function信息
        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "funEvl1"));

        System.out.println("获取function信息>>>>" + JSON.toJSONString(elv2));
        tEnv.registerCatalog(name,hiveCatalog);
        tEnv.useCatalog(name);
        tEnv.useDatabase("testdb");
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // 创建Hive表
//        tEnv.executeSql("CREATE TABLE ods_iot_test_di (" +
//                "  user_id STRING, " +
//                "  order_amount int " +
//                ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (" +
//                "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00'," +
//                "  'sink.partition-commit.trigger'='partition-time'," +
//                "  'sink.partition-commit.delay'='1 h'," +
//                "  'sink.partition-commit.watermark-time-zone'='Asia/Shanghai'," +
//                "  'sink.partition-commit.policy.kind'='metastore,success-file'" +
//                ")");
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tEnv.executeSql("CREATE TABLE ods_iot_test_di (" +
                "  user_id STRING, " +
                "  order_amount int " +
                ")");
        // 将Kafka数据写入Hive
//        tEnv.executeSql("INSERT INTO default_catalog.default_database.ods_iot_test_di \n" +
//                "SELECT funEvl1(deviceTime,mcStatus) aa, ctime , FROM_UNIXTIME(deviceTime, 'yyyy-MM-dd'),FROM_UNIXTIME(deviceTime, 'HH') \n" +
//                "FROM " + tableName).print();
        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tEnv.executeSql("INSERT INTO ods_iot_test_di \n" +
                "SELECT mcStatus aa, ctime  \n" +
                "FROM " + tableName).print();

//        String insertHive = "INSERT INTO TABLE hive_table \n" +
//                "SELECT mcStatusPre, funEvl1(deviceTime,mcStatus), '20230915', '12'\n" +
//                "FROM " + tableName;
//        tEnv.sqlQuery(insertHive).execute().print();

        System.out.println("运行完成...");

    }
}
