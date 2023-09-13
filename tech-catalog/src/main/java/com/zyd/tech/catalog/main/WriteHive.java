package com.zyd.tech.catalog.main;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.Properties;

/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-13 15:42:36
 * @Version 1.0
 **/
public class WriteHive {

    public static void main(String[] args) {
        selectData();
    }

    public static void writeData (){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String hiveSql = "create external table  fs_tables (" +
                "  id string," +
                "  name String" +
                ") partitioned by (dt string) " +
                "stored as orc " +
                "tblproperties (" +
                "  'partition.time-extractor.timestamp-pattern'='$dt'," +
                "  'sink.partition-commit.delay'='0s'," +
                "  'sink.partition-commit.trigger'='partition-time'," +
                "  'sink.partition-commit.policy.kind'='metastore'" +
                ")";
        tableEnv.executeSql(hiveSql);
//        Table result = table.groupBy($("age")).select($("id"), $("name"), $("age"));
//        Table table = tableEnv.sqlQuery("select id,name,age from test.hive_test7  where age > 18");

        tableEnv.executeSql("insert into  fs_tables select id,name,age from test.test_data_type_1 ");
    }

    public static void selectData (){
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() // 使用BlinkPlanner
                .inBatchMode() // Batch模式，默认为StreamingMode
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";
        String version = "2.1.1";                               // Hive版本号
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);

        TableResult result;
        String SelectTables_sql ="SELECT * FROM test.test_data_type_1";
        result = tableEnv.executeSql(SelectTables_sql);
        result.print();
    }

}
