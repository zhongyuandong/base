package com.hj.flink.demo.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;


/**
 * mysql binlog 数据实时写到adb
 */
public class HiveCatalog5 {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name = "hive_catalog_hj";
        String defaultDatabase = "catalog_test";
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, null);
        tableEnv.registerCatalog("hive_catalog_hj", (Catalog)hive);
        tableEnv.useCatalog("hive_catalog_hj");
        String source = "CREATE TABLE  default_catalog.default_database.flink_source_table(         id int NOT NULL,         num1 int,         num2 int,         str1 STRING,         str2 STRING,         name STRING,         type STRING,         primary key (id) not enforced    ) WITH (        'connector' = 'mysql-cdc',        'hostname' = 'qa-cdh-002',        'port' = '3306',        'username' = 'root',        'password' = 'dsf!G13#dsd',        'database-name' = 'hive_catalog_source',        'table-name' = 'source_table3'  )";
        String mysqlJdbcConnector = "create table default_catalog.default_database.flink_sink_table (\n        id int NOT NULL,         num1 int,         num2 int,         str1 STRING,         str2 STRING,         name STRING,         type STRING,         primary key (id) not enforced ) with (\n  'connector' = 'jdbc',\n  'url' = 'jdbc:mysql://am-bp14e9ue7iv58r1ru167320.ads.aliyuncs.com:3306/share_basic?useSSL=false&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=Asia/Shanghai&autoReconnect=true&useServerPrepStmts=false&rewriteBatchedStatements=true',\n  'username' = 'admin_uat',\n  'password' = 'JNrGPcRDzuKN8lik4B1aFe98mqmKP6kY',\n  'table-name' = 'source_table3',\n  'driver' = 'com.mysql.cj.jdbc.Driver',\n  'sink.buffer-flush.interval' = '3s',\n  'sink.buffer-flush.max-rows' = '1000',\n  'sink.max-retries' = '5'\n )";
        tableEnv.executeSql(source);
        tableEnv.executeSql(mysqlJdbcConnector);
        String insertAdbSql = "SELECT id  , num1 ,  num2 ,  str1 ,  str2 ,  name , type  from default_catalog.default_database.flink_source_table";
        tableEnv.sqlQuery(insertAdbSql).execute().print();
    }
}
