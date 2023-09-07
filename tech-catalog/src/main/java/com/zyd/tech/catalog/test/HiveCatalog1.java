package com.zyd.tech.catalog.test;

import com.zyd.tech.catalog.utils.FlinkUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * mysql binlog 数据实时写到adb
 */
public class HiveCatalog1 {

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
        String hiveConfDir     = "D:\\tmp\\hive-site";
//        String hiveConfDir = args[0];
        String version         = "2.1.1";

//        tableEnv.loadModule(name, new HiveModule(version));

        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);

        hiveCatalog.open();

//        tableEnv.registerFunction("wAvg", new WeightedAvg());

//        //创建函数
//        hiveCatalog.createFunction(new ObjectPath(defaultDatabase,"local_func1"), new CatalogFunctionImpl("com.zjkj.realtime.impl.Append8Function", FunctionLanguage.JAVA), false);
//
//        //修改函数
//        hiveCatalog.alterFunction(new ObjectPath(defaultDatabase,"append_new"), new CatalogFunctionImpl("com.zjkj.realtime.impl.Append7Function", FunctionLanguage.JAVA), false);
//
//        //删除函数
//        hiveCatalog.dropFunction(new ObjectPath(defaultDatabase, "elv3"), false);
//
        //获取funciton信息
        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "funEvl1"));

        System.out.println(elv2);

//        ClassLoader cl = ClassLoader.getSystemClassLoader();//获取默认系统类加载器
//        Class<?> cls = cl.loadClass("com.zjkj.realtime.impl.AppendFunction");//加载系统类ArrayList
//        ClassLoader actualLoader = cls.getClassLoader();
//        System.out.println(actualLoader);


        System.err.printf("clName:%s\n" ,Thread.currentThread().getContextClassLoader().getClass().getName() );

        tableEnv.registerCatalog(name,hiveCatalog);
        tableEnv.useCatalog(name);

        // 选择 database
        tableEnv.useDatabase(defaultDatabase);






        String catalogName = "null";
        String defaultDatabase1 = "null";
        String username = "null";
        String pwd = "null";
        String baseUrl = "null";

//        PostgresCatalog postgresCatalog = new PostgresCatalog(catalogName, defaultDatabase1, username, pwd, baseUrl);
//
//        JdbcCatalog jdbcCatalog = new JdbcCatalog(catalogName, defaultDatabase1, username, pwd, baseUrl);
//
//        jdbcCatalog.getFunction(null);


        //create a mysql cdc table source sql
        String cdcTableA="CREATE TABLE default_catalog.default_database.source_table1_7 ( "
                +"        id int NOT NULL, "
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
                +"       'table-name' = 'source_table1' "
                +" )";


        String cdcTableB="CREATE TABLE hive_catalog_7.test0513_8.source_table2_7_8 ( "
                +"        id int NOT NULL, "
                +"        addr STRING, "
                +"        primary key (id) not enforced"
                +"    ) WITH ( "
                +"       'connector' = 'mysql-cdc', "
                +"       'hostname' = 'qa-cdh-002', "
                +"       'port' = '3306', "
                +"       'username' = 'root', "
                +"       'password' = 'dsf!G13#dsd', "
//                +"       'is_generic' = 'false', "
                +"       'database-name' = 'hive_catalog_source', "
                +"       'table-name' = 'source_table2' "
                +" )";

        String cdcTableC="CREATE TABLE hive_catalog_7.catalog_test.flink_source_table_c3 ( "
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

//        tableEnv.executeSql(cdcTableC);
//        tableEnv.executeSql(cdcTableB);


        String  writeMysqlTableA =
                "  create table target_table1_7 ( "
                        +"        id int NOT NULL, "
                        +"        name STRING, "
                        +"        type STRING, "
                        +"        primary key (id) not enforced"
                        +"  ) with ( "
                        +"   'connector' = 'jdbc', "
                        +"   'url' = 'jdbc:mysql://qa-cdh-002:3306/hive_catalog?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC', "
                        +"   'username' = 'root', "
                        +"   'password' = 'dsf!G13#dsd', "
                        +"   'table-name' = 'target_table1', "
                        +"   'driver' = 'com.mysql.cj.jdbc.Driver', "
                        +"   'sink.parallelism' = '1', "
                        +"   'sink.buffer-flush.interval' = '3s', "
                        +"   'sink.buffer-flush.max-rows' = '1', "
                        +"       'is_generic' = 'false', "
                        +"   'sink.max-retries' = '5' "
                        +"  ) ";

        String  writeMysqlTableB =
                "  create table target_table2_7 ( "
                        +"        id int NOT NULL, "
                        +"        addr STRING, "
                        +"        primary key (id) not enforced"
                        +"  ) with ( "
                        +"   'connector' = 'jdbc', "
                        +"   'url' = 'jdbc:mysql://qa-cdh-002:3306/hive_catalog?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC', "
                        +"   'username' = 'root', "
                        +"   'password' = 'dsf!G13#dsd', "
                        +"   'table-name' = 'target_table2', "
                        +"   'driver' = 'com.mysql.cj.jdbc.Driver', "
                        +"   'sink.parallelism' = '1', "
                        +"   'sink.buffer-flush.interval' = '3s', "
                        +"   'sink.buffer-flush.max-rows' = '1', "
                        +"       'is_generic' = 'false', "
                        +"   'sink.max-retries' = '5' "
                        +"  ) ";

//        tableEnv.executeSql(writeMysqlTableA);
//        tableEnv.executeSql(writeMysqlTableB);

        String source3 = "CREATE TABLE `source_table3` ("
                +"        id int NOT NULL, "
                +"        num1 int, "
                +"        num2 int, "
                +"        str1 STRING, "
                +"        str2 STRING, "
                +"        name STRING, "
                +"        type STRING, "
                +"        primary key (id) not enforced"
                +") WITH ("
                +"   'connector' = 'jdbc', "
                +"   'url' = 'jdbc:mysql://qa-cdh-002:3306/hive_catalog?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC', "
                +"   'username' = 'root', "
                +"   'password' = 'dsf!G13#dsd', "
                +"   'table-name' = 'source_table3', "
                +"   'driver' = 'com.mysql.cj.jdbc.Driver', "
                +"   'lookup.max-retries' = '10', "
                +"   'lookup.cache.max-rows' = '5000', "
                +"   'lookup.cache.ttl' = '60s' "
                +"  ) ";
//        tableEnv.executeSql(source3);


        String insertSqlA = "insert into target_table1_7 select * from source_table1_7";

        String insertSqlB = "insert into target_table2_7 select * from source_table2_7";

//        String selectSqlA = "select * from source_table2_7";
        String selectSqlA = "select elv1(id,type) from source_table2_7";

        StatementSet statementSet = tableEnv.createStatementSet();
//        tableEnv.sqlQuery(selectSqlA).execute().print();

        String selectSqlB = "select substr(append3(id,addr),1,3) substr,append3(id,addr) append3,elv1(id,addr) elv1,elv2(id,addr) elv2 from source_table2_7";

        selectSqlB = "select func3(id,addr) func3,func6(id,addr) func6,func8(id,addr) func8,elv1(id,addr) elv1 from source_table2_7";

        selectSqlB = "select func8_8(id,type) func8_8,func8_6(id,type) func8_6,func6(id,type) func6,func8(id,type) func8,elv1(id,type) elv1 from default_catalog.default_database.source_table1_7";

        selectSqlB = "select hive_catalog_7.test0513_8.func88(id,type) func88 from default_catalog.default_database.source_table1_7";

//        selectSqlB = "select wAvg(id, id) wAvg, elv1(id,type) append_new9 from source_table1_7";

        selectSqlB = "select funEvl1(id,type) local_func1 from hive_catalog_7.catalog_test.flink_source_table_c3";

        tableEnv.sqlQuery(selectSqlB).execute().print();

//        statementSet.addInsertSql(insertSqlA);
//        statementSet.addInsertSql(insertSqlB);
//        statementSet.execute();

//        String selectSql ="select * from dgp_emp";
//
//        tableEnv.executeSql(selectSql).collect();
    }
}
