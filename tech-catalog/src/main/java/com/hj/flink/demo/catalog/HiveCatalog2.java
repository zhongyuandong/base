package com.hj.flink.demo.catalog;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * mysql binlog 数据实时写到adb
 */
public class HiveCatalog2 {

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
                //.useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, Settings);
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setSqlDialect(SqlDialect.DEFAULT);

        tableConfig.getConfiguration().setString("pipeline.name","flink-cdc-test");

//      System.setProperty("HADOOP_USER_NAME", "hive1");
        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
//        String hiveConfDir     = "D:\\tmp\\hive-site";
//      String hiveConfDir     = "/opt/flink/conf/hive";
            String hiveConfDir     = "";
        String version         = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, null);
        hiveCatalog.open();

//
        //获取function信息
//        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "funEvl1"));

//        System.out.println("获取function信息>>>>" + JSON.toJSONString(elv2));

        tableEnv.registerCatalog(name, (Catalog) hiveCatalog);
        tableEnv.useCatalog(name);

        long time = System.currentTimeMillis();

        // 选择 database
        tableEnv.useDatabase(defaultDatabase);
        String hostName = "qa-cdh-002";
        int port = 3306;

            telnet(hostName, port, 10000);

        String tableName = "default_catalog.default_database.flink_source_table_" + time;

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
                    +"       'database-name' = 'hive_catalog_source', "
                    +"       'table-name' = 'source_table3' "
                    +" )";

//            cdcTableC = "create table " + tableName + " (\n" +
//                    "  mdata row(\n" +
//                    "    cTime bigint,\n" +
//                    "    orgCode varchar\n" +
//                    "  ),\n" +
//                    "  cTime as mdata.cTime,\n" +
//                    "  orgCode as mdata.orgCode\n" +
//                    ") with (\n" +
//                    "  'connector' = 'kafka',\n" +
//                    "  'topic' = 'k8s_test01',\n" +
//                    "  'format' = 'json',\n" +
//                    "  'properties.bootstrap.servers' = 'qa-cdh-001:9093,qa-cdh-002:9093,qa-cdh-003:9093',\n" +
//                    "  'properties.group.id' = 'k8s_test0_202309121',\n" +
//                    "  'scan.startup.mode' = 'group-offsets',\n" +
//                    "  'scan.topic-partition-discovery.interval' = '1000',\n" +
//                    "  'json.ignore-parse-errors' = 'true'\n" +
//                    ")";

//            cdcTableC = "create table " + tableName + " (\n" +
//                    "id INT, name STRING, age INT\n" +
//                    ") with (\n" +
//                    "  'connector' = 'kafka',\n" +
//                    "  'topic' = 'k8s_test01',\n" +
//                    "  'format' = 'json',\n" +
//                    "  'properties.bootstrap.servers' = 'qa-kafka-cluster01:9093',\n" +
//                    "  'properties.group.id' = 'k8s_test0_202309121',\n" +
//                    "  'scan.startup.mode' = 'group-offsets',\n" +
//                    "  'scan.topic-partition-discovery.interval' = '1000',\n" +
//                    "  'json.ignore-parse-errors' = 'true'\n" +
//                    ")";
//
//            cdcTableC = "CREATE TABLE user_behavior (\n" +
//                    "    id BIGINT,\n" +
//                    "    name STRING,\n" +
//                    "    flag STRING\n" +
//                    ") WITH (\n" +
//                    "    'connector.type' = 'kafka',\n" +
//                    "    'connector.version' = 'universal',\n" +
//                    "    'connector.topic' = 'k8s_test01',\n" +
//                    "    'connector.startup-mode' = 'earliest-offset',\n" +
////                    "    'connector.properties.zookeeper.connect' = 'vm61:2181',\n" +
//                    "    'connector.properties.bootstrap.servers' = 'qa-kafka-cluster01:9093',\n" +
//                    "    'format.type' = 'json'\n" +
//                    ")";
        tableEnv.executeSql(cdcTableC).print();





//        String selectSqlB = "select id,type local_func1 from " + tableName;
//            String selectSqlB = "select * from " + tableName;
//            selectSqlB = "select * from user_behavior";
            String selectSqlB = "select funEvl1(num1,str1) local_func122 from " + tableName;

        tableEnv.sqlQuery(selectSqlB).execute().print();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("捕获到异常消息：" + e.getMessage());
        }

    }

    public static boolean telnet(String hostname, int port, int timeout){
        Socket socket = new Socket();
        boolean isConnected = false;
        try {
            socket.connect(new InetSocketAddress(hostname, port), timeout); // 建立连接
            isConnected = socket.isConnected(); // 通过现有方法查看连通状态
//            System.out.println(isConnected);    // true为连通
        } catch (IOException e) {
            System.out.println("false");        // 当连不通时，直接抛异常，异常捕获即可
        }finally{
            try {
                socket.close();   // 关闭连接
            } catch (IOException e) {
                System.out.println("false");
            }
        }
        System.out.println("telnet "+ hostname + " " + port + "\n==>isConnected: " + isConnected);
        return isConnected;
    }
}
