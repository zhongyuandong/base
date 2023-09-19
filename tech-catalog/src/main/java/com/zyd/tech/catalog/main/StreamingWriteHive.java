package com.zyd.tech.catalog.main;

import org.apache.commons.collections.MapUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.delegation.ParserFactory;

import javax.annotation.Nullable;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-13 17:02:28
 * @Version 1.0
 **/
public class StreamingWriteHive {

    public static void main(String[] args) throws Exception{

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration tableEnvConfig = tEnv.getConfig().getConfiguration();
        Map<String, String> tableEnvConf = new HashMap<String, String>();
        tableEnvConf.put("table.exec.state.ttl", "1d");
        if (MapUtils.isNotEmpty(tableEnvConf)) {
            tableEnvConfig.addAll(Configuration.fromMap(tableEnvConf));
        }

//        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        bsEnv.enableCheckpointing(10000);
//        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);
//        DataStream<UserInfo> dataStream = bsEnv.addSource(new MySource())
//                .assignTimestampsAndWatermarks(
//                        new AssignerWithPunctuatedWatermarks<UserInfo>(){
//                            long water = 0l;
//                            @Nullable
//                            @Override
//                            public Watermark checkAndGetNextWatermark(
//                                    UserInfo lastElement,
//                                    long extractedTimestamp){
//                                return new Watermark(water);
//                            }
//
//                            @Override
//                            public long extractTimestamp(
//                                    UserInfo element,
//                                    long recordTimestamp){
//                                water = element.getTs().getTime();
//                                return water;
//                            }
//                        });

        String tableName = "source_connector_hermes_test";
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


        //构造hive catalog
        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";
//          String hiveConfDir     = "/opt/flink/conf/hive";
        String version = "2.1.1";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tEnv.registerCatalog(name, hive);
        tEnv.useCatalog(name);
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.useDatabase("testdb");

//        tEnv.createTemporaryView("users", dataStream);

//      如果hive中已经存在了相应的表，则这段代码省略
//		String hiveSql = "CREATE external TABLE fs_table (\n" +
//		                 "  user_id STRING,\n" +
//		                 "  order_amount DOUBLE" +
//		                 ") partitioned by (dt string,h string,m string) " +
//		                 "stored as ORC " +
//		                 "TBLPROPERTIES (\n" +
//		                 "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',\n" +
//		                 "  'sink.partition-commit.delay'='0s',\n" +
//		                 "  'sink.partition-commit.trigger'='partition-time',\n" +
//		                 "  'sink.partition-commit.policy.kind'='metastore'" +
//		                 ")";
//		tEnv.executeSql(hiveSql);

//        String insertSql = "insert into  fs_table SELECT userId, amount, " +
//                " DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH'), DATE_FORMAT(ts, 'mm') FROM users";
        String insertSql = "SELECT * FROM " + tableName;
        tEnv.executeSql(insertSql).print();
    }


    public static class MySource implements SourceFunction<UserInfo> {

        String userids[] = {
                "4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
                "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
                "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
                "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
                "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
        };

        @Override
        public void run(SourceContext<UserInfo> sourceContext) throws Exception{

            while (true){
                String userid = userids[(int) (Math.random() * (userids.length - 1))];
                UserInfo userInfo = new UserInfo();
                userInfo.setUserId(userid);
                userInfo.setAmount(Math.random() * 100);
                userInfo.setTs(new Timestamp(System.currentTimeMillis()));
                sourceContext.collect(userInfo);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel(){

        }
    }

    public static class UserInfo implements java.io.Serializable{
        private String userId;
        private Double amount;
        private Timestamp ts;

        public String getUserId(){
            return userId;
        }

        public void setUserId(String userId){
            this.userId = userId;
        }

        public Double getAmount(){
            return amount;
        }

        public void setAmount(Double amount){
            this.amount = amount;
        }

        public Timestamp getTs(){
            return ts;
        }

        public void setTs(Timestamp ts){
            this.ts = ts;
        }
    }

}
