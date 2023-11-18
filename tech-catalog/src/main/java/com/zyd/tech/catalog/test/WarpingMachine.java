package com.zyd.tech.catalog.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-17 15:38:28
 * @Version 1.0
 **/
public class WarpingMachine {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔60000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
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

        tableConfig.getConfiguration().setString("pipeline.name","uac-sync");

        String test1 = "create table warping_machine_source (\n" +
                "shiftId varchar\n" +
                ",instId varchar\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'warping_machine_topic_test',\n" +
                "  'format' = 'json',\n" +
                "  'properties.bootstrap.servers' = 'qa-cdh-001:9093,qa-cdh-002:9093,qa-cdh-003:9093',\n" +
                "  'properties.group.id' = 'warping_machine_topic_test_group1',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'scan.topic-partition-discovery.interval' = '600s',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")";


        String insertSql1 = // "insert into test1_target " +
                "create table default_catalog.default_database.sink_print(\n" +
                        "    machineId varchar,\n" +
                        "\tshiftId varchar,\n" +
                        "\tinstId varchar,\n" +
                        "\tcurrentStatus bigint,\n" +
                        "\tcurrentStrip bigint,\n" +
                        "\tcurrentMeter bigint,\n" +
                        "\tpreShiftId varchar,\n" +
                        "\tpreInstId varchar \n" +
                        " )\n" +
                        " with (\n" +
                        "  'connector'='print'\n" +
                        " )";

        String insertSql2 = // "insert into test1_target " +
                "insert into default_catalog.default_database.sink_print\n" +
                        "SELECT\n" +
                        "\tt1.machineId,\n" +
                        "\tt1.shiftId,\n" +
                        "\tt1.instId,\n" +
                        "\tt1.currentStatus,\n" +
                        "\tt1.currentStrip,\n" +
                        "\tt1.currentMeter,\n" +
                        "\tlead( t1.shiftId ) over ( PARTITION BY t1.machineId ORDER BY t1.row_time ASC ) AS preShiftId,\n" +
                        "\tlead( t1.instId ) over ( PARTITION BY t1.machineId ORDER BY t1.row_time ASC ) AS preInstId \n" +
                        "FROM\n" +
                        "\tdefault_catalog.default_database.warping_machine_source t1";



        tableEnv.executeSql(test1);

        tableEnv.sqlQuery(insertSql1);

        tableEnv.sqlQuery(insertSql2);

//        StatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsertSql(insertSql1);
//        statementSet.execute();

    }

}
