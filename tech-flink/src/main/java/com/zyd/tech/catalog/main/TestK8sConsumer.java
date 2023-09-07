package com.zyd.tech.catalog.main;

import com.zyd.tech.catalog.operator.TestFlatMapFunction;
import com.zyd.tech.catalog.utils.ProperUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import  org.apache.hadoop.hbase.HBaseConfiguration;
/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-07 12:48:03
 * @Version 1.0
 **/
public class TestK8sConsumer {

    static Logger logger = LoggerFactory.getLogger(TestK8sConsumer.class);

    public static void main(String[] args) throws Exception {
        ProperUtil proper = new ProperUtil(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(60000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000*2);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new RocksDBStateBackend((String) proper.get("yeldMessage_savepoint_path")));

        Properties pro = new Properties();
        pro.put("bootstrap.servers", proper.get("kafka_yeldMassage_host"));
        pro.put("group.id", proper.get("kafka_yeldMassage_group"));
        pro.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        pro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        pro.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        EnvironmentSettings Settings = EnvironmentSettings.newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, Settings);
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setSqlDialect(SqlDialect.DEFAULT);

        tableConfig.getConfiguration().setString("pipeline.name","flink-cdc-test");

        System.setProperty("HADOOP_USER_NAME", "hive");
        String name            = "hive_catalog_7";
        String defaultDatabase = "catalog_test";
        String hiveConfDir     = "D:\\tmp\\hive-site";
        String version         = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);
        hiveCatalog.open();
        //获取funciton信息
        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "funEvl1"));
        System.out.println(elv2);
        System.err.printf("clName:%s\n" ,Thread.currentThread().getContextClassLoader().getClass().getName() );

        tableEnv.registerCatalog(name,hiveCatalog);
        tableEnv.useCatalog(name);

        // 选择 database
        tableEnv.useDatabase(defaultDatabase);
        String selectSqlB = "select funEvl1(id,type) local_func1 from hive_catalog_7.catalog_test.flink_source_table_c2";

        tableEnv.sqlQuery(selectSqlB).execute().print();



        String topic = (String) proper.get("kafka_k8s_test_topic");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), pro);

        //设置kafka启动模式
        SingleOutputStreamOperator<String> streamSource = env
                .addSource(kafkaConsumer)
                .uid("e3be2f75-49ad-46fe-b7e3-a4b175cdb1a1")
                .setParallelism(1);

        SingleOutputStreamOperator<Tuple5<String, String, String, String, String>> supplyFlatMapStream = streamSource
                .process(new TestFlatMapFunction())
                .setParallelism(Integer.valueOf(proper.get("parallelism")))
                .uid("22341bd9-71a3-48aa-ab6d-4ef3acc209a4")
                .name("22341bd9-71a3-48aa-ab6d-4ef3acc209a4");

        supplyFlatMapStream.keyBy(new KeySelector<Tuple5<String, String, String, String, String>, String>() {
                    @Override
                    public String getKey(Tuple5<String, String, String, String, String> tuple5) throws Exception {
                        return tuple5.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new ProcessWindowFunction<Tuple5<String, String, String, String, String>, Object, String, TimeWindow>() {

                    private Connection connection= null;
                    private String hbaseHost="";


                    @Override
                    public void open(Configuration parameters) throws Exception {

//                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
//                        conf.set("hbase.zookeeper.quorum", "qa-cdh-001.szzhijing.com:2181,qa-cdh-002.szzhijing.com:2181,qa-cdh-003.szzhijing.com:2181");
//                        connection = ConnectionFactory.createConnection(conf);

                    }

                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple5<String, String, String, String, String>, Object, String, TimeWindow>.Context context,
                                        Iterable<Tuple5<String, String, String, String, String>> iterable, Collector<Object> collector) throws Exception {
//                        BufferedMutator avTableBatch = null;
//                        List<Mutation> staleAvToHbaseList = new ArrayList<Mutation>();
//                        logger.info("开始写入hbase数据...");
//                        for (Tuple5<String, String, String, String, String> tuple5 : iterable){
//                            if (avTableBatch == null) {
//                                avTableBatch = connection.getBufferedMutator(TableName.valueOf(tuple5.f0));
//                            }
//                            Put putAv = new Put(Bytes.toBytes(tuple5.f1));
//                            putAv.addColumn(tuple5.f2.getBytes(), tuple5.f3.getBytes(), tuple5.f4.getBytes());
//                            staleAvToHbaseList.add(putAv);
//                        }
//                        if (CollectionUtils.isNotEmpty(staleAvToHbaseList)){
//                            avTableBatch.mutate(staleAvToHbaseList);
//                            avTableBatch.flush();
//                        }
                        logger.info("开始写入hbase数据...");
                        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                        conf.set("hbase.zookeeper.quorum", "qa-cdh-001.szzhijing.com:2181,qa-cdh-002.szzhijing.com:2181,qa-cdh-003.szzhijing.com:2181");
                        Connection connection = ConnectionFactory.createConnection(conf);
                        List<Mutation> staleAvToHbaseList = new ArrayList<Mutation>();
                        BufferedMutator avTableBatch = null;
                        for (int i = 0; i < 3; i++){
                            if (avTableBatch == null) {
                                avTableBatch = connection.getBufferedMutator(TableName.valueOf("k8s:wr_table_1"));
                            }
                            Put putAv = new Put(Bytes.toBytes("rowkey_" + System.currentTimeMillis()));
                            putAv.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name" + 1), Bytes.toBytes("value" + i));
                            staleAvToHbaseList.add(putAv);
                        }
                        if (CollectionUtils.isNotEmpty(staleAvToHbaseList)){
                            avTableBatch.mutate(staleAvToHbaseList);
                            avTableBatch.flush();
                        }

                        logger.info("写入hbase数据成功！");
                    }
                });

        env.execute("iot-k8s-test0-stream");
    }

}
