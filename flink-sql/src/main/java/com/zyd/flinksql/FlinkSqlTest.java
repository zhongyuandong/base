package com.zyd.flinksql;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;



/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-20 16:36:28
 * @Version 1.0
 **/
public class FlinkSqlTest {


    /**
     * 匹配SQL注释的正则
     */
    public static final Pattern SQL_COMMENT_PATTERN = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|//.*?$|/\\*.*?\\*/|#.*?$|");
    public static void main(String[] args) throws Exception {
        Map<String, String> envConf = new HashMap<>();
        envConf.put("table.exec.state.ttl", "1d");
        Configuration envConfig = new Configuration();
        if (MapUtils.isNotEmpty(envConf)) {
            envConfig.addAll(Configuration.fromMap(envConf));
        }
        envConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
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
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, Settings);
        StringWriter sw = new StringWriter();
        InputStream fileInput = FlinkSqlRunner.class.getClassLoader().getResourceAsStream("row_default_value.sql");
        IOUtils.copy(fileInput, sw, StandardCharsets.UTF_8);
        String allSql = sw.toString();
        // 去除注释
        allSql = SQL_COMMENT_PATTERN.matcher(allSql).replaceAll("$1");

        String[] split = allSql.split(";");
        for (String sql : split) {
            if (StringUtils.isBlank(sql)) {
                continue;
            }
            sql = sql.trim();
            System.out.println(sql);
            System.out.println("===================================");
            if (sql.toUpperCase().startsWith("SELECT")) {
                System.out.println("查询语句...");
                tableEnv.executeSql(sql).print();
                continue;
            }
            tableEnv.executeSql(sql).print();

        }
    }

}
