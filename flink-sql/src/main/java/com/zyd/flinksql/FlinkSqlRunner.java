package com.zyd.flinksql;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
/**
 * 本地运行flink,提交flink sql
 *
 * @author zhangchuqun
 * @since 2022/10/27 20:14
 */
public class FlinkSqlRunner {

    /**
     * 匹配SQL注释的正则
     */
    public static final Pattern SQL_COMMENT_PATTERN = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|//.*?$|/\\*.*?\\*/|#.*?$|");

    public static void main(String[] args) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("table.exec.state.ttl", "1d");
        runFlinkSql("output.sql", null, map);
//        runFlinkSql("row_default_value.sql", null, map);
    }

    public static void runFlinkSql(String sqlFile, Map<String, String> envConf, Map<String, String> tableEnvConf) throws Exception {
        Configuration envConfig = new Configuration();
        if (MapUtils.isNotEmpty(envConf)) {
            envConfig.addAll(Configuration.fromMap(envConf));
        }
        envConfig.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(envConfig);
        env.setParallelism(1);
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration tableEnvConfig = tEnv.getConfig().getConfiguration();
        if (MapUtils.isNotEmpty(tableEnvConf)) {
            tableEnvConfig.addAll(Configuration.fromMap(tableEnvConf));
        }
        InputStream fileInput = FlinkSqlRunner.class.getClassLoader().getResourceAsStream(sqlFile);

        StringWriter sw = new StringWriter();
        IOUtils.copy(fileInput, sw, StandardCharsets.UTF_8);
        String allSql = sw.toString();
        // 去除注释
        allSql = SQL_COMMENT_PATTERN.matcher(allSql).replaceAll("$1");

        String[] split = allSql.split(";");
        for (String sql : split) {
            if (StringUtils.isBlank(sql)) {
                continue;
            }
            System.out.println(sql);
            tEnv.executeSql(sql);
        }
    }
}
