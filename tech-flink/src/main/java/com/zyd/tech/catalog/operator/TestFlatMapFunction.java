package com.zyd.tech.catalog.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFlatMapFunction extends ProcessFunction<String, Tuple5<String, String, String, String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(TestFlatMapFunction.class);

    @Override
    public void processElement(String input,
                               Context context,
                               Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
        if (StringUtils.isBlank(input)){
            logger.error("数据为空！");
            return;
        }
        String[] split = input.split(",");
        int length = split.length;
        if (length < 5){
            logger.error("异常数据：{}", input);
            return;
        }
        logger.info("*******测试k8s，接收数据:{}",input);
        out.collect(Tuple5.of(split[0], split[1], split[2], split[3], split[4]));
    }
}
