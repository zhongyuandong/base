package com.zyd.tech.hbasebulkload.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WorkCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    final static Logger logger = Logger.getLogger(WorkCountReducer.class);

    IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        logger.info(String.format("进入reduce阶段>>> key: %s", key.toString()));
        int sum = 0;
        for (IntWritable value : values){
            int val = value.get();
            logger.info(String.format("reduce阶段遍历>>> value: %d", val));
            sum += val;
        }
        outV.set(sum);
        logger.info(String.format("reduce阶段汇总结果>>> sum: %d", sum));
        context.write(key, outV);
    }
}
