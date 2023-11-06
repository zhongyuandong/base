package com.zyd.tech.hbasebulkload.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WorkCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    final static Logger logger = Logger.getLogger(WorkCountMapper.class);

    IntWritable outV = new IntWritable(1);
    Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        logger.info(String.format("进入map阶段>>> key: %s line: %s", key.toString(), line));
        for (String work : line.split(",")) {
            logger.info(String.format("map阶段拆分结果>>> work: %s", work));
            outK.set(work);
            context.write(outK, outV);
        }
    }
}
