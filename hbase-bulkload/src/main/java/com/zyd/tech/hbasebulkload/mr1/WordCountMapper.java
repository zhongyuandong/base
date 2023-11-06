package com.zyd.tech.hbasebulkload.mr1;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    final static Logger logger = Logger.getLogger(WordCountMapper.class);
    private Text wordText=new Text();
    private IntWritable one = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String line=value.toString();
        logger.info("map line>>>>>" + line);
        String[] wordArray=line.split(",");
        logger.info("wordArray line>>>>>" + JSON.toJSONString(wordArray));
        for(String word:wordArray)
        {
            wordText.set(word);
            context.write(wordText, one);
        }
    }
}
