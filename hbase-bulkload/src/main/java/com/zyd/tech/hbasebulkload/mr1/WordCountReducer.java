package com.zyd.tech.hbasebulkload.mr1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    final static Logger logger = Logger.getLogger(WordCountReducer.class);

    private IntWritable result=new IntWritable();
    protected void reduce(Text key, Iterable<IntWritable> valueList,
                          Context context)
            throws IOException, InterruptedException {
        logger.info("reducer key>>>>>" + key.toString());
        int sum=0;
        for(IntWritable value:valueList)
        {
            sum+=value.get();
        }
        result.set(sum);
        logger.info("reducer sum>>>>>" + sum);
        context.write(key, result);
    }
}
