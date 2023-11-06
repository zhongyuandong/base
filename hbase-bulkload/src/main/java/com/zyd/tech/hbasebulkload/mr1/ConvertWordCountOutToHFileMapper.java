package com.zyd.tech.hbasebulkload.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ConvertWordCountOutToHFileMapper {

    final static Logger logger = Logger.getLogger(ConvertWordCountOutToHFileMapper.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // TODO Auto-generated method stub
        Configuration hadoopConfiguration=new Configuration();
        String[] dfsArgs = new GenericOptionsParser(hadoopConfiguration, args).getRemainingArgs();

        //第一个Job就是普通MR，输出到指定的目录
        Job job=new Job(hadoopConfiguration, "wordCountJob");
        job.setJarByClass(ConvertWordCountOutToHFileMapper.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        String inputPath = dfsArgs[0];
        String inputPath = "file:///F:\\tmp\\hadoop\\in";
        logger.info("inputPath>>>>>" + inputPath);
//        String outputPath = dfsArgs[1];
        String outputPath = "file:///F:\\tmp\\hadoop\\out";
        logger.info("outputPath>>>>>" + outputPath);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //提交第一个Job
        int wordCountJobResult=job.waitForCompletion(true)?0:1;
        System.out.println(wordCountJobResult);
    }

}
