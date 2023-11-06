package com.zyd.tech.hbasebulkload.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WorkCountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        System.setProperty("hadoop.home.dir","F:\\tmp\\hadoop");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WorkCountDriver.class);
        job.setMapperClass(WorkCountMapper.class);
        job.setReducerClass(WorkCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/data8/in"));

        FileOutputFormat.setOutputPath(job, new Path("/data8/out"));

        boolean result = job.waitForCompletion(true);

        System.exit(result?0:1);
    }

}
