package com.zyd.tech.hbasebulkload.loadhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-06 13:48:36
 * @Version 1.0
 **/
public class HbaseDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 获取配置，并设置hbase的zk地址
        Configuration conf = new Configuration();
        if ("qa".equals(args[3])){
            conf.set("hbase.zookeeper.quorum", "qa-cdh-001,qa-cdh-002,qa-cdh-003");
        }
        else if ("prod".equals(args[3])) {
            conf.set("hbase.zookeeper.quorum", "cdh-slave001.zhijing.com,cdh-slave003.zhijing.com,cdh-slave004.zhijing.com,cdh-slave005.zhijing.com,cdh-slave006.zhijing.com");
        } else {
            System.out.println("未指定环境！");
            return;
        }
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        String tableNameStr = "zz_cycle_all_copy_test1";
        String tableNameStr = args[0];
        // 创建job，并设置主类
        Job job = Job.getInstance(conf);
        job.setJarByClass(HbaseDriver.class);

        // scan查询实例
        Scan scan = new Scan();
        scan.setCaching(Integer.valueOf(args[2]));
        // 设置job的读取输入的表，Mapper类，输出k,v类型
        TableMapReduceUtil.initTableMapperJob(tableNameStr, scan, HbaseMapper.class,
                ImmutableBytesWritable.class, Put.class, job);
        job.setReducerClass(Reducer.class);

        if ("1".equals(args[4])){
            job.setNumReduceTasks(20);//设置Reducer任务数
//            job.getConfiguration().set("mapreduce.map.memory.mb", "10240");//设置Mapper任务使用的内存限制为8192MB。
            job.getConfiguration().set("mapreduce.reduce.memory.mb", "10240");//设置Reducer任务使用的内存限制为8192MB。
//            job.getConfiguration().set("mapreduce.map.cpu.vcores", "10");//设置Mapper任务使用的CPU核心数为8。
            job.getConfiguration().set("mapreduce.reduce.cpu.vcores", "13");//设置Reducer任务使用的CPU核心数为8
        }

        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);//输出路径
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf(tableNameStr);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
        job.waitForCompletion(true);
        if (job.isSuccessful()){
            System.out.println("执行成功！");
        } else {
            System.out.println("执行失败！");
        }
    }

}
