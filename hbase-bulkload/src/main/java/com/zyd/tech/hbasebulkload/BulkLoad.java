package com.zyd.tech.hbasebulkload;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @program: hbase-sink-cus-splite
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-01 16:08:25
 * @Version 1.0
 **/
public class BulkLoad {

    final static Logger logger = Logger.getLogger(BulkLoad.class);
    public static class BulkLoadMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,KeyValue>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("开始执行MAP任务...");
            logger.info("key:"+ JSON.toJSONString(key));
            logger.info("value:"+ JSON.toJSONString(value));
            logger.info("context:"+ JSON.toJSONString(context));

            String line = value.toString();
            String[] split = line.split(",");
            String mdn = split[0];
            String start_time = split[1];
            //经度
            String longitude = split[4];
            //纬度
            String latitude = split[5];
            //设置要写入表中的rowkey值
            String rowkey = mdn+"_"+start_time;
            //设置要写入表中的经纬度的值，由于KeyValue传入的是单元格cell的值
            //所以通过rowkey+列簇+列名的形式确定一个单元格，然后传入具体的单元格里面的值
            KeyValue lgKV = new KeyValue(rowkey.getBytes(),"info".getBytes(),"lg".getBytes(),longitude.getBytes());
            KeyValue latKV = new KeyValue(rowkey.getBytes(),"info".getBytes(),"lat".getBytes(),latitude.getBytes());

            context.write(new ImmutableBytesWritable(rowkey.getBytes()),latKV);
            context.write(new ImmutableBytesWritable(rowkey.getBytes()),latKV);

        }
    }

    //因为这里没有计算的需求，所以Reducer的代码可以省略不写

    //Driver端
    public static void main(String[] args) throws Exception {
        logger.info("BulkLoad程序开始运行...");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "qa-cdh-001");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Job job = Job.getInstance(conf);
        job.setJobName("bulkLoad");
        job.setJarByClass(BulkLoad.class);

        //此时不能再在Driver端设置reduce的数量，因为reduce的数量与表里面的region数量一致
        //即使在这里写了设置语句，也不会生效


        logger.info("配置Map...");
        //配置Map
        job.setMapperClass(BulkLoadMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        //保证不同的reduce处理的数据不会有重叠，并且reduce之间是有序的
        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        logger.info("配置reduce...");
        //配置reduce
        //保证在Reduce内部的数据之间是有序的
        job.setReducerClass(KeyValueSortReducer.class);

        String pathString_out = "/data8/tmp_data/testBulkload_out";
        String pathString_int = "D:\\data8\\tmp_data\\testBulkload";

//        pathString_out = "D:\\data8\\tmp_data\\testBulkload_out";
//        pathString_int = "D:\\data8\\tmp_data\\testBulkload";
////
//        pathString = "hdfs://qa-cdh-003.szzhijing.com:9870/data8/tmp_data/testBulkload";

        //配置输入输出路径
        FileInputFormat.addInputPath(job,new Path(pathString_int));
        Path outputPath = new Path(pathString_out);
        Path intputPath = new Path(pathString_int);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(intputPath)){
            logger.info("删除文件：" + pathString_int);
            fileSystem.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);

        FileInputFormat.setInputPaths(job, intputPath);

        logger.info("配置Hfile文件的生成与加载入库Hbase...");
        //配置Hfile文件的生成与加载入库Hbase
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        String tableName = "test_table5_1";
        String importTableName = "test_table5_2";
        Table dianxin_bulk = conn.getTable(TableName.valueOf(tableName));
        //获取RegionLocator对象
        //因为不同的HFile文件属于不同的region，RegionLocator对象就是用来告诉HFile文件应该去哪个Region的
        RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));

        logger.info("第一步，生成HFile...");
        //第一步，生成HFile
        // 使用HFileOutputFormat2将输出的数据格式化为HFile文件
        //这个方法需要传入三个参数，分别是job，表名，RegionLocator对象
        HFileOutputFormat2.configureIncrementalLoad(
                job,
                dianxin_bulk,
                regionLocator
        );

        logger.info("执行第二步加载数据...");
        //等待第一步的HFile文件写入完成，因为调用MapReduce任务是为了生成HFile文件
        //所以第二步加载数据，应该在job.waitForCompletion之后，即任务完成后再加载HFile文件入库
        boolean flag = job.waitForCompletion(true);
        logger.info("所以第二步加载完成：" + flag);
        Table import_dianxin_bulk = conn.getTable(TableName.valueOf(importTableName));
        //获取RegionLocator对象
        //因为不同的HFile文件属于不同的region，RegionLocator对象就是用来告诉HFile文件应该去哪个Region的
        RegionLocator import_regionLocator = conn.getRegionLocator(TableName.valueOf(importTableName));

        if(flag){
            //第二步，加载HFile到HBase中
            LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
            //需要传入方法的参数依次是输出路径，Admin对象，表名，RegionLocator对象
            load.doBulkLoad(
                    outputPath,
                    admin,
                    import_dianxin_bulk,
                    import_regionLocator
            );
            logger.info("MapReduce任务运行成功！");
        }else{
            logger.info("MapReduce任务运行失败！");
        }

    }

}
