package com.zyd.tech.hbasebulkload;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.UUID;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-03 13:07:42
 * @Version 1.0
 **/
public class GemeratorHFile2 {

    final static Logger logger = Logger.getLogger(GemeratorHFile2.class);

    static class HFileImportMapper2 extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        protected final String CF_KQ = "cf";

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            logger.info("开始执行MAP任务...");
            logger.info("key:"+ JSON.toJSONString(key));
            logger.info("value:"+ JSON.toJSONString(value));
            logger.info("context:"+ JSON.toJSONString(context));

            String line = value.toString();
            logger.info("map line : " + line);
            String[] datas = line.split(",");
            String row = new Date().getTime() + "_" + datas[1];
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(row));
            KeyValue kv = new KeyValue(Bytes.toBytes(row), this.CF_KQ.getBytes(), datas[1].getBytes(), datas[2].getBytes());
            context.write(rowkey, kv);
        }
    }

    public static void main(String[] args) {
        logger.info("GemeratorHFile2程序开始运行...");
//        if (args.length != 1) {
//            logger.error("<Usage>Please input hbase-site.xml path.</Usage>");
//            return;
//        }
        Configuration conf = new Configuration();
        conf.addResource(new Path("/data8/tmp_data/hbase-site.xml"));
        conf.set("hbase.fs.tmp.dir", "partitions_" + UUID.randomUUID());
        String tableName = "test_table5_1";
        String input = "/data8/tmp_data/testBulkload_in/person.txt";
        String output = "/data8/tmp_data/testBulkload_out";
        System.out.println("table : " + tableName);
        HTable table;
        try {
            try {
                FileSystem fs = FileSystem.get(URI.create(output), conf);
                Path outputPath = new Path(output);
                if(fs.exists(outputPath)){
                    logger.info("删除文件：" + output);
                    fs.delete(outputPath,true);
                }
                fs.delete(outputPath, true);
                fs.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }

            Connection conn = ConnectionFactory.createConnection(conf);
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
            Job job = Job.getInstance(conf);
            job.setJobName("Generate HFile");

            job.setJarByClass(GemeratorHFile2.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(HFileImportMapper2.class);
            FileInputFormat.setInputPaths(job, input);
            FileOutputFormat.setOutputPath(job, new Path(output));
            logger.info("作业配置完成！");
            //获取RegionLocator对象
            //因为不同的HFile文件属于不同的region，RegionLocator对象就是用来告诉HFile文件应该去哪个Region的
            RegionLocator regionLocator = conn.getRegionLocator(TableName.valueOf(tableName));
            logger.info("第一步，生成HFile...");
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
            try {
                logger.info("执行第二步加载数据...");
                job.waitForCompletion(true);
                logger.info("第二步加载数据完成");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
