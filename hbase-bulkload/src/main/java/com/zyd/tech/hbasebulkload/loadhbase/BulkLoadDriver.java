package com.zyd.tech.hbasebulkload.loadhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BulkLoadDriver extends Configured implements Tool {

    private static final String DATA_SEPERATOR = "@";
    private static final String TABLE_NAME = "test_table5_231104_6";//表名
    private static final String INPUT_TABLE_NAME = "test_table5_231104_6";//表名

    public static void main(String[] args) {

        try {
            int response = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadDriver(), args);
            if(response == 0) {
                System.out.println("Job is successfully completed...");
            } else {
                System.out.println("Job failed...");
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String outputPath = args[1];
        /**
         * 设置作业参数
         */
        Configuration configuration = getConf();
        configuration.set("data.seperator", DATA_SEPERATOR);
        configuration.set("hbase.table.name", TABLE_NAME);
        configuration.set("hbase.zookeeper.quorum", "qa-cdh-001,qa-cdh-002,qa-cdh-003");
//        configuration.set("hbase.zookeeper.quorum", "cdh-slave001.zhijing.com,cdh-slave003.zhijing.com,cdh-slave004.zhijing.com,cdh-slave005.zhijing.com,cdh-slave006.zhijing.com");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Job job = Job.getInstance(configuration, "Bulk Loading HBase Table::" + TABLE_NAME);
        job.setJarByClass(BulkLoadDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);//指定输出键类
        job.setMapOutputValueClass(Put.class);//指定输出值类
        job.setMapperClass(BulkLoadMapper.class);//指定Map函数
        FileInputFormat.addInputPaths(job, args[0]);//输入路径
        FileSystem fs = FileSystem.get(configuration);
        Path output = new Path(outputPath);
        if (fs.exists(output)) {
            fs.delete(output, true);//如果输出路径存在，就将其删除
        }
        FileOutputFormat.setOutputPath(job, output);//输出路径
        Connection connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf(TABLE_NAME);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName), connection.getRegionLocator(tableName));
        job.waitForCompletion(true);
        if (job.isSuccessful()){
            System.out.println("执行成功！");
//            Configuration dwConf = getConf();
//            dwConf.set("hbase.zookeeper.quorum", "zjkj-dw0002,zjkj-dw0003,zjkj-dw0005");
//            dwConf.set("hbase.zookeeper.property.clientPort", "2181");
//            Connection dwConn = ConnectionFactory.createConnection(dwConf);
//            Admin admin = dwConn.getAdmin();
//            Table import_dianxin_bulk = dwConn.getTable(TableName.valueOf(INPUT_TABLE_NAME));
//            //获取RegionLocator对象
//            //因为不同的HFile文件属于不同的region，RegionLocator对象就是用来告诉HFile文件应该去哪个Region的
//            RegionLocator import_regionLocator = dwConn.getRegionLocator(TableName.valueOf(INPUT_TABLE_NAME));
//            //第二步，加载HFile到HBase中
//            LoadIncrementalHFiles load = new LoadIncrementalHFiles(dwConf);
//            //需要传入方法的参数依次是输出路径，Admin对象，表名，RegionLocator对象
//            load.doBulkLoad(
//                    output,
//                    admin,
//                    import_dianxin_bulk,
//                    import_regionLocator
//            );
            return 0;
        } else {
            System.out.println("执行失败！");
            return 1;
        }
    }
}
