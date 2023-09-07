package com.zyd.tech.catalog.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.MalformedURLException;

public class HdfsFile {

    public static void main(String[] args) throws MalformedURLException {
        Configuration hadoopConf = new Configuration();
        String confDir     = "D:\\tmp\\hive-site\\file";
        //后端路径
        hadoopConf.addResource(new File(confDir + "\\core-site.xml").toURI().toURL());
//        hadoopConf.addResource(new File(confDir + "\\yarn-site.xml").toURI().toURL());
        hadoopConf.addResource(new File(confDir + "\\hdfs-site.xml").toURI().toURL());
        String engineJarPath = "D:\\tmp\\hive-site\\jar\\local_tohdfs\\";
        engineJarPath = "D:\\tmp\\hive-site\\jar\\local_tohdfs\\test0601.jar";
        String hdfsPath = "/test/jar_file/";
//        System.setProperty("HADOOP_USER_NAME","user");
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            File localFile = new File(engineJarPath);
            boolean copy = FileUtil.copy(localFile, fs, new Path(hdfsPath), false, hadoopConf);
            FileUtil.copy(localFile, fs, new Path(hdfsPath), false, hadoopConf);
            System.out.println(copy);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
