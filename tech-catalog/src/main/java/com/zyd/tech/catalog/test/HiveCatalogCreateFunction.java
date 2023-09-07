package com.zyd.tech.catalog.test;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.module.hive.HiveModule;

/**
 * mysql binlog 数据实时写到adb
 */
public class HiveCatalogCreateFunction {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hive");
        String name            = "hive_catalog_71111";
        String defaultDatabase = "test0513_7";
        String hiveConfDir     = "D:\\tmp\\hive-site";
        String version         = "2.1.1";


        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);

        hiveCatalog.open();

//        //创建函数
        hiveCatalog.createFunction(new ObjectPath(defaultDatabase,"ap9f"), new CatalogFunctionImpl("com.zjkj.realtime.impl.Append9Function"), false);
//        hiveCatalog.dropFunction(new ObjectPath(defaultDatabase,"ap9f"), false);
//        hiveCatalog.createDatabase(null, null, false);
        //获取funciton信息
        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "ap9f"));

        System.out.println(elv2);

    }
}
