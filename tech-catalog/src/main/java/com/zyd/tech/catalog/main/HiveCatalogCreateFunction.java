package com.zyd.tech.catalog.main;

import com.zyd.tech.catalog.model.FunctionRegisterModel;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * mysql binlog 数据实时写到adb
 */
public class HiveCatalogCreateFunction {

    public static void main(String[] args) throws Exception {
        FunctionRegisterModel functionRegisterModel = new FunctionRegisterModel();
        functionRegisterModel.setCatalogCode("hive_catalog_7")
                .setDefaultDatabase("catalog_test").setLocalDirPath("D:/tmp/hive-site/")
                .setVersion("2.1.1").setFunctionName("funEvl1")
                .setFunctionImpl("com.zjkj.realtime.impl.AppendFunction3");
        register(functionRegisterModel);
//        System.setProperty("HADOOP_USER_NAME", "hive");
//        String name            = "hive_catalog_71111";
//        String defaultDatabase = "test0513_7";
//        String hiveConfDir     = "D:/tmp/hive-site/";
//        String version         = "2.1.1";
//
//
//        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir,version);
//
//        hiveCatalog.open();
//
////        //创建函数
//        hiveCatalog.createFunction(new ObjectPath(defaultDatabase,"ap9f13"), new CatalogFunctionImpl("com.zjkj.realtime.impl.Append9Function"), false);
////        hiveCatalog.dropFunction(new ObjectPath(defaultDatabase,"ap9f"), false);
//        hiveCatalog.createDatabase(null, null, false);
//        //获取funciton信息
//        CatalogFunction elv2 = hiveCatalog.getFunction(new ObjectPath(defaultDatabase, "ap9f"));
//
//        System.out.println(elv2);

    }

    public static String register(FunctionRegisterModel functionRegisterModel) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hive");
        String localDirPath = functionRegisterModel.getLocalDirPath();
        if (!localDirPath.endsWith("/")) {
            localDirPath = localDirPath.substring(0, localDirPath.lastIndexOf("/") + 1);
        }
        HiveCatalog hiveCatalog = new HiveCatalog(functionRegisterModel.getCatalogCode(), functionRegisterModel.getDefaultDatabase(), localDirPath, functionRegisterModel.getVersion());
        hiveCatalog.open();
        if (hiveCatalog.functionExists(new ObjectPath(functionRegisterModel.getDefaultDatabase(), functionRegisterModel.getFunctionName()))) {
            return "(函数已存在,无需注册)";
        }
        //创建函数
        hiveCatalog.createFunction(new ObjectPath(functionRegisterModel.getDefaultDatabase(), functionRegisterModel.getFunctionName()), new CatalogFunctionImpl(functionRegisterModel.getFunctionImpl()), false);
        try {
            hiveCatalog.close();
        } catch (CatalogException e) {}
        return "";
    }

}
