//package com.zyd.tech.catalog.test;
//
//import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
//import org.apache.flink.table.catalog.CatalogFunction;
//import org.apache.flink.table.catalog.CatalogFunctionImpl;
//import org.apache.flink.table.catalog.FunctionLanguage;
//import org.apache.flink.table.catalog.ObjectPath;
//import org.apache.flink.table.catalog.hive.HiveCatalog;
//
///**
// * mysql binlog 数据实时写到adb
// */
//public class JDBCCatalogCreateFunction {
//
//    public static void main(String[] args) throws Exception {
//        String catalogName = "jdbc_catalog_7";
//        String defaultDatabase = "jdbc_catalog_7";
//        String username = "root";
//        String pwd = "dsf!G13#dsd";
//        String baseUrl = "jdbc:mysql://qa-cdh-002:3306";
//
//
//        JdbcCatalog jdbcCatalog = new JdbcCatalog(catalogName,
//                defaultDatabase,
//                username,
//                pwd,
//                baseUrl);
//
//        jdbcCatalog.open();
//
////        //创建函数
//        jdbcCatalog.createFunction(new ObjectPath(defaultDatabase,"func8_8"), new CatalogFunctionImpl("com.zjkj.realtime.impl.AppendFunction6", FunctionLanguage.JAVA), true);
//
//        jdbcCatalog.createDatabase(null,null,false);
//
//        //获取funciton信息
//        CatalogFunction elv2 = jdbcCatalog.getFunction(new ObjectPath(defaultDatabase, "func8_8"));
//
//        System.out.println(elv2);
//
//    }
//}
