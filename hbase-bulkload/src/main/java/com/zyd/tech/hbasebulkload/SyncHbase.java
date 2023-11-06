package com.zyd.tech.hbasebulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-03 20:59:21
 * @Version 1.0
 **/
public class SyncHbase {

    final static Logger logger = Logger.getLogger(SyncHbase.class);

    // 声明静态配置
    private static Configuration iotConf = null;
    private static Connection iotConn = null;
    private static Configuration dwConf = null;
    private static Connection dwConn = null;
    static {
        iotConf = HBaseConfiguration.create();
//        iotConf.set("hbase.zookeeper.quorum", "qa-cdh-001,qa-cdh-002,qa-cdh-003");
        iotConf.set("hbase.zookeeper.quorum", "cdh-slave001.zhijing.com,cdh-slave003.zhijing.com,cdh-slave004.zhijing.com,cdh-slave005.zhijing.com,cdh-slave006.zhijing.com");
        iotConf.set("hbase.zookeeper.property.client", "2181");
        iotConf.setInt("hbase.client.scanner.timeout.period", 600000);
        iotConf.setInt("hbase.regionserver.lease.period", 1200000);
        iotConf.setInt("hbase.rpc.timeout", 600000);
        iotConf.set("hbase.defaults.for.version.skip", "true");
        dwConf = HBaseConfiguration.create();
//        dwConf.set("hbase.zookeeper.quorum", "qa-cdh-001,qa-cdh-002,qa-cdh-003");
        dwConf.set("hbase.zookeeper.quorum", "zjkj-dw0002,zjkj-dw0003,zjkj-dw0005");
        dwConf.set("hbase.zookeeper.property.client", "2181");
        dwConf.setInt("hbase.client.scanner.timeout.period", 600000);
        dwConf.setInt("hbase.regionserver.lease.period", 1200000);
        dwConf.setInt("hbase.rpc.timeout", 600000);
        dwConf.set("hbase.defaults.for.version.skip", "true");
        try{
            iotConn = ConnectionFactory.createConnection(iotConf);
            dwConn = ConnectionFactory.createConnection(dwConf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String tableName = "zz_cycle_all_copy_test1";
//        String tableName = "zz_cycle_all";
        String targetTableName = "zz_cycle_all_bak";
        Table table = iotConn.getTable(TableName.valueOf(tableName));
        TableName target_tableName = TableName.valueOf(targetTableName);
        BufferedMutator mutator = dwConn.getBufferedMutator(target_tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        AtomicBoolean occurError = new AtomicBoolean(false);
        int nbRows = 5000;
        int flushSize = 10000;
        int count = 0;
        long startTime = System.currentTimeMillis();
        logger.info(String.format("开始批量[%d]写入hbase数据...", nbRows));
        try {
            while (true) {
                logger.info(String.format("开始写入第%d批数据...", ++count));
                Result[] next = scanner.next(nbRows);
                if (next == null || next.length == 0 || occurError.get()) {
                    logger.info("退出循环...");
                    break;
                }
                List<Put> puts = new ArrayList<>();
                try {
                    for (int i = 0; i < next.length; i++) {
                        Result result = next[i];
                        String rowKey = Bytes.toString(result.getRow());
                        Put put = new Put(Bytes.toBytes(rowKey));
                        List<Cell> cells= result.listCells();
                        for (Cell cell : cells) {
                            String family1 = Bytes.toString(CellUtil.cloneFamily(cell));
                            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            put.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                        }
                        puts.add(put);
                        if (puts.size() > flushSize){
                            mutator.mutate(puts);
                            puts.clear();
                        }
                    }

                    if (puts.size() > 0){
                        mutator.mutate(puts);
                        mutator.flush();
                        puts.clear();
                    }
                } catch (IOException e) {
                    logger.error(String.format("程序异常：%s", e.getMessage()));
                    occurError.set(true);
                }
                logger.info(String.format("完成第%d批数据写入，耗时：%d s", count, (System.currentTimeMillis() - startTime)/1000));
            }
            mutator.close();
        } catch (Exception e) {
            mutator.close();
            iotConn.close();
            dwConn.close();
            logger.error(String.format("程序异常：%s", e.getMessage()));
            e.printStackTrace();
        } finally {
            mutator.close();
            iotConn.close();
            dwConn.close();
            logger.info("执行finally...");
        }
        logger.info(String.format("hbase数据同步完成，耗时：%d s", count, (System.currentTimeMillis() - startTime)/1000));
    }

}
