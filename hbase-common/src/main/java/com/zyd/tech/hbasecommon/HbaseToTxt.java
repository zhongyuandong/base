package com.zyd.tech.hbasecommon;

import com.zyd.common.utils.StringCompressUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-03 20:59:21
 * @Version 1.0
 **/
public class HbaseToTxt {

    final static Logger logger = LoggerFactory.getLogger(HbaseToTxt.class);

    // 声明静态配置
    private static Configuration iotConf = null;
    private static Connection iotConn = null;
    static {
        iotConf = HBaseConfiguration.create();
        iotConf.set("hbase.zookeeper.quorum", "qa-cdh-001,qa-cdh-002,qa-cdh-003");
//        iotConf.set("hbase.zookeeper.quorum", "cdh-slave001.zhijing.com,cdh-slave003.zhijing.com,cdh-slave004.zhijing.com,cdh-slave005.zhijing.com,cdh-slave006.zhijing.com");
        iotConf.set("hbase.zookeeper.property.client", "2181");
        iotConf.setInt("hbase.client.scanner.timeout.period", 600000);
        iotConf.setInt("hbase.regionserver.lease.period", 1200000);
        iotConf.setInt("hbase.rpc.timeout", 600000);
        iotConf.set("hbase.defaults.for.version.skip", "true");
        try{
            iotConn = ConnectionFactory.createConnection(iotConf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        String tableName = "zz_cycle_all_copy_test1";
        String tableName = "zz_cycle_all";
        Table table = iotConn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        AtomicBoolean occurError = new AtomicBoolean(false);
        int nbRows = 6;
        int count = 0;
        int fileCount = 0;
        long startTime = System.currentTimeMillis();
//        String fileNameFormat = "D:\\tmp\\hadoop\\bulk_intput1\\zz_cycle_all_%d.txt";
        String fileNameFormat = "/data8/tmp/testfile/hbase/data/zz_cycle_all_%d.txt";
        String format = "%s%s%s%s%s";
        String split20 = Objects.toString((char)20);
        String split21 = Objects.toString((char)21);
        String split22 = Objects.toString((char)22);
        logger.info(String.format("开始批量[%d]写入hbase数据...", nbRows));
        List<String> puts = new ArrayList<String>();
        FileWriter fileWriter = null;
        BufferedWriter writer = null;
        List<String> contentList = new ArrayList<>();
        String fileName = null;

        try {
            while (true) {
                if (count%5 == 0){
                    fileName = String.format(fileNameFormat, ++fileCount);
                    if (writer != null){
                        writer.close();
                    }
                    if (fileWriter != null){
                        fileWriter.close();
                    }
                    fileWriter = new FileWriter(fileName);
                    writer = new BufferedWriter(fileWriter);
                }
                logger.info(String.format("开始写入第%d批数据, 文件输出路径：%s", ++count, fileName));
                Result[] next = scanner.next(nbRows);
                if (next == null || next.length == 0 || occurError.get()) {
                    logger.info("退出循环...");
                    break;
                }
                try {
                    for (int i = 0; i < next.length; i++) {
                        Result result = next[i];
                        String rowKey = Bytes.toString(result.getRow());
                        List<Cell> cells= result.listCells();
                        for (Cell cell : cells) {
                            String family = Bytes.toString(CellUtil.cloneFamily(cell));
                            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            puts.add(StringCompressUtils.compress(String.format(format, family, split20, qualifier, split20, value)));
                        }
                        contentList.add(String.format("%s%s%s", rowKey, split22, StringUtils.join(puts, split21)));
                        puts.clear();
                    }
                    writer.write(StringCompressUtils.compress(StringUtils.join(contentList, "\t")));
                    writer.newLine();
                    contentList.clear();
                } catch (IOException e) {
                    logger.error(String.format("程序异常：%s", e.getMessage()));
                    occurError.set(true);
                }
                logger.info(String.format("完成第%d批数据写入，耗时：%d s", count, (System.currentTimeMillis() - startTime)/1000));
            }
        } catch (Exception e) {
            iotConn.close();
            writer.close();
            fileWriter.close();
            logger.error(String.format("程序异常：%s", e.getMessage()));
            e.printStackTrace();
        } finally {
            iotConn.close();
            writer.close();
            fileWriter.close();
            logger.info("执行finally...");
        }
        logger.info(String.format("hbase数据同步完成，耗时：%d s", count, (System.currentTimeMillis() - startTime)/1000));
    }

}
