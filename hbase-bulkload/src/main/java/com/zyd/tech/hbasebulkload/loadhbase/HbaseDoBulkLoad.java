package com.zyd.tech.hbasebulkload.loadhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-06 16:37:21
 * @Version 1.0
 **/
public class HbaseDoBulkLoad {
    final static Logger logger = LoggerFactory.getLogger(HbaseDoBulkLoad.class);

    public static void main(String[] args) throws IOException {
        Connection dwConn = null;
        try {
            Configuration configuration = new Configuration();
            if ("qa".equals(args[2])){
                logger.info("配置测试环境hbase配置...");
                configuration.set("hbase.zookeeper.quorum", "qa-cdh-001,qa-cdh-002,qa-cdh-003");
            }
            else if ("prod".equals(args[2])) {
                logger.info("配置生产环境hbase配置...");
                configuration.set("hbase.zookeeper.quorum", "zjkj-dw0002,zjkj-dw0003,zjkj-dw0005");
            } else {
                logger.error("未指定环境！");
                return;
            }
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            String tableName = args[1];
            Path output = new Path(args[0]);

            configuration.setInt("hbase.rpc.timeout", 600000);
            configuration.setInt("hbase.client.operation.timeout", 600000);
            configuration.setInt("hbase.client.scanner.timeout.period", 600000);
            configuration.setInt("hbase.regionserver.lease.period", 600000);
            configuration.setInt("phoenix.query.timeoutMs", 600000);
            configuration.setInt("phoenix.query.keepAliveMs", 600000);
            configuration.set("hbase.client.ipc.pool.type", "RoundRobinPool");
            configuration.setInt("hbase.client.ipc.pool.size", 10);
            configuration.setLong("hbase.hregion.max.filesize", 10737418240L);
            configuration.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", 1024);//默认为32
            configuration.setInt("hbase.loadincremental.threads.max", 20);//bulkload的线程池,默认为当前机器的core数量
            LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
            dwConn = ConnectionFactory.createConnection(configuration);
            long startTime = System.currentTimeMillis();
            logger.info("开始load hfile...");
            //需要传入方法的参数依次是输出路径，Admin对象，表名，RegionLocator对象
            load.doBulkLoad(
                    output,
                    dwConn.getAdmin(),
                    dwConn.getTable(TableName.valueOf(tableName)),
                    dwConn.getRegionLocator(TableName.valueOf(tableName))
            );
            dwConn.close();
            logger.info(String.format("load hfile 完成，耗时：%d s", (System.currentTimeMillis() - startTime)/1000));
        } catch (Exception e) {
            logger.error("执行异常！");
            e.printStackTrace();
        } finally {
            dwConn.close();
        }

    }

}
