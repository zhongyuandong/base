package com.zyd.tech.catalog.main;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-05 18:23:17
 * @Version 1.0
 **/
public class HbaseTest {

    public static void main(String[] args) throws IOException {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "qa-cdh-001.szzhijing.com:2181,qa-cdh-002.szzhijing.com:2181,qa-cdh-003.szzhijing.com:2181");
        Connection connection = ConnectionFactory.createConnection(conf);
        List<Mutation> staleAvToHbaseList = new ArrayList<Mutation>();
        BufferedMutator avTableBatch = null;
        for (int i = 0; i < 3; i++){
            if (avTableBatch == null) {
                avTableBatch = connection.getBufferedMutator(TableName.valueOf("k8s:wr_table_1"));
            }
            Put putAv = new Put(Bytes.toBytes("rowkey1_" + i));
            putAv.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name" + 1), Bytes.toBytes("value" + i));
            staleAvToHbaseList.add(putAv);
        }
        if (CollectionUtils.isNotEmpty(staleAvToHbaseList)){
            avTableBatch.mutate(staleAvToHbaseList);
            avTableBatch.flush();
        }
    }

}
