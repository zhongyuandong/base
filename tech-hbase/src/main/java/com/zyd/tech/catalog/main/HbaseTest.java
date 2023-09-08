package com.zyd.tech.catalog.main;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            Put putAv = new Put(Bytes.toBytes("rowkey33_" + i));
            putAv.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name" + 1), Bytes.toBytes("value" + i));
            staleAvToHbaseList.add(putAv);
        }
        if (CollectionUtils.isNotEmpty(staleAvToHbaseList)){
            avTableBatch.mutate(staleAvToHbaseList);
            avTableBatch.flush();
        }


        Table avTableQuery = connection.getTable(TableName.valueOf("iot_sz:av_detail"));

        Get get = new Get(Bytes.toBytes("0000000251_axisVarietyId1_1207yc428143_1638890523"));
        Result result = avTableQuery.get(get);
        Map<String, String> axisMap = new HashMap<>();
        for (Cell kv : result.rawCells()) {
            String colName = Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(), kv.getQualifierLength());
            String value = Bytes.toString(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength());
            System.out.println("获取到hbase数据colName：" + colName);
            System.out.println("获取到hbase数据value：" + value);
        }
    }

}