package com.zyd.tech.hbasebulkload.loadhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;
import java.util.List;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-06 13:49:40
 * @Version 1.0
 **/
public class HbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    int coun = 0;
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(value.getRow());
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(value.getRow());
        List<Cell> cells= value.listCells();
        for (Cell cell : cells) {
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
        }
        context.write(rowKey, put);
        System.out.println(++coun);
    }

}
