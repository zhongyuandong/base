package com.zyd.tech.hbasebulkload.loadhbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

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

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] row = value.getRow();
        String rowkeyString = Bytes.toString(key.get());
        if (StringUtils.isBlank(rowkeyString) || rowkeyString.split("_").length != 3){
            return;
        }
        String onWorkTime = rowkeyString.split("_")[2];
        if (StringUtils.isBlank(onWorkTime) || !StringUtils.isNumeric(onWorkTime) || Long.valueOf(onWorkTime).longValue() < 1694275200){
            return;
        }
        Put put = new Put(row);
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(row);
        List<Cell> cells= value.listCells();
        for (Cell cell : cells) {
            put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
        }
        context.write(rowKey, put);
    }

}
