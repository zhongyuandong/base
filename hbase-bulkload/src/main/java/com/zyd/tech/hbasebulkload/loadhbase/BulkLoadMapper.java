package com.zyd.tech.hbasebulkload.loadhbase;

import com.zyd.common.utils.StringCompressUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Objects;

public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    private String dataSeperator;
    private String split20 = Objects.toString((char)20);
    private String split21 = Objects.toString((char)21);
    private String split22 = Objects.toString((char)22);

    private String lineFeed = "\t";

    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();//获取作业参数
        dataSeperator = configuration.get("data.seperator");
    }

    public void map(LongWritable key, Text value, Context context){
        try {
            if (value == null || StringUtils.isBlank(value.toString())){
                return;
            }
            String uncompressContent = StringCompressUtils.uncompress(value.toString());
            if (StringUtils.isBlank(uncompressContent) || uncompressContent.split(lineFeed).length == 0){
                return;
            }
            for (String line : uncompressContent.split(lineFeed)){
                if (StringUtils.isBlank(line) || line.split(split22).length != 2){
                    continue;
                }
                String[] values = line.split(split22);
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(values[0].getBytes());
                Put put = new Put(Bytes.toBytes(values[0]));
                String content = values[1];
                if (StringUtils.isNotBlank(content) && content.split(split21).length > 0){
                    for (String valContent : content.split(split21)){
                        if (StringUtils.isBlank(valContent)){
                            continue;
                        }
                        String uncompress = StringCompressUtils.uncompress(valContent);
                        if (StringUtils.isBlank(uncompress) || uncompress.split(split20).length != 3){
                            continue;
                        }
                        String[] valArr = uncompress.split(split20);
                        put.addColumn(Bytes.toBytes(valArr[0]), Bytes.toBytes(valArr[1]), Bytes.toBytes(valArr[2]));
                    }
                }
                context.write(rowKey, put);
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

}
