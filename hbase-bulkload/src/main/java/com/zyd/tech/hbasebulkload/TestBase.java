package com.zyd.tech.hbasebulkload;

import org.apache.log4j.Logger;

/**
 * @program: hbase-bulkload
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-04 01:11:29
 * @Version 1.0
 **/
public class TestBase {

    final static Logger logger = Logger.getLogger(TestBase.class);

    public static void main(String[] args) {

        logger.info("进入main.....");

        for (int i = 0; i < 10; i++) {
            logger.info(String.format("循环：%d", i));
        }
    }

}
