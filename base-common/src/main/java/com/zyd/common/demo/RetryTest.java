package com.zyd.common.demo;

import com.zyd.common.utils.RetryUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: base
 * @description: demo示例
 * @author: zhongyuandong
 * @create: 2023-08-15 11:55:33
 * @Version 1.0
 **/
public class RetryTest {


    public static void main(String[] args) throws Exception {
        testRetry();
    }


    public static void testRetry () throws Exception {
        AtomicInteger count = new AtomicInteger(-1);
        RetryUtil.executeWithRetry(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                count.incrementAndGet();
                if (count.get() < 3){
                    System.out.println("重试次数：" + count.get());
                    throw new Exception("开始重试...");
                }
                return true;
            }
        }, 5, 200, false);
    }

}
