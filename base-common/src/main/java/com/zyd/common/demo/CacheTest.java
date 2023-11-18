package com.zyd.common.demo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.zyd.common.utils.StringCompressUtils;

import java.util.concurrent.TimeUnit;

/**
 * @program: sz-streaming-marking
 * @description:
 * @author: zhongyuandong
 * @create: 2023-11-13 15:48:34
 * @Version 1.0
 **/
public class CacheTest {

    static Cache<String, String> cache = CacheBuilder.newBuilder()
            .expireAfterWrite(5,TimeUnit.SECONDS) // 设置过期时间为10分钟
                .build();

    public static void main(String[] args) throws InterruptedException {
        String str = String.format("%s@%s@%s", "952962519542861824", "952962519542861827", "22.05");
        System.out.println("压缩前：" + str.length());
        System.out.println("压缩后：" + StringCompressUtils.compress(str).length());
//        cache.put("key1", "value1");
//        cache.put("key2", "value2");
//        StringCompressUtils.compress("")
//        for (int i = 0; i < 10; i++) {
//            String value = cache.getIfPresent("key1");
//            System.out.println("value == " + value);
//            Thread.sleep(1000);
//        }

    }

}
