package com.zyd.tech.catalog.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Method;

public class FlinkUtil {
 
    /**
     * 获取 Flink 的 Configuration
     * @param env
     * @return
     */
    public static Configuration getConfiguration(StreamExecutionEnvironment env) {
        try {
            Method method = getMethod(env.getClass(),"getConfiguration");
            method.setAccessible(true);
 
            return (Configuration) method.invoke(env);
        } catch (Exception e) {
            e.printStackTrace();
            return GlobalConfiguration.loadConfiguration();
        }
    }
 
    /**
     * 获取类的方法，包括继承自父类中的方法
     * @param clazz
     * @param methodName
     * @return
     * @throws NoSuchMethodException
     */
    private static Method getMethod(Class clazz, String methodName) throws NoSuchMethodException {
        while (clazz != null) {
            Method[] methods = clazz.getDeclaredMethods();
            for (Method method : methods) {
                if (method.getName().equals(methodName)) {
                    return method;
                }
            }
            clazz = clazz.getSuperclass();
        }
        throw new NoSuchMethodException("Can't find method [" + methodName + "] from Class " + clazz.getName());
    }
 
}