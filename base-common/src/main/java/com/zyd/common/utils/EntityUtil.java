package com.zyd.common.utils;

import com.alibaba.fastjson.JSON;
import com.zyd.common.annotation.BeanFieldAttr;
import com.zyd.common.annotation.ToMap;
import com.zyd.common.enumeration.ToMapType;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-28 17:36:38
 * @Version 1.0
 **/
public class EntityUtil {

    /**
     * 根据注解 @BeanFieldAttr 和 参数arr 给对象赋值初始化
     * @param <E>
     * @param entity 初始化对象
     * @param arr 属性值
     * @return
     * @throws Exception
     */
    public static <E> E initialize (E entity, String msg, String splitStr) throws Exception {
        if (StringUtils.isBlank(msg) || StringUtils.isBlank(splitStr)) {
            return null;
        }
        String[] arr = msg.split(splitStr);
        if (arr == null || arr.length == 0) {
            return null;
        }
        Pattern compile = Pattern.compile("^-?\\d+(\\.\\d+)?$");
        Field[] fields = entity.getClass().getDeclaredFields();
        BeanFieldAttr annotation = null;
        String fieldValue = null;
        Class<?> type = null;
        try {
            for (Field field : fields) {
                annotation = field.getAnnotation(BeanFieldAttr.class);
                if (annotation != null && annotation.order() < arr.length) {
                    fieldValue = arr[annotation.order()];
                    if (StringUtils.isBlank(fieldValue)) {
                        continue;
                    }
                    field.setAccessible(true);
                    type = field.getType();
                    if (type == String.class) {
                        field.set(entity, fieldValue);
                    } else if (type == Long.class && StringUtils.isNumeric(fieldValue)) {
                        field.set(entity, Long.parseLong(fieldValue));
                    } else if (type == Integer.class && StringUtils.isNumeric(fieldValue)) {
                        field.set(entity, Integer.parseInt(fieldValue));
                    } else if (type == Double.class) {
                        try {
                            field.set(entity, Double.valueOf(fieldValue));
                        }catch (NumberFormatException e) {
                            throw new NumberFormatException(String.format("【%s】暂不支持【%s】字段类型转换！", fieldValue, type));
                        }
                    } else if (type == BigDecimal.class && compile.matcher(fieldValue).matches()){
                        field.set(entity, new BigDecimal(fieldValue));
                    } else {
                        throw new UnsupportedOperationException(String.format("【%s】暂不支持【%s】字段类型转换！", fieldValue, type));
                    }
                }
            }
            annotation = null;
            fieldValue = null;
        } catch (Exception e) {
            throw new Exception(String.format("初始化对象，消息体【%s】异常【%s】", msg.replaceAll(splitStr, ","), e.getMessage()));
        }
        return entity;
    }

    /**
     * 根据 @BeanFieldAttr 注解，把对象属性值按分隔符split 拼接
     * @param <E>
     * @param entity
     * @return
     * @throws Exception
     */
    public static <E> String fieldToString (E entity, String split) throws Exception {
        Field[] fields = entity.getClass().getDeclaredFields();
        BeanFieldAttr annotation = null;
        List<Object> list = new LinkedList<Object>();
        Map<Integer, Object> map = new HashMap<Integer, Object>();
        try {
            for (Field field : fields) {
                annotation = field.getAnnotation(BeanFieldAttr.class);
                if (annotation != null) {
                    field.setAccessible(true);
                    map.put(annotation.order(), field.get(entity));
                }
            }
            annotation = null;
        } catch (Exception e) {
            throw new Exception("初始化对象异常：" + e.getMessage());
        }
        List<Entry<Integer, Object>> listMap = new LinkedList<Entry<Integer, Object>>(map.entrySet());
        Collections.sort(listMap, new Comparator<Entry<Integer, Object>>() {
            //升序排序
            public int compare(Entry<Integer, Object> o1,
                               Entry<Integer, Object> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        for(Entry<Integer, Object> mapping : listMap) {
            list.add(mapping.getValue());
        }
        return StringUtils.join(list, split);
    }

    /*
     * @Description: bean对象转map
     * @Param: [obj]
     * @return: java.util.Map<java.lang.String,java.lang.Object>
     * @Author: zyd
     * @Date: 2020-04-01 17:35
     */
    public static Map<String, Object> objectToMap(Object obj) throws IllegalAccessException {
        Map<String, Object> map = new HashMap<String,Object>();
        if (obj == null){
            return map;
        }
        Class<?> clazz = obj.getClass();
        String fieldName = null;
        for (Field field : clazz.getDeclaredFields()){
            field.setAccessible(true);
            fieldName = field.getName();
            if (StringUtils.isBlank(fieldName)) {
                continue;
            }
            map.put(fieldName, field.get(obj));
        }
        return map;
    }

    /*
     * @Description: bean对象转map
     * @Param: [entity]
     * @return: java.util.Map<java.lang.String,java.lang.String>
     * @Author: zyd
     * @Date: 2021-07-28 11:06
     */
    public static Map<String, String> toMap (Object entity) throws Exception{
        return toMap(entity, null);
    }

    public static Map<String, String> toMap (Object entity, ToMapType type) throws Exception{
        Map<String, String> map = new HashMap<String, String>();
        if (entity == null){
            return map;
        }
        Field[] fields = entity.getClass().getDeclaredFields();
        if (fields == null || fields.length == 0){
            return map;
        }
        ToMap annotation = null;
        Object value = null;
        String fileName = null;
        for (Field field : fields){
            field.setAccessible(true);
            annotation = field.getAnnotation(ToMap.class);
            if (annotation != null && annotation.ignore()){
                continue;
            }
            value = field.get(entity);
            if (value == null && annotation != null && annotation.notNull()){
                continue;
            }
            if (value == null && type == ToMapType.MUST_NOT_NULL){
                continue;
            }
            if (value == null){
                value = "";
            }
            if (value != null && annotation != null && annotation.type() == JSON.class){
                value = JSON.toJSONString(value);
            }
            fileName = field.getName();
            if (annotation != null && StringUtils.isNotBlank(annotation.name())){
                fileName = annotation.name();
            }
            if (StringUtils.isBlank(fileName)){
                continue;
            }
            //把属性名-属性值 存到Map中
            map.put(fileName, Objects.toString(value));
        }
        return map;
    }


}
