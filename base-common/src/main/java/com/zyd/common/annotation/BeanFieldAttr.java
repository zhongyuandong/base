package com.zyd.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * @program: base
 * @description:
 * @author: zhongyuandong
 * @create: 2023-09-28 17:37:31
 * @Version 1.0
 **/
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface BeanFieldAttr {

    /**
     * 标注该属性的顺序
     * @return 该属性的顺序
     */
    int order();

}
