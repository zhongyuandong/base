package com.zyd.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ToMap {

    /**默认初始化类型*/
    Class type() default String.class;

    /**忽略该属性*/
    boolean ignore() default false;

    /**属性值不能为空*/
    boolean notNull() default false;

    String name() default "";

}
