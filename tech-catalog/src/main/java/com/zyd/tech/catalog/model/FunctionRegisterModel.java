package com.zyd.tech.catalog.model;

import lombok.Data;
import lombok.experimental.Accessors;

/**
  * @ClassName FunctionRegisterModel
  * @Description 函数注册入参
  * @Author Joseph
  * @Date 2022/6/16 16:22
  * @Version 1.0
  */
@Data
@Accessors(chain = true)
public class FunctionRegisterModel {

    /**
     * catalog编码
     */
    private String catalogCode;

    /**
     * 默认数据库
     */
    private String defaultDatabase;

    /**
     * localDirPath
     */
    private String localDirPath;

    /**
     * 版本
     */
    private String version;

    /**
     * 函数名称
     */
    private String functionName;

    /**
     * 函数实现路径
     */
    private String functionImpl;

}
