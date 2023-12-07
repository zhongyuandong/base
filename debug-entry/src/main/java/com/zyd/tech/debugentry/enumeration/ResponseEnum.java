package com.zyd.tech.debugentry.enumeration;

/**
 * @program: base
 * @description: 通用返回结构枚举
 * @author: zhongyuandong
 * @create: 2023-11-27 11:37:34
 * @Version 1.0
 **/
public enum ResponseEnum {

    SUCCESS("成功"),
    FAIL("操作失败"),
    UPLOAD_FILE_EMPTY("上传文件为空"),
    UPLOAD_FILE_VALIDATE_FAIL("上传文件校验失败"),
    UPLOAD_FILE_DOMAIN_NOT_SUPPORTS("不支持的文件上传业务域"),
    IAM_LOGIN_DIRECT("iam登录重定向"),
    DATA_NOT_EXISTS("数据不存在"),
    HTTP_CLIENT_CONNECTION_FAIL("HTTP请求失败"),
    DATABASE_CONTROL_FAILED("数据库操作失败！");

    /**
     * 响应说明
     */
    private String msg;

    ResponseEnum(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }


}
