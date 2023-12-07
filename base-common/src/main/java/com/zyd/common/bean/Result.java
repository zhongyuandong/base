package com.zyd.common.bean;

import com.zyd.common.constance.Constants;
import com.zyd.common.enumeration.ResponseEnum;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * @program: base
 * @description: 通用返回结果
 * @author: zhongyuandong
 * @create: 2023-11-27 11:35:58
 * @Version 1.0
 **/
@Data
@Accessors(chain = true)
public class Result<T> implements Serializable {

    private static final long serialVersionUID = 4299226193009385159L;

    /**
     * 业务响应状态码
     */
    private String code;

    /**
     * 业务响应信息
     */
    private String msg;

    /**
     * 业务响应数据对象
     */
    private T data;

    /**
     * 前端兼容apaas,成功时时1，失败时是-1
     */
    private Integer status;

    public Result() {
        this.code = ResponseEnum.SUCCESS.name();
        this.msg = ResponseEnum.SUCCESS.getMsg();
        this.status = Constants.NUMBER_ZERO;
    }


    public Result(T data) {
        this.code = ResponseEnum.SUCCESS.name();
        this.msg = ResponseEnum.SUCCESS.getMsg();
        this.data = data;
        this.status = Constants.NUMBER_ZERO;
    }

    private Result(ResponseEnum status) {
        if (status != null) {
            this.code = status.name();
            this.msg = status.getMsg();
            this.status = ResponseEnum.SUCCESS.equals(status) ? Constants.NUMBER_ZERO : -1;
        }
    }

    private Result(String code, String msg) {
        this.code = code;
        this.msg = msg;
        this.status = ResponseEnum.SUCCESS.name().equals(code) ? Constants.NUMBER_ZERO : -1;
    }

    /**
     * Call this function if there is success
     *
     * @param data data
     * @param <T>  type
     * @return resule
     */
    public static <T> Result<T> success(T data) {
        return new Result<>(data);
    }

    /**
     * Call this function if there is any error
     *
     * @param status status
     * @return result
     */
    public static Result error(ResponseEnum status) {
        return new Result(status);
    }

    public static Result error(String code, String msg) {
        return new Result(code, msg);
    }
}
