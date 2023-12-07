package com.zyd.tech.debugentry.bean.warping;

import lombok.Data;
import lombok.experimental.Accessors;

import java.text.SimpleDateFormat;

/**
 * @program: warping-streaming
 * @description: 班次基础信息
 * @author: zhongyuandong
 * @create: 2023-11-22 16:48:28
 * @Version 1.0
 **/
@Data
@Accessors(chain = true)
public class ShiftInfo {

    /**班次日期，时间戳, 毫秒*/
    private Long scheduleDate;

    /**班次ID*/
    private String shiftId;

    /**班次名称 eg:对班早*/
    private String shiftName;

    /**班次开始时间戳，单位：毫秒*/
    private Long startTime;

    /**班次结束时间戳，单位：毫秒*/
    private Long endTime;

    /**班次开始时间*/
    private String startTimeFormat;

    /**班次结束时间*/
    private String endTimeFormat;

    /**班次日期*/
    private String scheduleDateFormat;

    public ShiftInfo initDate (){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (this.startTime != null) {
            this.startTimeFormat = sdf.format(this.startTime);
        }
        if (this.endTime != null) {
            this.endTimeFormat = sdf.format(this.endTime);
        }
        if (this.scheduleDate != null) {
            this.scheduleDateFormat = sdf.format(this.scheduleDate);
        }
        return this;
    }


}

