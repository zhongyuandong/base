package com.zyd.tech.debugentry.service;

import com.zyd.tech.debugentry.bean.warping.ShiftInfo;

import java.util.List;

/**
 * @program: base
 * @description: 整经机redis service
 * @author: zhongyuandong
 * @create: 2023-11-27 11:08:55
 * @Version 1.0
 **/
public interface WarpingRedisService {

    /*
     * @Description: 根据时间获取班次信息
     * @Param: [key, time]
     * @return: java.util.List<com.zyd.tech.debugentry.bean.warping.ShiftInfo>
     * @Author: zhongyuandong
     * @Date: 2023-11-28 17:13
     */
    List<ShiftInfo> queryShiftInfo (String key, long time);

    /*
     * @Description: 获取班次信息
     * @Param: [key]
     * @return: java.util.List<com.zyd.tech.debugentry.bean.warping.ShiftInfo>
     * @Author: zhongyuandong
     * @Date: 2023-11-27 11:28
     */
    List<ShiftInfo> queryShiftInfo (String key);
}
