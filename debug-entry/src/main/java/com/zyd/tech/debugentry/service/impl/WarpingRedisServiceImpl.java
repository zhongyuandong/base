package com.zyd.tech.debugentry.service.impl;

import com.alibaba.fastjson.JSON;
import com.zyd.tech.debugentry.bean.warping.ShiftInfo;
import com.zyd.tech.debugentry.dao.IRedisBaseDao;
import com.zyd.tech.debugentry.service.WarpingRedisService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: base
 * @description: 整经机sevice 实现类
 * @author: zhongyuandong
 * @create: 2023-11-27 11:19:22
 * @Version 1.0
 **/
@Service
public class WarpingRedisServiceImpl implements WarpingRedisService {


    @Autowired
    IRedisBaseDao redisBaseDao;

    /*
    * @Description: 获取班次信息
    * @Param: [key]
    * @return: java.util.List<com.zyd.tech.debugentry.bean.warping.ShiftInfo>
    * @Author: zhongyuandong
    * @Date: 2023-11-27 11:28
    */
    @Override
    public List<ShiftInfo> queryShiftInfo (String key) {
        List<String> shiftInfos = redisBaseDao.lRange(key, 0, -1);
        if (CollectionUtils.isEmpty(shiftInfos)) {
            return null;
        }
        List<ShiftInfo> shifts = shiftInfos.stream().filter(s -> JSON.isValidObject(s))
                .map(s -> JSON.parseObject(s, ShiftInfo.class)).collect(Collectors.toList());
        return shifts;
    }

    /*
    * @Description: 根据时间获取班次信息
    * @Param: [key, time]
    * @return: java.util.List<com.zyd.tech.debugentry.bean.warping.ShiftInfo>
    * @Author: zhongyuandong
    * @Date: 2023-11-28 17:13
    */
    @Override
    public List<ShiftInfo> queryShiftInfo (String key, long time) {
        List<ShiftInfo> shifts = queryShiftInfo(key);
        if (CollectionUtils.isEmpty(shifts)) {
            return null;
        }
        List<ShiftInfo> currentShift = shifts.stream().filter(shift -> {
            if (shift == null || StringUtils.isBlank(shift.getShiftId()) ||
                    shift.getStartTime() == null || shift.getEndTime() == null){
                return false;
            }
            Long startTime = shift.getStartTime();
            Long endTime = shift.getEndTime();
            if (startTime.longValue() <= time && time < endTime.longValue()) {
                return true;
            }
            return false;
        }).map(s -> s.initDate()).collect(Collectors.toList());
        return currentShift;
    }


}
