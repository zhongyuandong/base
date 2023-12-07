package com.zyd.tech.debugentry.controller;

import com.zyd.tech.debugentry.bean.warping.Result;
import com.zyd.tech.debugentry.bean.warping.ShiftInfo;
import com.zyd.tech.debugentry.enumeration.ResponseEnum;
import com.zyd.tech.debugentry.service.WarpingRedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

/**
 * @program: base
 * @description: 整经机接口
 * @author: zhongyuandong
 * @create: 2023-11-27 10:40:32
 * @Version 1.0
 **/
@RestController
@RequestMapping("/redis")
public class WarpingController {

    Logger log = LoggerFactory.getLogger(WarpingController.class);

    @Autowired
    private WarpingRedisService warpingRedisService;

    /* 
    * @Description: lRange 查询
    * @Param: [key] 
    * @return: com.zyd.common.bean.Result<java.util.List<com.zyd.tech.debugentry.bean.warping.ShiftInfo>> 
    * @Author: zhongyuandong
    * @Date: 2023-11-28 17:08
    */
    @PostMapping("indexList")
    public Result<List<ShiftInfo>> lRange(String key) {
        log.info("redis lRange 查看:{}", key);
        Result result = new Result<List<ShiftInfo>>();
        List<ShiftInfo> shiftInfos = null;
        try {
            shiftInfos = warpingRedisService.queryShiftInfo(key);
        } catch (Exception e) {
            result.setCode(ResponseEnum.FAIL.name()).setMsg(e.getMessage());
            log.error("查询redis key[{}] 失败：{}", key, e.getMessage());
        }
        return result.setData(shiftInfos);
    }

    /*
     * @Description: 查询当前班次
     * @Param: [key]
     * @return: com.zyd.common.bean.Result<java.util.List<com.zyd.tech.debugentry.bean.warping.ShiftInfo>>
     * @Author: zhongyuandong
     * @Date: 2023-11-28 17:08
     */
    @PostMapping("queryCurrentShifts")
    public Result<List<ShiftInfo>> queryCurrentShifts(String key, Long time) {
        log.info("查询当前班次,key:{} time:{}", key, time);
        Result result = new Result<List<ShiftInfo>>();
        List<ShiftInfo> shiftInfos = null;
        try {
            shiftInfos = warpingRedisService.queryShiftInfo(key, time);
        } catch (Exception e) {
            result.setCode(ResponseEnum.FAIL.name()).setMsg(e.getMessage());
            log.error("查询redis key[{}] 失败：{}", key, e.getMessage());
        }
        return result.setData(shiftInfos);
    }

}
