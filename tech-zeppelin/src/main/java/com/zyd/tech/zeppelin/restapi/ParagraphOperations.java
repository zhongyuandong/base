package com.zyd.tech.zeppelin.restapi;

import com.zyd.common.utils.OkHttpUtils;

/**
 * @program: base
 * @description: Paragraph REST API测试
 * @author: zhongyuandong
 * @create: 2023-08-10 14:42:49
 * @Version 1.0
 **/
public class ParagraphOperations {

    public static void main(String[] args) {
        getTheStatus();
    }

    public static void getTheStatus (){
        String url = "http://uat-bigdata-20-84:8281/api/notebook/job/2J7VFYDQJ/paragraph_1691576494269_769428519";
        String async = OkHttpUtils.builder().url(url).get().async();
        System.out.println(async);
    }

}
