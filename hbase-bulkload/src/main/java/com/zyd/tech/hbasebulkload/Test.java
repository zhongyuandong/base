package com.zyd.tech.hbasebulkload;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Test {
    public static void main(String[] args) {
        String input = "{\"aa\":123}";
        JSONObject json = JSON.parseObject(input);
        Integer bb = json.getIntValue("BB");
        System.out.println(bb);
    }
}
