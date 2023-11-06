package com.zyd.tech.hbasebulkload;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Test {
    public static void main(String[] args) {
        String split20 = Objects.toString((char) 20);
        List<String> array = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            array.add(StringCompressUtils.compress("ZHONGYUANDONG"));
        }
        String str1 = StringCompressUtils.compress(StringUtils.join(array, split20));

        String uncompress = StringCompressUtils.uncompress(str1);

        String[] arr = uncompress.split(split20);
        for (String cont : arr) {
            System.out.println(StringCompressUtils.uncompress(cont));
        }
    }
}
