package org.ut.cs.dataeng_streams.connector.kym;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        String[] aa = StringUtils.split("\"abuu\":{}", "\"", 2);
        System.out.println(Arrays.toString(aa));
    }
}
