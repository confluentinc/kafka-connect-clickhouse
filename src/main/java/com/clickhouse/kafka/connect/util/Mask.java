package com.clickhouse.kafka.connect.util;

public class Mask {

    private static final String PASSWORD_MASK = "********";

    public static String passwordMask(String password) {
        return PASSWORD_MASK;
    }
}
