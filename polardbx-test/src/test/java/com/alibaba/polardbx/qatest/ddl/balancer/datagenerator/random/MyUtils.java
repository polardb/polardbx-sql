package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random;

import java.util.Arrays;

public class MyUtils {

    public static double[] scale(double[] arr, double scale) {
        return Arrays.stream(arr).map(e -> e * scale).toArray();
    }

    public static double[] offset(double[] arr, double offset) {
        return Arrays.stream(arr).map(e -> e + offset).toArray();
    }

    public static int[] double2int(double[] arr) {
        return Arrays.stream(arr).mapToInt(i -> (int) i).toArray();
    }

    public static double[] int2double(int[] arr) {
        return Arrays.stream(arr).mapToDouble(i -> (double) i).toArray();
    }

    public static String[] double2String(double[] arr) {
        String[] result = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = String.valueOf(arr[i]);
        }
        return result;
    }

    public static String[] int2String(int[] arr) {
        String[] result = new String[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = String.valueOf(arr[i]);
        }
        return result;
    }

}
