package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random;

import java.util.Arrays;

public class MyUtils {

    public static double[] scale(double[] arr, double scale) {
        double[] result = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = arr[i] * scale;
        }
        return result;
    }

    public static int[] scaleAndOffsetToInt(double[] arr, double scale, double offset) {
        int[] result = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = (int) (arr[i] * scale + offset);
        }
        return result;
    }

    public static double[] offset(double[] arr, double offset) {
        double[] result = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = arr[i] + offset;
        }
        return result;
    }

    public static int[] double2int(double[] arr) {
        int[] result = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = (int) arr[i];
        }
        return result;
    }

    public static double[] int2double(int[] arr) {
        double[] result = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            result[i] = arr[i];
        }
        return result;
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
