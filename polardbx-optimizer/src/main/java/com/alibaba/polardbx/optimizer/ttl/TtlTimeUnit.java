package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.druid.util.StringUtils;

/**
 * @author chenhui.lch
 */
public enum TtlTimeUnit {

    YEAR(0, "YEAR"),
    MONTH(1, "MONTH"),
    DAY(2, "DAY"),
    HOUR(3, "HOUR"),
    MINUTE(4, "MINUTE"),
    SECOND(5, "SECOND");
    private int timeUnitCode;
    private String timeUnitName;

    TtlTimeUnit(int code, String name) {
        this.timeUnitCode = code;
        this.timeUnitName = name;
    }

    public int getTimeUnitCode() {
        return timeUnitCode;
    }

    public String getTimeUnitName() {
        return timeUnitName;
    }

//    public static int convertInterval(
//                              Integer srcTimeUnitInterval,
//                              Integer srcTimeUnitCode,
//                              Integer tarTimeUnitCode
//                              ) {
//        switch (srcTimeUnitCode) {
//        case 0:
//            return TtlTimeUnit.YEAR;
//        case 1:
//            return TtlTimeUnit.MONTH;
//        case 2:
//            return TtlTimeUnit.DAY;
//        case 3:
//            return TtlTimeUnit.HOUR;
//        case 4:
//            return TtlTimeUnit.MINUTE;
//        case 5:
//            return TtlTimeUnit.SECOND;
//        }
//    }

    public static TtlTimeUnit of(Integer timeUnitCode) {
        switch (timeUnitCode) {
        case 0:
            return TtlTimeUnit.YEAR;
        case 1:
            return TtlTimeUnit.MONTH;
        case 2:
            return TtlTimeUnit.DAY;
        case 3:
            return TtlTimeUnit.HOUR;
        case 4:
            return TtlTimeUnit.MINUTE;
        case 5:
            return TtlTimeUnit.SECOND;
        }
        return null;
    }

    public static TtlTimeUnit of(String timeUnitName) {

        if (StringUtils.isEmpty(timeUnitName)) {
            return null;
        }

        if (TtlTimeUnit.YEAR.getTimeUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.YEAR;
        } else if (TtlTimeUnit.MONTH.getTimeUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.MONTH;
        } else if (TtlTimeUnit.DAY.getTimeUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.DAY;
        } else if (TtlTimeUnit.HOUR.getTimeUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.HOUR;
        } else if (TtlTimeUnit.MINUTE.getTimeUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.MINUTE;
        } else if (TtlTimeUnit.SECOND.getTimeUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.SECOND;
        } else {
            return null;
        }
    }
}
