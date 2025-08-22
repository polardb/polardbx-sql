package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.druid.util.StringUtils;

/**
 * @author chenhui.lch
 */
public enum TtlTimeUnit {

    UNDEFINED(-1, "UNDEFINED"),
    YEAR(0, "YEAR"),
    MONTH(1, "MONTH"),
    DAY(2, "DAY"),
    HOUR(3, "HOUR"),
    MINUTE(4, "MINUTE"),
    SECOND(5, "SECOND"),
    NUMBER(10, "NUMBER");
    private int unitCode;
    private String unitName;

    TtlTimeUnit(int code, String name) {
        this.unitCode = code;
        this.unitName = name;
    }

    public int getUnitCode() {
        return unitCode;
    }

    public String getUnitName() {
        return unitName;
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
        case -1:
            return TtlTimeUnit.UNDEFINED;
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
        case 10:
            return TtlTimeUnit.NUMBER;
        }
        return null;
    }

    public static TtlTimeUnit of(String timeUnitName) {

        if (StringUtils.isEmpty(timeUnitName)) {
            return null;
        }

        if (TtlTimeUnit.YEAR.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.YEAR;
        } else if (TtlTimeUnit.MONTH.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.MONTH;
        } else if (TtlTimeUnit.DAY.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.DAY;
        } else if (TtlTimeUnit.HOUR.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.HOUR;
        } else if (TtlTimeUnit.MINUTE.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.MINUTE;
        } else if (TtlTimeUnit.SECOND.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.SECOND;
        } else if (TtlTimeUnit.NUMBER.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.NUMBER;
        } else if (TtlTimeUnit.UNDEFINED.getUnitName().equalsIgnoreCase(timeUnitName)) {
            return TtlTimeUnit.UNDEFINED;
        } else {
            return null;
        }
    }

    public boolean isNumberUnit() {
        return this == TtlTimeUnit.NUMBER;
    }
}
