package com.alibaba.polardbx.gms.lbac;

/**
 * @author pangzhaoxing
 */
public class PolarSecurityLabelColumn {

    public static final String COLUMN_NAME = "_polar_security_label";

    public static String COLUMN_TYPE = "varchar";

    public static boolean checkPSLName(String name) {
        return COLUMN_NAME.equalsIgnoreCase(name);
    }

    public static boolean checkPSLType(String type) {
        return COLUMN_TYPE.equalsIgnoreCase(type);
    }

}
