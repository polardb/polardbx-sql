/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.config.impl;

import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConfigParam;
import com.alibaba.polardbx.common.properties.FloatConfigParam;
import com.alibaba.polardbx.common.properties.IntConfigParam;
import com.alibaba.polardbx.common.properties.LongConfigParam;
import com.alibaba.polardbx.common.properties.StringConfigParam;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MAINTENANCE_TIME_END;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAINTENANCE_TIME_START;
import static com.alibaba.polardbx.common.properties.ConnectionParams.REBALANCE_MAINTENANCE_ENABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.REBALANCE_MAINTENANCE_TIME_END;
import static com.alibaba.polardbx.common.properties.ConnectionParams.REBALANCE_MAINTENANCE_TIME_START;

/**
 * @author fangwu
 */
public class InstConfUtil {
    private static final Logger logger = LoggerFactory.getLogger(InstConfUtil.class);

    private static final int MAX_TIME_CONFIG = 24 * 60;

    public static Map<String, Long> fetchLongConfigs(LongConfigParam... props) {
        Map<String, Long> results = new HashMap<>();
        for (LongConfigParam prop : props) {
            results.put(prop.getName(), getLong(prop));
        }
        return results;
    }

    public static boolean isInMaintenanceTimeWindow() {
        return isInMaintenanceTimeWindow(Calendar.getInstance(), MAINTENANCE_TIME_START, MAINTENANCE_TIME_END);
    }

    public static boolean isInRebalanceMaintenanceTimeWindow() {
        return isInRebalanceMaintenanceTimeWindow(REBALANCE_MAINTENANCE_TIME_START, REBALANCE_MAINTENANCE_TIME_END,
            REBALANCE_MAINTENANCE_ENABLE);
    }

    public static boolean isInRebalanceMaintenanceTimeWindow(StringConfigParam maintenanceTimeStart,
                                                             StringConfigParam maintenanceTimeEnd,
                                                             BooleanConfigParam rebalanceMainTenanceEnable) {
        boolean enabled = getBool(rebalanceMainTenanceEnable);
        if (!enabled) {
            return true;// no limit, always works.
        }
        String startTime = getOriginVal(maintenanceTimeStart);
        String endTime = getOriginVal(maintenanceTimeEnd);
        try {
            int startTimeInt = getMinute(startTime);
            int endTimeInt = getMinute(endTime);
            if (startTimeInt == endTimeInt) {
                return true;// no limit, always works.
            }
        } catch (Exception e) {
            logger.error(
                "rebalance maintenance time parse error, check config  " + maintenanceTimeStart + "/"
                    + maintenanceTimeEnd,
                e);
            return false;
        }
        return isInMaintenanceTimeWindow(Calendar.getInstance(),
            maintenanceTimeStart, maintenanceTimeEnd);
    }

    public static boolean isInMaintenanceTimeWindow(Calendar calendar, StringConfigParam maintenanceTimeStart,
                                                    StringConfigParam maintenanceTimeEnd) {
        int currentMinute = calendar.get(Calendar.MINUTE) + calendar.get(Calendar.HOUR_OF_DAY) * 60;
        String startTime = getOriginVal(maintenanceTimeStart);
        String endTime = getOriginVal(maintenanceTimeEnd);
        try {
            int startTimeInt = getMinute(startTime);
            int endTimeInt = getMinute(endTime);

            // check time config valid
            checkTimeValid(startTimeInt, endTimeInt);

            if (startTimeInt <= endTimeInt) {
                return startTimeInt <= currentMinute && currentMinute <= endTimeInt;
            } else {
                return (MAX_TIME_CONFIG >= currentMinute && currentMinute >= startTimeInt)
                    || endTimeInt >= currentMinute;
            }

        } catch (Exception e) {
            logger.error(
                "maintenance time parse error, check config  " + maintenanceTimeStart + "/" + maintenanceTimeEnd,
                e);
            return false;
        }
    }

    private static void checkTimeValid(int startTimeInt, int endTimeInt) throws Exception {
        if (startTimeInt < 0 ||
            endTimeInt < 0 ||
            startTimeInt > MAX_TIME_CONFIG ||
            endTimeInt > MAX_TIME_CONFIG) {
            throw new Exception("time config exceed range limit 0~1440:" + startTimeInt + "," + endTimeInt);
        }
    }

    /**
     * @param time like 05:00
     */
    public static int getMinute(String time) {
        int hour = Integer.parseInt(time.split(":")[0]);
        int minute = Integer.parseInt(time.split(":")[1]);

        return hour * 60 + minute;
    }

    public static Boolean getBool(BooleanConfigParam c) {
        String val = MetaDbInstConfigManager.getInstance().propertiesInfoMap.getProperty(c.getName());
        if (val == null) {
            val = c.getDefault();
        }
        return Boolean.valueOf(val).booleanValue();
    }

    public static Boolean getValBool(String name) {
        String val = MetaDbInstConfigManager.getInstance().getCnVariableConfigMap().getProperty(name);
        if (val == null) {
            return false;
        }
        return Boolean.valueOf(val).booleanValue();
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     *
     * @return default for param if it wasn't explicitly set
     */
    public static float getFloat(FloatConfigParam c) {

        /* See if it's specified. */
        String val = MetaDbInstConfigManager.getInstance().propertiesInfoMap.getProperty(c.getName());
        if (val == null) {
            val = c.getDefault();
        }
        float floatValue = 0;
        if (val != null) {
            try {
                floatValue = Float.parseFloat(val);
            } catch (NumberFormatException e) {
                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */

                assert false : e.getMessage();
            }
        }
        return floatValue;
    }

    /**
     * Get this parameter from the environment wide configuration settings.
     *
     * @return default for param if it wasn't explicitly set
     */
    public static long getLong(LongConfigParam c) {

        /* See if it's specified. */
        String val = MetaDbInstConfigManager.getInstance().propertiesInfoMap.getProperty(c.getName());
        if (val == null) {
            val = c.getDefault();
        }
        long longValue = 0;
        if (val != null) {
            try {
                longValue = Long.parseLong(val);
            } catch (NumberFormatException e) {
                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */

                assert false : e.getMessage();

            }
        }
        return longValue;
    }

    public static int getInt(IntConfigParam c) {
        String val = MetaDbInstConfigManager.getInstance().propertiesInfoMap.getProperty(c.getName());
        if (val == null) {
            val = c.getDefault();
        }
        int intValue = 0;
        if (val != null) {
            try {
                intValue = Integer.parseInt(val);
            } catch (NumberFormatException e) {

                /*
                 * This should never happen if we put error checking into the
                 * loading of config values.
                 */
                assert false : e.getMessage();
            }
        }
        return intValue;
    }

    public static String getOriginVal(ConfigParam c) {
        String val = MetaDbInstConfigManager.getInstance().propertiesInfoMap.getProperty(c.getName());
        if (val == null) {
            val = c.getDefault();
        }
        if (val == null) {
            val = "";
        }
        return val;
    }
}
