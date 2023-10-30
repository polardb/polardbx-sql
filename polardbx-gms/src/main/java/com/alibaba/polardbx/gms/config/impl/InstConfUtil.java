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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MAINTENANCE_TIME_END;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAINTENANCE_TIME_START;

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
        return isInMaintenanceTimeWindow(Calendar.getInstance());
    }

    public static boolean isInMaintenanceTimeWindow(Calendar calendar) {
        int currentMinute = calendar.get(Calendar.MINUTE) + calendar.get(Calendar.HOUR_OF_DAY) * 60;
        String startTime = getOriginVal(MAINTENANCE_TIME_START);
        String endTime = getOriginVal(MAINTENANCE_TIME_END);
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
                "maintenance time parse error, check config  " + MAINTENANCE_TIME_START + "/" + MAINTENANCE_TIME_END,
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
    private static int getMinute(String time) {
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
