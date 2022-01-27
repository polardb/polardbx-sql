

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

package com.alibaba.polardbx.common.properties;

import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;


public class PropUtil {

    public static final String MPP_LOCALCLUSTER = "localcluster";

    /**
     * Converts the given duration (interval value plus unit) to milliseconds,
     * ensuring that any given value greater than zero converts to at least one
     * millisecond to avoid a zero millisecond result, since Object.wait(0)
     * waits forever.
     *
     * @throws IllegalArgumentException if the duration argument is illegal.
     * Thrown via API setter methods such as Transaction.setLockTimeout.
     */
    public static int durationToMillis(final long val, final TimeUnit unit) {
        if (val == 0) {

            return 0;
        }
        if (unit == null) {
            throw new IllegalArgumentException("Duration TimeUnit argument may not be null if interval "
                + "is non-zero");
        }
        if (val < 0) {
            throw new IllegalArgumentException("Duration argument may not be negative: " + val);
        }
        final long newVal = unit.toMillis(val);
        if (newVal == 0) {

            return 1;
        }
        if (newVal > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Duration argument may not be greater than "
                + "Integer.MAX_VALUE milliseconds: " + newVal);
        }
        return (int) newVal;
    }

    public static long millisToDuration(final int val, final TimeUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("TimeUnit argument may not be null");
        }
        return unit.convert(val, TimeUnit.MILLISECONDS);
    }

    public static int parseDuration(final String property) {
        StringTokenizer tokens = new StringTokenizer(property.toUpperCase(java.util.Locale.ENGLISH), " \t");
        if (!tokens.hasMoreTokens()) {
            throw new IllegalArgumentException("Duration argument is empty");
        }
        final long time;
        try {
            time = Long.parseLong(tokens.nextToken());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Duration argument does not start with a long integer: " + property);
        }

        long millis;
        if (tokens.hasMoreTokens()) {
            final String unitName = tokens.nextToken();
            if (tokens.hasMoreTokens()) {
                throw new IllegalArgumentException("Duration argument has extra characters after unit: " + property);
            }
            try {
                final TimeUnit unit = TimeUnit.valueOf(unitName);
                millis = TimeUnit.MILLISECONDS.convert(time, unit);
            } catch (IllegalArgumentException e) {
                try {
                    final IEEETimeUnit unit = IEEETimeUnit.valueOf(unitName);
                    millis = unit.toMillis(time);
                } catch (IllegalArgumentException e2) {
                    throw new IllegalArgumentException("Duration argument has unknown unit name: " + property);
                }
            }
        } else {

            millis = TimeUnit.MILLISECONDS.convert(time, TimeUnit.MICROSECONDS);
        }

        if (time > 0 && millis == 0) {
            return 1;
        }
        if (millis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Duration argument may not be greater than "
                + "Integer.MAX_VALUE milliseconds: " + property);
        }
        return (int) millis;
    }

    public static String formatDuration(long time, TimeUnit unit) {
        return String.valueOf(time) + ' ' + unit.name();
    }

    private enum IEEETimeUnit {

        NS() {
            long toMillis(long val) {
                return millisUnit.convert(val, TimeUnit.NANOSECONDS);
            }
        },

        US() {
            long toMillis(long val) {
                return millisUnit.convert(val, TimeUnit.MICROSECONDS);
            }
        },

        MS() {
            long toMillis(long val) {
                return millisUnit.convert(val, TimeUnit.MILLISECONDS);
            }
        },

        S() {
            long toMillis(long val) {
                return millisUnit.convert(val, TimeUnit.SECONDS);
            }
        },

        MIN() {
            long toMillis(long val) {
                return val * 60000;
            }
        },

        H() {
            long toMillis(long val) {
                return val * 3600000;
            }
        };

        private static final TimeUnit millisUnit = TimeUnit.MILLISECONDS;

        abstract long toMillis(long val);
    }

    public enum AutoTrueFalse {
        AUTO, TRUE, FALSE;

        public boolean toBoolean() {
            switch (this) {
            case TRUE:
                return true;
            case FALSE:
                return false;
            case AUTO:
            default:
                throw new AssertionError();
            }
        }
    }

    public enum ExplainOutputFormat {
        TEXT, JSON
    }

    /**
     * DEFAULT_VALUE_MODE mean the empty char will be replace by default value
     * NULL_MODE mean the empty char will be replace by null value
     * N_MODE mean the "\N" char will be replace by null value,
     */
    public enum LOAD_NULL_MODE {
        DEFAULT_VALUE_MODE, NULL_MODE, N_MODE, DEFAULT_VALUE_AND_N_MODE
    }

    public static String getCluster(String defaultClusterName) {
        return System.getProperty(MPP_LOCALCLUSTER) != null ?
            System.getProperty(MPP_LOCALCLUSTER) : defaultClusterName;
    }
}
