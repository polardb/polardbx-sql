

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

public class DurationConfigParam extends ConfigParam {

    private static final String DEBUG_NAME = DurationConfigParam.class.getName();

    private String              minString;
    private int                 minMillis;
    private String              maxString;
    private int                 maxMillis;

    public DurationConfigParam(String configName, String minVal, String maxVal, String defaultValue, boolean mutable){
        super(configName, defaultValue, mutable);
        if (minVal != null) {
            minString = minVal;
            minMillis = PropUtil.parseDuration(minVal);
        }
        if (maxVal != null) {
            maxString = maxVal;
            maxMillis = PropUtil.parseDuration(maxVal);
        }
    }

    @Override
    public void validateValue(String value) throws IllegalArgumentException {
        final int millis;
        try {

            millis = PropUtil.parseDuration(value);
        } catch (IllegalArgumentException e) {

            throw new IllegalArgumentException(DEBUG_NAME + ":" + " param " + name + " doesn't validate, " + value
                                               + " fails validation: " + e.getMessage());
        }

        if (minString != null) {
            if (millis < minMillis) {
                throw new IllegalArgumentException(DEBUG_NAME + ":" + " param " + name + " doesn't validate, " + value
                                                   + " is less than min of " + minString);
            }
        }
        if (maxString != null) {
            if (millis > maxMillis) {
                throw new IllegalArgumentException(DEBUG_NAME + ":" + " param " + name + " doesn't validate, " + value
                                                   + " is greater than max of " + maxString);
            }
        }
    }
}
