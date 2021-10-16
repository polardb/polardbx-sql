

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

public class IntConfigParam extends ConfigParam {

    private static final String DEBUG_NAME = IntConfigParam.class.getName();

    private final Integer       min;
    private final Integer       max;

    public IntConfigParam(String configName, Integer minVal, Integer maxVal, Integer defaultValue, boolean mutable){

        super(configName, defaultValue.toString(), mutable);
        min = minVal;
        max = maxVal;
    }

    private void validate(Integer value) throws IllegalArgumentException {

        if (value != null) {
            if (min != null) {
                if (value.compareTo(min) < 0) {
                    throw new IllegalArgumentException(DEBUG_NAME + ":" + " param " + name + " doesn't validate, "
                                                       + value + " is less than min of " + min);
                }
            }
            if (max != null) {
                if (value.compareTo(max) > 0) {
                    throw new IllegalArgumentException(DEBUG_NAME + ":" + " param " + name + " doesn't validate, "
                                                       + value + " is greater than max of " + max);
                }
            }
        }
    }

    @Override
    public void validateValue(String value) throws IllegalArgumentException {

        try {
            validate(new Integer(value));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(DEBUG_NAME + ": " + value + " not valid value for " + name);
        }
    }
}
