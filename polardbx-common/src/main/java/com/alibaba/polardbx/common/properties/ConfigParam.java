

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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;


public class ConfigParam {

    protected String      name;
    private final String  defaultValue;
    private final boolean mutable;
    private boolean       isMultiValueParam;

    public ConfigParam(String configName, String configDefault, boolean mutable) throws IllegalArgumentException{
        if (configName == null) {
            name = null;
        } else {

            int mvFlagIdx = configName.indexOf(".#");
            if (mvFlagIdx < 0) {
                name = configName;
                isMultiValueParam = false;
            } else {
                name = configName.substring(0, mvFlagIdx);
                isMultiValueParam = true;
            }
        }

        defaultValue = configDefault;
        this.mutable = mutable;

        validateName(name);

        ConnectionParams.addSupportedParam(this);
    }

    public static String multiValueParamName(String paramName) {
        int mvParamIdx = paramName.lastIndexOf('.');
        if (mvParamIdx < 0) {
            return null;
        }
        return paramName.substring(0, mvParamIdx);
    }

    public static String mvParamIndex(String paramName) {
        int mvParamIdx = paramName.lastIndexOf('.');
        return paramName.substring(mvParamIdx + 1);
    }

    public String getName() {
        return name;
    }

    public String getDefault() {
        return defaultValue;
    }

    public boolean isMutable() {
        return mutable;
    }

    public boolean isMultiValueParam() {
        return isMultiValueParam;
    }

    private void validateName(String name) throws IllegalArgumentException {
        if ((name == null) || (name.length() < 1)) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG,
                "A configuration parameter name can't be null or 0 length");
        }
    }

    public void validateValue(String value) throws IllegalArgumentException {

    }

    @Override
    public String toString() {
        return name;
    }
}
