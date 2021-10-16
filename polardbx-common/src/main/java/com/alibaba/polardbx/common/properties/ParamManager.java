

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ParamManager {

    public static final String PROPFILE_NAME = "tddl.properties";

    protected Map<String, String> props = new HashMap<String, String>();

    public ParamManager(Map connectionMap) {
        if (connectionMap != null) {
            props = connectionMap;
        }
    }

    public String get(ConfigParam configParam) {
        return getConfigParam(props, configParam.getName());
    }

    public String get(String configParamName) {
        return getConfigParam(props, configParamName);
    }

    public boolean getBoolean(BooleanConfigParam configParam) {

        String val = get(configParam);
        return Boolean.valueOf(val).booleanValue();
    }

    public boolean getBoolean(BooleanConfigParam configParam, boolean defaultValue) {
        if (!props.containsKey(configParam.name)) {
            return defaultValue;
        }
        return getBoolean(configParam);
    }

    public short getShort(ShortConfigParam configParam) {

        String val = get(configParam);
        short shortValue = 0;
        if (val != null) {
            try {
                shortValue = Short.parseShort(val);
            } catch (NumberFormatException e) {

                assert false : e.getMessage();
            }
        }
        return shortValue;
    }

    public int getInt(IntConfigParam configParam) {

        String val = get(configParam);
        int intValue = 0;
        if (val != null) {
            try {
                intValue = Integer.parseInt(val);
            } catch (NumberFormatException e) {

                assert false : e.getMessage();
            }
        }
        return intValue;
    }

    public long getLong(LongConfigParam configParam) {

        String val = get(configParam);
        long longValue = 0;
        if (val != null) {
            try {
                longValue = Long.parseLong(val);
            } catch (NumberFormatException e) {

                assert false : e.getMessage();
            }
        }
        return longValue;
    }

    public float getFloat(FloatConfigParam configParam) {

        String val = get(configParam);
        float floatValue = 0;
        if (val != null) {
            try {
                floatValue = Float.parseFloat(val);
            } catch (NumberFormatException e) {

                assert false : e.getMessage();
            }
        }
        return floatValue;
    }

    public Enum getEnum(EnumConfigParam configParam) {
        String val = get(configParam);
        return configParam.getValue(val);
    }

    public String getString(StringConfigParam configParam) {
        return get(configParam);
    }

    public int getDuration(DurationConfigParam configParam) {
        String val = get(configParam);
        int millis = 0;
        if (val != null) {
            try {
                millis = PropUtil.parseDuration(val);
            } catch (IllegalArgumentException e) {

                assert false : e.getMessage();
            }
        }
        return millis;
    }

    @SuppressWarnings("unchecked")
    public static void validateMap(Map<String, String> props, boolean isRepConfigInstance, String configClassName)
        throws IllegalArgumentException {

        Iterator propNames = props.keySet().iterator();
        while (propNames.hasNext()) {
            String name = (String) propNames.next();

            ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(name);

            if (param == null) {

                String mvParamName = ConfigParam.multiValueParamName(name);
                param = ConnectionParams.SUPPORTED_PARAMS.get(mvParamName);

                if (param == null) {

                    if (configClassName == null && !isRepConfigInstance) {
                        props.remove(name);
                        continue;
                    }

                    throw new IllegalArgumentException(name + " is not a valid BDBJE environment configuration");
                }
            }

            param.validateValue(props.get(name));
        }
    }

    public static void setConfigParam(Map<String, String> props, String paramName, String value,
                                      boolean requireMutability, boolean validateValue, boolean forReplication,
                                      boolean verifyForReplication) throws IllegalArgumentException {

        boolean isMVParam = false;

        ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(paramName);

        if (param == null) {

            String mvParamName = ConfigParam.multiValueParamName(paramName);
            param = ConnectionParams.SUPPORTED_PARAMS.get(mvParamName);
            if (param == null || !param.isMultiValueParam()) {
                throw new IllegalArgumentException(paramName + " is not a valid BDBJE environment parameter");
            }
            isMVParam = true;
            assert param.isMultiValueParam();
        }

        if (requireMutability && !param.isMutable()) {
            throw new IllegalArgumentException(paramName + " is not a mutable BDBJE environment configuration");
        }

        if (isMVParam) {
            setVal(props, param, paramName, value, validateValue);
        } else {
            setVal(props, param, value, validateValue);
        }
    }

    public static String getConfigParam(Map<String, String> props, String paramName) throws IllegalArgumentException {

        boolean isMVParam = false;

        ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(paramName);
        if (param == null) {

            String mvParamName = ConfigParam.multiValueParamName(paramName);
            param = ConnectionParams.SUPPORTED_PARAMS.get(mvParamName);
            if (param == null) {
                throw new IllegalArgumentException(paramName + " is not a valid BDBJE environment configuration");
            }
            isMVParam = true;
            assert param.isMultiValueParam();
        } else if (param.isMultiValueParam()) {
            throw new IllegalArgumentException("Use getMultiValueValues() to retrieve Multi-Value "
                + "parameter values.");
        }

        if (isMVParam) {
            return ParamManager.getVal(props, param, paramName);
        }
        return ParamManager.getVal(props, param);
    }

    public static String getVal(Map<String, String> props, ConfigParam param) {

        Object o = props.get(param.getName());

        if (o == null) {
            o = param.getDefault();

            if (o == null) {
                return null;
            }
        }
        String val = String.valueOf(o);

        return val;
    }

    public static String getVal(Map<String, String> props, ConfigParam param, String paramName) {
        String val = props.get(paramName);
        if (val == null) {
            val = param.getDefault();
        }
        return val;
    }

    public static void setVal(Map<String, String> props, ConfigParam param, String val, boolean validateValue)
        throws IllegalArgumentException {

        if (validateValue) {
            param.validateValue(val);
        }
        props.put(param.getName(), val);
    }

    public static void setVal(Map<String, String> props, ConfigParam param, String paramName, String val,
                              boolean validateValue) throws IllegalArgumentException {

        if (validateValue) {
            param.validateValue(val);
        }
        props.put(paramName, val);
    }

    public static int getIntVal(Map<String, String> props, IntConfigParam param) {
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Bad value for " + param.getName() + ": "
                + e.getMessage());
        }
    }

    public static long getLongVal(Map<String, String> props, LongConfigParam param) {
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Bad value for " + param.getName() + ": "
                + e.getMessage());
        }
    }

    public static void setIntVal(Map<String, String> props, IntConfigParam param, int val, boolean validateValue) {
        setVal(props, param, Integer.toString(val), validateValue);
    }

    public static boolean getBooleanVal(Map<String, String> props, BooleanConfigParam param) {
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        return Boolean.parseBoolean(val);
    }

    public static void setBooleanVal(Map<String, String> props, BooleanConfigParam param, boolean val,
                                     boolean validateValue) {
        setVal(props, param, Boolean.toString(val), validateValue);
    }

    public static long getDurationVal(Map<String, String> props, DurationConfigParam param, TimeUnit unit) {
        if (unit == null) {
            throw new IllegalArgumentException("TimeUnit argument may not be null");
        }
        String val = ParamManager.getVal(props, param);
        if (val == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "No value for " + param.getName());
        }
        try {
            return unit.convert(PropUtil.parseDuration(val), TimeUnit.MILLISECONDS);
        } catch (IllegalArgumentException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Bad value for " + param.getName() + ": "
                + e.getMessage());
        }
    }

    public static void setDurationVal(Map<String, String> props, DurationConfigParam param, long val, TimeUnit unit,
                                      boolean validateValue) {
        setVal(props, param, PropUtil.formatDuration(val, unit), validateValue);
    }

    public Map<String, String> getProps() {
        return props;
    }
}
