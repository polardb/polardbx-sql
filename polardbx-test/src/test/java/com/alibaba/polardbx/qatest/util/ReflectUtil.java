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

package com.alibaba.polardbx.qatest.util;

import com.alibaba.polardbx.qatest.data.ColumnDataGenerateRule;
import com.alibaba.polardbx.qatest.data.ColumnDataRandomGenerateRule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;

/**
 * DataGenerateRule的反射类
 */
public class ReflectUtil {
    private final Log log = LogFactory.getLog(ReflectUtil.class);

    private ColumnDataGenerateRule c = null;
    private ColumnDataRandomGenerateRule d = null;
    private String[] parameters;

    public ReflectUtil(ColumnDataGenerateRule c, ColumnDataRandomGenerateRule d) {
        this.c = c;
        this.d = d;
    }

    public Object getResult(String methodStr) {
        Method method = getMethod(methodStr);
        return invoke(method);
    }

    public Object getResultRD(String methodStr) {
        Method method = getMethodRD(methodStr);
        return invokeRD(method);
    }

    private Method getMethod(String methodStr) {
        String methodName = methodStr.substring(0, methodStr.indexOf("("))
            .trim();
        String methodParams = methodStr.substring(methodStr.indexOf("(") + 1,
            methodStr.indexOf(")"));

        if (methodParams != null && !methodParams.trim().equals("")) {
            String[] params = methodParams.split(",");
            for (int index = 0; index < params.length; index++) {
                params[index] = params[index].trim();
            }

            parameters = params;
            return getMethod(methodName, params);
        } else {
            parameters = null;
            return getMethod(methodName, null);
        }
    }

    private Method getMethodRD(String methodStr) {
        String methodName = methodStr.substring(0, methodStr.indexOf("("))
            .trim();
        String methodParams = methodStr.substring(methodStr.indexOf("(") + 1,
            methodStr.indexOf(")"));

        if (methodParams != null && !methodParams.trim().equals("")) {
            String[] params = methodParams.split(",");
            for (int index = 0; index < params.length; index++) {
                params[index] = params[index].trim();
            }

            parameters = params;
            return getMethodRD(methodName, params);
        } else {
            parameters = null;
            return getMethodRD(methodName, null);
        }
    }

    private Method getMethod(String methodName, String[] params) {
        try {

            if (methodName == null || methodName.equals("")) {
                throw new RuntimeException(
                    "Can not find method name from sql params!");
            }

            if (params != null) {
                Class<?>[] paramsClasses = new Class[params.length];
                for (int index = 0; index < params.length; index++) {
                    paramsClasses[index] = String.class;
                }

                Method method =
                    ColumnDataGenerateRule.class.getMethod(methodName, paramsClasses);
                return method;
            } else {
                Method method = ColumnDataGenerateRule.class.getMethod(methodName);
                return method;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private Method getMethodRD(String methodName, String[] params) {
        try {

            if (methodName == null || methodName.equals("")) {
                throw new RuntimeException(
                    "Can not find method name from sql params!");
            }

            if (params != null) {
                Class<?>[] paramsClasses = new Class[params.length];
                for (int index = 0; index < params.length; index++) {
                    paramsClasses[index] = String.class;
                }

                Method method = ColumnDataRandomGenerateRule.class.getMethod(methodName, paramsClasses);
                return method;
            } else {
                Method method = ColumnDataRandomGenerateRule.class.getMethod(methodName);
                return method;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private Object invoke(Method method) {
        try {
            return method.invoke(c, parameters);
        } catch (Throwable t) {
            if (method != null) {
                log.error("method: " + method.getName());
            }
            if (parameters != null) {
                for (String param : parameters) {
                    log.error("param: " + param);
                }
            }
            throw new RuntimeException(t);
        }
    }

    private Object invokeRD(Method method) {
        try {
            return method.invoke(d, parameters);
        } catch (Throwable t) {
            if (method != null) {
                log.error("method: " + method.getName());
            }
            if (parameters != null) {
                for (String param : parameters) {
                    log.error("param: " + param);
                }
            }
            throw new RuntimeException(t);
        }
    }

    public void setPk(long pk) {
        c.setPk(pk);
    }

    public void resetPk() {
        c.resetPk();
    }

    public String timestampStartValue() {
        return c.timestampStartValue();
    }

    public String timestampEndValue() {
        return c.timestampEndValue();
    }

    public String date_testStartValue() {
        return c.date_testStartValue();
    }

    public String date_testEndValue() {
        return c.date_testEndValue();
    }

    public void clearTestValues() {
        c.clearInteger_testValue();
    }

    public void setPkRD(long pk) {
        d.setPk(pk);
    }

    public void resetPkRD() {
        d.resetPk();
    }

    public void clearTestValuesRD() {
        d.clearInteger_testValue();
    }
}
