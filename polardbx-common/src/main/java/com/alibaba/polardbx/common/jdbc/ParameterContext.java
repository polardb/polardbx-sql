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

package com.alibaba.polardbx.common.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.Serializable;


public class ParameterContext implements Serializable {

    public static long INSTANCE_MEM_SIZE = 16;

    private ParameterMethod parameterMethod;

    private Object[] args;

    public ParameterContext() {
    }

    @JsonCreator
    public ParameterContext(@JsonProperty("parameterMethod") ParameterMethod parameterMethod,
                            @JsonProperty("args") Object[] args) {
        this.parameterMethod = parameterMethod;
        this.args = args;
    }

    @JsonProperty
    public ParameterMethod getParameterMethod() {
        return parameterMethod;
    }

    public void setParameterMethod(ParameterMethod parameterMethod) {
        this.parameterMethod = parameterMethod;
    }

    @JsonProperty
    public Object[] getArgs() {
        return args;
    }

    public Object getValue() {
        return args[1];
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public void setValue(Object value) {
        this.args[1] = value;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        for (int i = 1; i < args.length; ++i) {
            if (args[i] instanceof byte[]) {
                buffer.append(GeneralUtil.printBytes((byte[]) args[i]));
            } else {
                buffer.append(args[i]);
            }
            if (i != args.length - 1) {
                buffer.append(", ");
            }
        }

        return buffer.toString();
    }
}
