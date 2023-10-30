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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;


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
        // length==2 and args[1] is a list meaning it's a targetList args for IN expr.
        if (args.length == 2 && args[1] instanceof List) {
            args[1] = new RawString((List) args[1]);
        }
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

    @JsonIgnore
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
            } else if (args[i] instanceof RawString) {
                buffer.append(((RawString) args[i]).display());
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
