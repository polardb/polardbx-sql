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

package com.alibaba.polardbx.util.bloomfilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;

public class HashMethodInfo {
    private static final HashMethodInfo DEFAULT_METHOD = new HashMethodInfo(Murmur3_128Method.METHOD_NAME);

    private final String methodName;
    private final Object[] args;

    @JsonCreator
    public HashMethodInfo(
        @JsonProperty("methodName") String methodName,
        @JsonProperty("args") Object... args) {
        this.methodName = methodName;
        this.args = args;
    }

    @JsonProperty
    public String getMethodName() {
        return methodName;
    }

    @JsonProperty
    public Object[] getArgs() {
        return args;
    }

    public static HashMethodInfo defaultHashMethod() {
        return DEFAULT_METHOD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HashMethodInfo that = (HashMethodInfo) o;
        return Objects.equals(getMethodName(), that.getMethodName()) &&
            Arrays.equals(getArgs(), that.getArgs());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(getMethodName());
        result = 31 * result + Arrays.hashCode(getArgs());
        return result;
    }
}
