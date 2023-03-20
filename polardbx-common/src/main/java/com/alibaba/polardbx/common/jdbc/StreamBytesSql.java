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
import org.bouncycastle.util.Arrays;

import java.util.Collections;
import java.util.List;

/**
 * Try solve the stream sql requirement
 *
 * @author jilong.ljl
 */
public class StreamBytesSql extends UnionBytesSql {

    @JsonProperty
    private byte[] streamLimit;

    @JsonProperty
    private boolean isContainSelect;

    @JsonCreator
    public StreamBytesSql(@JsonProperty("bytesArray") byte[][] bytesArray,
                          @JsonProperty("parameterLast") boolean parameterLast,
                          @JsonProperty("unionSize") int unionSize,
                          @JsonProperty("order") byte[] order,
                          @JsonProperty("limit") byte[] limit,
                          @JsonProperty("streamLimit") byte[] streamLimit,
                          @JsonProperty("isContainSelect") boolean isContainSelect) {
        super(bytesArray, parameterLast, unionSize, order, limit);
        this.streamLimit = streamLimit;
        this.isContainSelect = isContainSelect;
    }

    /**
     * @return union byte[] for a UionBytesSql
     */
    @Override
    public byte[] getBytes() {
        byte[] temp = super.getBytes();
        if (isContainSelect) {
            return Arrays.concatenate(UNION_HEAD, temp, PARENTHESES_END,
                Arrays.concatenate(UNION_ALIAS, LIMIT_KW, streamLimit));
        } else {
            return Arrays.concatenate(temp, LIMIT_KW, streamLimit);
        }
    }

    @Override
    public byte[] getBytes(List<ParameterContext> parameterContexts) {
        if (!containRawString(parameterContexts)) {
            return getBytes();
        }
        byte[] temp = super.getBytes(parameterContexts);
        if (isContainSelect) {
            return Arrays.concatenate(UNION_HEAD, temp, PARENTHESES_END,
                Arrays.concatenate(UNION_ALIAS, LIMIT_KW, streamLimit));
        } else {
            return Arrays.concatenate(temp, LIMIT_KW, streamLimit);
        }
    }
}
