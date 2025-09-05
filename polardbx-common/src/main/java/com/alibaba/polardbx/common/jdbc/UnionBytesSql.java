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

import java.util.Collections;
import java.util.List;

/**
 * Try solve the union sql requirement
 *
 * @author jilong.ljl
 */
public class UnionBytesSql extends BytesSql {
    public final static byte[] UNION_HEAD = "SELECT * FROM (".getBytes();
    public final static byte[] UNION_KW = "\nUNION ALL\n".getBytes();
    public final static byte[] PARENTHESES_FRONT = " ( ".getBytes();
    public final static byte[] PARENTHESES_END = " ) ".getBytes();
    public final static byte[] LIMIT_KW = " LIMIT ".getBytes();
    public final static byte[] ORDERBY_KW = " ORDER BY ".getBytes();
    public final static byte[] UNION_ALIAS = " __DRDS_ALIAS_T_ ".getBytes();

    @JsonProperty
    private int unionSize;

    @JsonProperty
    private byte[] order;

    @JsonProperty
    private byte[] limit;
    
    private byte[] unionHead;

    @JsonCreator
    public UnionBytesSql(@JsonProperty("bytesArray") byte[][] bytesArray,
                         @JsonProperty("parameterLast") boolean parameterLast,
                         @JsonProperty("unionSize") int unionSize,
                         @JsonProperty("order") byte[] order,
                         @JsonProperty("limit") byte[] limit) {
        super(bytesArray, parameterLast);
        this.unionSize = unionSize;
        this.order = order;
        this.limit = limit;
        this.unionHead = UNION_HEAD;
    }

    public UnionBytesSql(byte[][] bytesArray, boolean parameterLast, int unionSize, byte[] order, byte[] limit,
                         byte[] unionHead) {
        super(bytesArray, parameterLast);
        this.unionSize = unionSize;
        this.order = order;
        this.limit = limit;
        this.unionHead = unionHead;
    }

    /**
     * return parameter size of union BytesSql
     *
     * @return parameter size
     */
    @Override
    public int dynamicSize() {
        return unionSize * super.dynamicSize();
    }

    /**
     * @return union byte[] for a UionBytesSql
     */
    @Override
    public byte[] getBytes() {
        if (unionSize == 1) {
            return super.getBytes();
        }
        byte[] originBytes = super.getBytes();
        int length =
            unionSize * originBytes.length + (unionSize - 1) * UNION_KW.length + unionSize * PARENTHESES_FRONT.length
                + unionSize * PARENTHESES_END.length;
        if (order != null) {
            length +=
                unionHead.length + PARENTHESES_END.length + UNION_ALIAS.length + ORDERBY_KW.length + order.length;
        }
        if (limit != null) {
            length += LIMIT_KW.length + limit.length;
        }
        byte[] rs = new byte[length];
        int currentLenght = 0;
        if (order != null) {
            currentLenght = arrayCopy(unionHead, rs, currentLenght);
        }
        currentLenght = arrayCopy(PARENTHESES_FRONT, rs, currentLenght);
        currentLenght = arrayCopy(originBytes, rs, currentLenght);
        currentLenght = arrayCopy(PARENTHESES_END, rs, currentLenght);

        for (int i = 1; i < unionSize; i++) {
            currentLenght = arrayCopy(UNION_KW, rs, currentLenght);
            currentLenght = arrayCopy(PARENTHESES_FRONT, rs, currentLenght);
            currentLenght = arrayCopy(originBytes, rs, currentLenght);
            currentLenght = arrayCopy(PARENTHESES_END, rs, currentLenght);
        }
        if (order != null) {
            currentLenght = arrayCopy(PARENTHESES_END, rs, currentLenght);
            currentLenght = arrayCopy(UNION_ALIAS, rs, currentLenght);
            currentLenght = arrayCopy(ORDERBY_KW, rs, currentLenght);
            currentLenght = arrayCopy(order, rs, currentLenght);
        }

        if (limit != null) {
            currentLenght = arrayCopy(LIMIT_KW, rs, currentLenght);
            arrayCopy(limit, rs, currentLenght);
        }
        return rs;
    }

    @Override
    public byte[] getBytes(List<ParameterContext> parameterContexts) {
        if (unionSize == 1) {
            return super.getBytes(parameterContexts);
        }
        if (!containRawString(parameterContexts)) {
            return getBytes();
        }
        byte[][] originBytesArray = new byte[unionSize][];
        int originLength = 0;
        for (int i = 0; i < originBytesArray.length; i++) {
            if (parameterContexts == null) {
                originBytesArray[i] = super.getBytes(Collections.emptyList());
            } else {
                originBytesArray[i] =
                    super.getBytes(
                        parameterContexts.subList(i * super.dynamicSize(),
                            i * super.dynamicSize() + super.dynamicSize()));
            }
            originLength += originBytesArray[i].length;
        }
        int length = originLength + (unionSize - 1) * UNION_KW.length + unionSize * PARENTHESES_FRONT.length
            + unionSize * PARENTHESES_END.length;
        if (order != null) {
            length +=
                unionHead.length + PARENTHESES_END.length + UNION_ALIAS.length + ORDERBY_KW.length + order.length;
        }
        if (limit != null) {
            length += LIMIT_KW.length + limit.length;
        }
        byte[] rs = new byte[length];
        int currentLenght = 0;
        if (order != null) {
            currentLenght = arrayCopy(unionHead, rs, currentLenght);
        }
        currentLenght = arrayCopy(PARENTHESES_FRONT, rs, currentLenght);
        currentLenght = arrayCopy(originBytesArray[0], rs, currentLenght);
        currentLenght = arrayCopy(PARENTHESES_END, rs, currentLenght);

        for (int i = 1; i < unionSize; i++) {
            currentLenght = arrayCopy(UNION_KW, rs, currentLenght);
            currentLenght = arrayCopy(PARENTHESES_FRONT, rs, currentLenght);
            currentLenght = arrayCopy(originBytesArray[i], rs, currentLenght);
            currentLenght = arrayCopy(PARENTHESES_END, rs, currentLenght);
        }
        if (order != null) {
            currentLenght = arrayCopy(PARENTHESES_END, rs, currentLenght);
            currentLenght = arrayCopy(UNION_ALIAS, rs, currentLenght);
            currentLenght = arrayCopy(ORDERBY_KW, rs, currentLenght);
            currentLenght = arrayCopy(order, rs, currentLenght);
        }

        if (limit != null) {
            currentLenght = arrayCopy(LIMIT_KW, rs, currentLenght);
            arrayCopy(limit, rs, currentLenght);
        }
        return rs;
    }

    /**
     * copy array and return the length
     *
     * @param src source byte array
     * @param des target byte array
     * @param currentLenght the start index for copy
     * @return end index of this copy
     */
    private int arrayCopy(byte[] src, byte[] des, int currentLenght) {
        System.arraycopy(src, 0, des, currentLenght, src.length);
        return currentLenght + src.length;
    }
}
