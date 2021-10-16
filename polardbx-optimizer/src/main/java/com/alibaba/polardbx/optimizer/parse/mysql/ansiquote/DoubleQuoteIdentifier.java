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

package com.alibaba.polardbx.optimizer.parse.mysql.ansiquote;

/**
 * 这个类主要用于标记sql中一段由双引号标记的identifier，需要将该identifier改为反引号的形式。
 *
 * @author arnkore 2017-08-30 15:32
 */
public class DoubleQuoteIdentifier {

    /**
     * 被替换后的字符串
     */
    private final byte[] substitution;

    /**
     * 双引号标记的字符串的起始位置（包括）
     */
    private final int startIndex;

    /**
     * 双引号标记的字符串的终止位置（不包括）
     */
    private final int endIndex;

    public DoubleQuoteIdentifier(byte[] substitution, int startIndex, int endIndex) {
        this.substitution = substitution;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public byte[] getSubstitution() {
        return substitution;
    }

    /**
     * 判断给定索引值是否在该identifier之前
     *
     * @param index
     * @return
     */
    public boolean indexBeforeScope(int index) {
        return index < startIndex;
    }

    /**
     *
     * 判断给定索引值是否在该identifier范围内
     * @param index
     * @return
     */
    public boolean indexInScope(int index) {
        return index >= startIndex && index < endIndex;
    }

    /**
     * 判断给定索引值是否在该identifier之后
     *
     * @param index
     * @return
     */
    public boolean indexAfterScope(int index) {
        return index >= endIndex;
    }
}
