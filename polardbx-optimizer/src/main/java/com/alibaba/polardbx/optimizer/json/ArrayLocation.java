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

package com.alibaba.polardbx.optimizer.json;

/**
 * arrayLocation:
 * leftBracket ( nonNegativeInteger | asterisk ) rightBracket
 *
 * @author arnkore 2017-07-12 17:14
 */
public class ArrayLocation extends AbstractPathLeg {
    private final boolean isAsterisk;

    private Integer arrayIndex;

    public ArrayLocation(Integer arrayIndex) {
        this.isAsterisk = (arrayIndex == null);
        this.arrayIndex = arrayIndex;
    }

    @Override
    public String toString() {
        StringBuilder appendable = new StringBuilder();
        appendable.append("[");

        if (isAsterisk) {
            appendable.append("*");
        } else {
            appendable.append(arrayIndex);
        }

        appendable.append("]");

        return appendable.toString();
    }

    @Override
    public boolean isAsterisk() {
        return isAsterisk;
    }

    public Integer getArrayIndex() {
        return arrayIndex;
    }

    public void setArrayIndex(Integer arrayIndex) {
        this.arrayIndex = arrayIndex;
    }
}
