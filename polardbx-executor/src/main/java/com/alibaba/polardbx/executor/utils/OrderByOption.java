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

package com.alibaba.polardbx.executor.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.rel.RelFieldCollation;

public class OrderByOption {

    public final int index;
    public final boolean asc;     // 默认是asc
    public final boolean nullLast;

    public OrderByOption(
        int index,
        RelFieldCollation.Direction direct,
        RelFieldCollation.NullDirection nullDirect) {
        super();
        this.index = index;
        this.asc = direct == RelFieldCollation.Direction.ASCENDING;
        this.nullLast = nullDirect == RelFieldCollation.NullDirection.LAST;
    }

    @JsonCreator
    public OrderByOption(
        @JsonProperty("index") int index,
        @JsonProperty("asc") boolean asc,
        @JsonProperty("nullLast") boolean nullLast) {
        super();
        this.index = index;
        this.asc = asc;
        this.nullLast = nullLast;
    }

    @JsonProperty
    public boolean isAsc() {
        return asc;
    }

    @JsonProperty
    public boolean isNullLast() {
        return nullLast;
    }

    @JsonProperty
    public int getIndex() {
        return index;
    }

    public RelFieldCollation createRelFieldCollation() {
        RelFieldCollation.Direction direction =
            asc ? RelFieldCollation.Direction.ASCENDING : RelFieldCollation.Direction.DESCENDING;
        RelFieldCollation.NullDirection nullDirect =
            nullLast ? RelFieldCollation.NullDirection.LAST : RelFieldCollation.NullDirection.FIRST;
        return new RelFieldCollation(index, direction, nullDirect);
    }
}
