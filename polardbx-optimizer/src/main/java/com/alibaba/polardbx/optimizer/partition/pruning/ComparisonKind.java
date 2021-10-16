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

package com.alibaba.polardbx.optimizer.partition.pruning;

/**
 * @author chenghui.lch
 */
public enum ComparisonKind {

    GREATER_THAN(0, ">"),
    GREATER_THAN_OR_EQUAL(1, ">="),
    EQUAL(2, "="),
    LESS_THAN_OR_EQUAL(3, "<="),
    LESS_THAN(4, "<"),
    NOT_EQUAL(5, "!=");

    private int comparison = -1;
    private String comparisionSymbol = null;

    ComparisonKind(int comparison, String symbol) {
        this.comparison = comparison;
        this.comparisionSymbol = symbol;
    }

    int getComparison() {
        return this.comparison;
    }

    public static int getComparisonKindMaxVal() {
        return 6;
    }

    public String getComparisionSymbol() {
        return comparisionSymbol;
    }

    public boolean containEqual() {
        return this.comparison >= 1 && this.comparison <= 3;
    }
}
