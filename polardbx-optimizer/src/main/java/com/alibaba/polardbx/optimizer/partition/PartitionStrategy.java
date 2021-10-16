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

package com.alibaba.polardbx.optimizer.partition;

/**
 * @author chenghui.lch
 */
public enum PartitionStrategy {
    RANGE("RANGE", "RANGE"),
    LIST("LIST", "LIST"),
    HASH("HASH", "HASH"),
    KEY("KEY", "KEY"),
    RANGE_COLUMNS("RANGE_COLUMNS", "RANGE COLUMNS"),
    LIST_COLUMNS("LIST_COLUMNS", "LIST COLUMNS");

    private String strategyStr;
    private String strategyExplainName;
    PartitionStrategy(String strategyStr, String strategyExplainName) {
        this.strategyStr = strategyStr;
        this.strategyExplainName = strategyExplainName;
    }

    @Override
    public String toString() {
        return strategyStr;
    }

    public String getStrategyExplainName() {
        return strategyExplainName;
    }

    public boolean isSingleValue() {
        switch (this) {
        case RANGE:
        case HASH:
        case KEY:
        case RANGE_COLUMNS:
            return true;
        case LIST:
        case LIST_COLUMNS:
            return false;
        default:
            throw new RuntimeException("Unreachable");
        }
    }

    public boolean isMultiValue() {
        return !isSingleValue();
    }

    public boolean isHashed() {
        return this.equals(HASH) || this.equals(KEY);
    }

    public boolean isKey() {
        return this.equals(KEY);
    }

    public boolean isList() {
        return this.equals(LIST) || this.equals(LIST_COLUMNS);
    }

    public boolean isRange() {
        return this.equals(RANGE) || this.equals(RANGE_COLUMNS);
    }
}
