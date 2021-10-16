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

package com.alibaba.polardbx.gms.metadb.table;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents status of table schema change and column schema change
 */
public enum TableStatus {

    ABSENT(0),
    PUBLIC(1),
    WRITE_ONLY(2),
    WRITE_REORG(3);

    private final int value;

    TableStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static TableStatus convert(int value) {
        switch (value) {
        case 0:
            return ABSENT;
        case 1:
            return PUBLIC;
        case 2:
            return WRITE_ONLY;
        case 3:
            return WRITE_REORG;
        default:
            return null;
        }
    }

    /**
     * Schema change for add a column
     */
    public static List<Pair<TableStatus, TableStatus>> schemaChangeForAddColumn() {
        return enumerateStatusChange(addColumnStatusList());
    }

    /**
     * Schema change for drop a column
     */
    public static List<Pair<TableStatus, TableStatus>> schemaChangeForDropColumn() {
        return enumerateStatusChange(Lists.reverse(addColumnStatusList()));
    }

    private static List<TableStatus> addColumnStatusList() {
        return Arrays.asList(ABSENT, WRITE_ONLY, WRITE_REORG, PUBLIC);
    }

    private static List<Pair<TableStatus, TableStatus>> enumerateStatusChange(List<TableStatus> statuses) {
        assert statuses.size() > 1;
        List<Pair<TableStatus, TableStatus>> result = new ArrayList<>();
        for (int i = 1; i < statuses.size(); i++) {
            result.add(Pair.of(statuses.get(i - 1), statuses.get(i)));
        }
        return result;
    }
}
