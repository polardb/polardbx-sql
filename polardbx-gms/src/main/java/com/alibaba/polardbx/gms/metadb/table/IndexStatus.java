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

import com.alibaba.polardbx.common.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

public enum IndexStatus {
    /**
     * create phy table
     */
    CREATING(0),
    DELETE_ONLY(1),
    WRITE_ONLY(2),
    /**
     * backfill data
     */
    WRITE_REORG(3),
    PUBLIC(4),
    /**
     * remove data
     */
    DROP_WRITE_ONLY(5), // Same to WRITE_ONLY.
    /**
     * remove phy table
     */
    DROP_DELETE_ONLY(6), // Same to DELETE_ONLY.
    /**
     * remove meta data
     */
    ABSENT(7);

    private final int value;

    IndexStatus(int value) {
        this.value = value;
    }

    public static IndexStatus from(int value) {
        switch (value) {
        case 0:
            return CREATING;
        case 1:
            return DELETE_ONLY;
        case 2:
            return WRITE_ONLY;
        case 3:
            return WRITE_REORG;
        case 4:
            return PUBLIC;
        case 5:
            return DROP_WRITE_ONLY;
        case 6:
            return DROP_DELETE_ONLY;
        case 7:
            return ABSENT;
        default:
            return null;
        }
    }

    public int getValue() {
        return value;
    }

    public static final EnumSet<IndexStatus> ALL = EnumSet.allOf(IndexStatus.class);

    public static final EnumSet<IndexStatus> DELETABLE =
        EnumSet.of(DELETE_ONLY, WRITE_ONLY, WRITE_REORG, PUBLIC, DROP_WRITE_ONLY, DROP_DELETE_ONLY);

    public static final EnumSet<IndexStatus> WRITABLE = EnumSet.of(WRITE_ONLY, WRITE_REORG, PUBLIC, DROP_WRITE_ONLY);

    public static final EnumSet<IndexStatus> READABLE = EnumSet.of(PUBLIC);

    public static final EnumSet<IndexStatus> INDEX_TABLE_CREATED = EnumSet
        .of(DELETE_ONLY, WRITE_ONLY, WRITE_REORG, PUBLIC, DROP_WRITE_ONLY, DROP_DELETE_ONLY);

    public boolean isDeletable() {
        return DELETABLE.contains(this);
    }

    public boolean isWritable() {
        return WRITABLE.contains(this);
    }

    public boolean isBackfillStatus() {
        return WRITE_ONLY == this;
    }

    public boolean isIndexTableCreated() {
        return INDEX_TABLE_CREATED.contains(this);
    }

    public boolean isPublished() {
        return PUBLIC == this;
    }

    public boolean belongsTo(EnumSet<IndexStatus> statuses) {
        return statuses.contains(this);
    }

    public static List<Pair<IndexStatus, IndexStatus>> addColumnStatusChange() {
        return enumerateStatusChange(addColumnStatusList());
    }

    public static List<Pair<IndexStatus, IndexStatus>> addGsiStatusChange() {
        return enumerateStatusChange(addGsiStatusList());
    }

    public static List<Pair<IndexStatus, IndexStatus>> dropGsiStatusChange() {
        return enumerateStatusChange(dropGsiStatusList());
    }

    private static List<Pair<IndexStatus, IndexStatus>> enumerateStatusChange(List<IndexStatus> statuses) {
        List<Pair<IndexStatus, IndexStatus>> result = new ArrayList<>();
        for (int i = 1; i < statuses.size(); i++) {
            result.add(Pair.of(statuses.get(i - 1), statuses.get(i)));
        }
        return result;
    }

    private static List<IndexStatus> addColumnStatusList() {
        return Arrays.asList(ABSENT, DELETE_ONLY, WRITE_ONLY, WRITE_REORG, PUBLIC);
    }

    private static List<IndexStatus> addGsiStatusList() {
        return Arrays.asList(CREATING, DELETE_ONLY, WRITE_ONLY, WRITE_REORG, PUBLIC);
    }

    private static List<IndexStatus> dropGsiStatusList() {
        return Arrays.asList(PUBLIC, WRITE_ONLY, DELETE_ONLY, ABSENT);
    }

}
