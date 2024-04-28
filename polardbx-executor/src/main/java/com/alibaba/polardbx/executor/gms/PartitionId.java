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

package com.alibaba.polardbx.executor.gms;

import lombok.Data;

/**
 * The unique identifier of a partition.
 */
@Data
public class PartitionId {
    private final String partName;
    private final String logicalSchema;
    private final Long tableId;

    public PartitionId(String partName, String logicalSchema, Long tableId) {
        this.partName = partName;
        this.logicalSchema = logicalSchema;
        this.tableId = tableId;
    }

    public static PartitionId of(String partName, String logicalSchema, Long tableId) {
        return new PartitionId(partName, logicalSchema, tableId);
    }
}
