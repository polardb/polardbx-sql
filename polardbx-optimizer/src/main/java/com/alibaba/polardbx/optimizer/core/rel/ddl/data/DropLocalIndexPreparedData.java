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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;

@EqualsAndHashCode(callSuper = true)
@Data
public class DropLocalIndexPreparedData extends DdlPreparedData {

    private String indexName;
    private boolean onClustered;
    private boolean onGsi;

    /**
     * Can equals to an alter table drop index
     */
    public boolean canEquals(AlterTablePreparedData alterTable) {
        return getSchemaName().equals(alterTable.getSchemaName()) &&
            getTableName().equals(alterTable.getTableName()) &&
            (onClustered ||
                (CollectionUtils.isNotEmpty(alterTable.getDroppedIndexes()) &&
                    alterTable.getDroppedIndexes().get(0).equals(this.indexName)));

    }
}
