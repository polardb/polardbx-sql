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

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;

@Data
@NoArgsConstructor
public class DdlPreparedData {

    private String schemaName;
    private String tableName;
    private boolean withHint;
    private boolean needRenamePhyTable;

    /**
     * the table's version when prepare data for ddl task
     */
    protected Long tableVersion = -1L;
    /**
     * Every ddl has its own version id
     */
    protected Long ddlVersionId = DEFAULT_DDL_VERSION_ID;

    public DdlPreparedData(final String schemaName, final String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public synchronized void setDdlVersionId(@NotNull Long ddlVersionId) {
        Preconditions.checkArgument(Objects.equals(this.ddlVersionId, DEFAULT_DDL_VERSION_ID),
            "Do not support update ddl version id");
        this.ddlVersionId = ddlVersionId;
    }
}
