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

package com.alibaba.polardbx.executor.archive.schemaevolution;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;

import java.sql.Timestamp;

public class ColumnMetaWithTs {

    Timestamp create;

    ColumnMeta meta;

    public ColumnMetaWithTs(Timestamp create, ColumnMeta meta) {
        this.create = create;
        this.meta = meta;
    }

    public Timestamp getCreate() {
        return create;
    }

    public ColumnMeta getMeta() {
        return meta;
    }
}
