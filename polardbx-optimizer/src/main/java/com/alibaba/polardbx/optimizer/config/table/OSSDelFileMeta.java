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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;

/**
 * File meta for table-file with suffix .del
 */
public class OSSDelFileMeta extends FileMeta {
    public OSSDelFileMeta(String logicalSchemaName, String logicalTableName, String physicalTableSchema,
                          String physicalTableName, String partitionName, String fileName,
                          long fileSize, long tableRows, Long commitTs, Long removeTs, Long schemaTs,
                          String createTime, String updateTime, Engine engine, Long fileHash) {
        super(logicalSchemaName, logicalTableName, physicalTableSchema, physicalTableName, partitionName, fileName,
            fileSize, tableRows, commitTs, removeTs, schemaTs, createTime, updateTime, engine, fileHash);
    }
}
