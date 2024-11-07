/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPartitionStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class CciMetaManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(CciMetaManager.class);

    private static CciMetaManager INSTANCE = new CciMetaManager();

    private final ColumnarTableEvolutionAccessor columnarTableEvolution = new ColumnarTableEvolutionAccessor();
    private final ColumnarPartitionEvolutionAccessor columnarPartitionEvolution =
        new ColumnarPartitionEvolutionAccessor();
    private final TablePartitionAccessor tablePartition = new TablePartitionAccessor();

    @Override
    protected void doInit() {
        super.doInit();
        logger.info("init CciMetaManager");

        if (!ConfigDataMode.isPolarDbX()) {
            return;
        }

        try {
            loadPartitions();
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "init cci meta manager failed");
        }
    }

    public static CciMetaManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    private void setConnection(Connection metaConn) {
        columnarTableEvolution.setConnection(metaConn);
        columnarPartitionEvolution.setConnection(metaConn);
        tablePartition.setConnection(metaConn);
    }

    public synchronized void loadPartitions() {
        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            setConnection(metaConn);

            List<ColumnarTableEvolutionRecord> evolutionRecords =
                getColumnarTableEvolution().queryPartitionEmptyRecords();
            for (ColumnarTableEvolutionRecord evolutionRecord : evolutionRecords) {
                String tableScHema = evolutionRecord.tableSchema;
                String columnarTableName = evolutionRecord.indexName;
                long versionId = evolutionRecord.versionId;
                long ddlJobId = evolutionRecord.ddlJobId;
                long tableId = evolutionRecord.tableId;

                final List<TablePartitionRecord> partitionRecords =
                    getTablePartition().getTablePartitionsByDbNameTbName(tableScHema, columnarTableName, false);

                // Skip if removed table's partition
                if (partitionRecords.isEmpty()) {
                    continue;
                }

                // Insert partition evolution records
                List<ColumnarPartitionEvolutionRecord> partitionEvolutionRecords =
                    new ArrayList<>(partitionRecords.size());
                for (TablePartitionRecord partitionRecord : partitionRecords) {
                    partitionEvolutionRecords.add(
                        new ColumnarPartitionEvolutionRecord(tableId, partitionRecord.partName,
                            versionId, ddlJobId, partitionRecord, ColumnarPartitionStatus.PUBLIC.getValue()));
                }
                getColumnarPartitionEvolution().insert(partitionEvolutionRecords);
                getColumnarPartitionEvolution().updatePartitionIdAsId(tableId, versionId);

                partitionEvolutionRecords =
                    getColumnarPartitionEvolution().queryTableIdVersionIdOrderById(tableId, versionId);
                final List<Long> partitions =
                    partitionEvolutionRecords.stream().map(r -> r.id).collect(Collectors.toList());

                getColumnarTableEvolution().updatePartition(tableId, partitions);
            }

        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

}
