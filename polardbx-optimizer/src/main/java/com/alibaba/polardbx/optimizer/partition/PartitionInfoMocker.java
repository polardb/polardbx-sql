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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionInfoMocker {


    public static PartitionInfo buildPartitionInfoByTableName(String dbName,
                                                              String tbName,
                                                              String shardKey,
                                                              SqlCreateTable createTable) {

        PartitionInfo partitionInfo = new PartitionInfo();

        /**
         * <pre>
         *     PARTITION BY RANGE(col) (
                PARTITION p1 VALUES LESS THAN (1000),
                PARTITION p2 VALUES LESS THAN (2000),
                PARTITION p3 VALUES LESS THAN (2000),
                PARTITION p4 VALUES LESS THAN (2000),
               )
         * </pre>
         */
        partitionInfo.tableSchema = dbName;
        partitionInfo.tableName = tbName;
        partitionInfo.spTemplateFlag = TablePartitionRecord.SUBPARTITION_TEMPLATE_NOT_EXISTED;
        partitionInfo.tableGroupId = -1L;
        partitionInfo.metaVersion = 1L;

        partitionInfo.partitionBy = new PartitionByDefinition();

        TableMeta tableMeta = TableMetaParser.parse(tbName, createTable);
        RelDataType shardKeyRelDataType = null;
        for (int i = 0; i < tableMeta.getAllColumns().size(); i++) {
            ColumnMeta cm = tableMeta.getAllColumns().get(i);
            if (!cm.getName().equalsIgnoreCase(shardKey)) {
                break;
            }
            shardKeyRelDataType = cm.getField().getRelType();
            break;
        }

        SqlIdentifier shardExpr = new SqlIdentifier(shardKey, SqlParserPos.ZERO);
        partitionInfo.partitionBy.getPartitionExprList().add(shardExpr);
        partitionInfo.subPartitionBy = null;
        partitionInfo.partitionBy.strategy = PartitionStrategy.RANGE;

        final Field shardKeyField = new Field(shardKeyRelDataType);
        ColumnMeta shardKeyColumnMeta = new ColumnMeta(tbName, shardKey, null, shardKeyField);
        partitionInfo.partitionBy.partitionFieldList = new ArrayList<>();
        partitionInfo.partitionBy.partitionFieldList.add(shardKeyColumnMeta);
        partitionInfo.partitionBy.partitionColumnNameList = new ArrayList<>();
        partitionInfo.partitionBy.partitionColumnNameList.add(shardKey);

        List<Long> boundValList = new ArrayList<>();
        boundValList.add(1000L);
        boundValList.add(2000L);
        boundValList.add(3000L);
        boundValList.add(4000L);

        int grpCnt = 0;
        DbGroupInfoAccessor dbGroupInfoAccessor;
        try(Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);
            List<DbGroupInfoRecord>  grpList = dbGroupInfoAccessor.queryDbGroupByDbName(dbName);
            grpCnt = grpList.size();
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

        TableGroupLocation.GroupAllocator groupAllocator = TableGroupLocation.buildGroupAllocator(dbName);
        for (int i = 0; i < boundValList.size(); i++) {
            PartitionSpec p = new PartitionSpec();
            RangeBoundSpec pBoundSpec = new RangeBoundSpec();
            pBoundSpec.strategy = PartitionStrategy.RANGE;
            Long shardKeyObj = boundValList.get(i);
            PartitionField partFld = PartitionFieldBuilder.createField(DataTypes.LongType);
            partFld.store(shardKeyObj, DataTypes.LongType);
            PartitionBoundVal pBoundVal = PartitionBoundVal.createPartitionBoundVal(partFld, PartitionBoundValueKind.DATUM_NORMAL_VALUE);

            pBoundSpec.setBoundValue(pBoundVal);
            p.position = Long.valueOf(i + 1);
            p.isMaxValueRange = false;
            p.boundSpec = pBoundSpec;
            p.strategy = pBoundSpec.strategy;
            p.name = String.format("p%s", i + 1);
            p.engine = TablePartitionRecord.PARTITION_ENGINE_INNODB;

            PartitionLocation location = new PartitionLocation();
            location.setGroupKey(groupAllocator.allocate());
            p.setLocation(location);
            partitionInfo.partitionBy.partitions.add(p);
            //partitionInfo.setPartSpecSearcher(PartSpecSearcher.buildPartSpecSearcher(partitionInfo.getTableType(),partitionInfo.getPartitionBy()));
            partitionInfo.initPartSpecSearcher();
        }
        return partitionInfo;
    }

}
