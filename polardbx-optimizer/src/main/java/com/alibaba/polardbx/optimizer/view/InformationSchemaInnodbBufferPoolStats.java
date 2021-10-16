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

package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

public class InformationSchemaInnodbBufferPoolStats extends VirtualView {

    public InformationSchemaInnodbBufferPoolStats(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.INNODB_BUFFER_POOL_STATS);
    }

    public InformationSchemaInnodbBufferPoolStats(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("POOL_ID", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("POOL_SIZE", 1, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("FREE_BUFFERS", 2, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("DATABASE_PAGES", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("OLD_DATABASE_PAGES", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("MODIFIED_DATABASE_PAGES", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PENDING_DECOMPRESS", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PENDING_READS", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PENDING_FLUSH_LRU", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PENDING_FLUSH_LIST", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PAGES_MADE_YOUNG", 10, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PAGES_NOT_MADE_YOUNG", 11,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("PAGES_MADE_YOUNG_RATE", 12, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("PAGES_MADE_NOT_YOUNG_RATE", 13, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("NUMBER_PAGES_READ", 14, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("NUMBER_PAGES_CREATED", 15,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("NUMBER_PAGES_WRITTEN", 16,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("PAGES_READ_RATE", 17, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PAGES_CREATE_RATE", 18, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PAGES_WRITTEN_RATE", 19, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("NUMBER_PAGES_GET", 20, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("HIT_RATE", 21, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("YOUNG_MAKE_PER_THOUSAND_GETS", 22,
                typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("NOT_YOUNG_MAKE_PER_THOUSAND_GETS", 23,
            typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("NUMBER_PAGES_READ_AHEAD", 24,
                typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl("NUMBER_READ_AHEAD_EVICTED", 25, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("READ_AHEAD_RATE", 26, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("READ_AHEAD_EVICTED_RATE", 27, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LRU_IO_TOTAL", 28, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("LRU_IO_CURRENT", 29, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("UNCOMPRESS_TOTAL", 30, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("UNCOMPRESS_CURRENT", 31, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }
}
