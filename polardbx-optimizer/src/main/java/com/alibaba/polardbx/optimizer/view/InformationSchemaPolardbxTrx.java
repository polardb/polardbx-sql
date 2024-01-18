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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

/**
 * @author yaozhili
 */
public class InformationSchemaPolardbxTrx extends VirtualView {

    public static final List<ColumnDef> COLUMNS;

    static {
        int i = 0;
        COLUMNS = new ImmutableList.Builder<ColumnDef>()
            .add(new ColumnDef("TRX_ID", i++, SqlTypeName.VARCHAR))
            .add(new ColumnDef("SCHEMA", i++, SqlTypeName.VARCHAR))
            .add(new ColumnDef("PROCESS_ID", i++, SqlTypeName.VARCHAR))
            .add(new ColumnDef("TRX_TYPE", i++, SqlTypeName.VARCHAR))
            .add(new ColumnDef("START_TIME", i++, SqlTypeName.VARCHAR))
            .add(new ColumnDef("DURATION_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("ACTIVE_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("IDLE_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("WRITE_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("READ_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("WRITE_ROWS", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("READ_ROWS", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("CN_MDL_WAIT_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("GET_TSO_TIME", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("SQL_COUNT", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("RW_SQL_COUNT", i++, SqlTypeName.BIGINT))
            .add(new ColumnDef("CN_ADDRESS", i++, SqlTypeName.VARCHAR))
            .add(new ColumnDef("SQL", i++, SqlTypeName.VARCHAR))
            .build();
    }

    public InformationSchemaPolardbxTrx(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.POLARDBX_TRX);
    }

    public InformationSchemaPolardbxTrx(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        for (ColumnDef column : COLUMNS) {
            columns.add(new RelDataTypeFieldImpl(column.name, column.index, typeFactory.createSqlType(column.type)));
        }
        return typeFactory.createStructType(columns);
    }
}
