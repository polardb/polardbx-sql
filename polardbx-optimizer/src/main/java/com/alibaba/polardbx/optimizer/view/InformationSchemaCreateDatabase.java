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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class InformationSchemaCreateDatabase extends VirtualView {
    public static final String DDL_JOB_ID = "DDL_JOB_ID";
    public static final String SOURCE_SCHEMA = "SOURCE_SCHEMA";
    public static final String TARGET_SCHEMA = "TARGET_SCHEMA";
    public static final String TABLE_SEQ = "TABLE/SEQ";
    public static final String STAGE = "STAGE";
    public static final String STATUS = "STATUS";
    public static final String DETAIL = "DETAIL";
    public static final String SQL_SRC = "SQL_SRC";
    public static final String SQL_DST = "SQL_DST";
    public static final String BACKFILL_START_TIME = "BACKFILL_START_TIME";
    public static final String CURRENT_SPEED = "CURRENT_SPEED(ROWS/SEC)";
    public static final String AVERAGE_SPEED = "AVERAGE_SPEED(ROWS/SEC)";
    public static final String FINISHED_ROWS = "FINISHED_ROWS";
    public static final String APPROXIMATE_TOTAL_ROWS = "APPROXIMATE_TOTAL_ROWS";
    public static final String BACKFILL_PROGRESS = "BACKFILL_PROGRESS";

    protected InformationSchemaCreateDatabase(RelOptCluster cluster,
                                              RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.CREATE_DATABASE);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int i = 0;
        columns.add(new RelDataTypeFieldImpl(DDL_JOB_ID, i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(SOURCE_SCHEMA, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(TARGET_SCHEMA, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(TABLE_SEQ, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(STAGE, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(STATUS, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(DETAIL, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(SQL_SRC, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(SQL_DST, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(BACKFILL_START_TIME, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(CURRENT_SPEED, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(AVERAGE_SPEED, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(FINISHED_ROWS, i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(
            new RelDataTypeFieldImpl(APPROXIMATE_TOTAL_ROWS, i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(BACKFILL_PROGRESS, i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        return typeFactory.createStructType(columns);
    }

}
