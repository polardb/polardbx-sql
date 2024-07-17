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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.parse.util.Pair;
import com.google.common.collect.Lists;
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
 * @author fangwu
 */
public class InformationSchemaStatisticsData extends VirtualView {

    public InformationSchemaStatisticsData(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.STATISTICS_DATA);
    }

    public InformationSchemaStatisticsData(RelInput relInput) {
        super(relInput);
    }

    public static List<Pair<String, SqlTypeName>> meta = Lists.newArrayList();

    static {
        meta.add(new Pair("HOST", SqlTypeName.VARCHAR));
        meta.add(new Pair("SCHEMA_NAME", SqlTypeName.VARCHAR));
        meta.add(new Pair("TABLE_NAME", SqlTypeName.VARCHAR));
        meta.add(new Pair("COLUMN_NAME", SqlTypeName.VARCHAR));
        meta.add(new Pair("TABLE_ROWS", SqlTypeName.BIGINT));
        meta.add(new Pair("NDV", SqlTypeName.BIGINT));
        meta.add(new Pair("NDV_SOURCE", SqlTypeName.VARCHAR));
        meta.add(new Pair("TOPN", SqlTypeName.VARCHAR));
        meta.add(new Pair("HISTOGRAM", SqlTypeName.VARCHAR));
        meta.add(new Pair("SAMPLE_RATE", SqlTypeName.VARCHAR));
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        int index = 0;
        for (Pair<String, SqlTypeName> pair : meta) {
            columns.add(new RelDataTypeFieldImpl(pair.getKey(), index++, typeFactory.createSqlType(pair.getValue())));
        }
        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // TABLE_SCHEMA && TABLE_NAME
        return i == getTableSchemaIndex() || i == getTableNameIndex();
    }

    public int getTableSchemaIndex() {
        return 1;
    }

    public int getTableNameIndex() {
        return 2;
    }

    public static DataType transform(SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
        case VARCHAR:
            return DataTypes.StringType;
        case BIGINT:
            return DataTypes.LongType;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "not supported data type");
        }
    }
}
