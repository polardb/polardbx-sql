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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.List;

public class FilterExecTest extends BaseExecTest {

    public FilterExecTest() {
    }

    @Test
    public void testFilter() {
        RowChunksBuilder rowChunksBuilder =
            RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
                .row(null, 1).row(2, 2).row(3, 3).row(4, 4);
        MockExec input = rowChunksBuilder.buildExec();

        context.setParams(new Parameters());

        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RexBuilder rexBuilder = new RexBuilder(factory);
        RexNode call = rexBuilder.makeCall(
            TddlOperatorTable.GREATER_THAN_OR_EQUAL,
            new RexInputRef(0, factory.createSqlType(SqlTypeName.INTEGER)),
            rexBuilder.makeLiteral(2, factory.createSqlType(SqlTypeName.INTEGER), false)
        );
        IExpression condition = RexUtils.buildRexNode(call, context);

        FilterExec filter = new FilterExec(input, condition, null, context);
        List<Chunk> expects = RowChunksBuilder.rowChunksBuilder(DataTypes.IntegerType, DataTypes.IntegerType)
            .row(2, 2).row(3, 3).row(4, 4).build();
        execForSmpMode(filter, expects, false);
    }
}
