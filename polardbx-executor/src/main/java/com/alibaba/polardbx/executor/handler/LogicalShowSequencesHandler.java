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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptNewAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequencesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowSequences;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;

public class LogicalShowSequencesHandler extends HandlerCommon {

    public LogicalShowSequencesHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        String whereClause = buildWhereClause(logicalPlan);

        ArrayResultCursor resultCursor = buildResultCursor();

        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            sequencesAccessor.setConnection(metaDbConn);

            List<SequencesRecord> sequences = sequencesAccessor.show(executionContext.getSchemaName(), whereClause);

            for (SequencesRecord seq : sequences) {
                resultCursor.addRow(buildRow(seq));
            }

            return resultCursor;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("SEQUENCES");
        resultCursor.addColumn("SCHEMA_NAME", DataTypes.StringType);
        resultCursor.addColumn("NAME", DataTypes.StringType);
        resultCursor.addColumn("VALUE", DataTypes.StringType);
        resultCursor.addColumn("UNIT_COUNT", DataTypes.StringType);
        resultCursor.addColumn("UNIT_INDEX", DataTypes.StringType);
        resultCursor.addColumn("INNER_STEP", DataTypes.StringType);
        resultCursor.addColumn("INCREMENT_BY", DataTypes.StringType);
        resultCursor.addColumn("START_WITH", DataTypes.StringType);
        resultCursor.addColumn("MAX_VALUE", DataTypes.StringType);
        resultCursor.addColumn("CYCLE", DataTypes.StringType);
        resultCursor.addColumn("TYPE", DataTypes.StringType);
        resultCursor.addColumn("PHY_SEQ_NAME", DataTypes.StringType);
        return resultCursor;
    }

    private Object[] buildRow(SequencesRecord seq) {
        String phySeqName = STR_NA;
        if (Type.valueOf(seq.type) == Type.NEW) {
            phySeqName = SequenceOptNewAccessor.genNameForNewSequence(seq.schemaName, seq.name);
        }
        return new Object[] {
            seq.schemaName, seq.name, seq.value, seq.unitCount, seq.unitIndex, seq.innerStep,
            seq.incrementBy, seq.startWith, seq.maxValue, seq.cycle, seq.type, phySeqName
        };
    }

    private String buildWhereClause(RelNode logicalPlan) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowSequences showSequences = (SqlShowSequences) show.getNativeSqlNode();

        StringBuilder whereClause = new StringBuilder();

        if (showSequences.where != null) {
            whereClause.append(" WHERE ").append(showSequences.where);
        }

        if (showSequences.orderBy != null) {
            whereClause.append(" ORDER BY ").append(showSequences.orderBy);
        }

        if (showSequences.limit != null) {
            whereClause.append(" LIMIT ").append(showSequences.limit);
        }

        return whereClause.toString();
    }

}
