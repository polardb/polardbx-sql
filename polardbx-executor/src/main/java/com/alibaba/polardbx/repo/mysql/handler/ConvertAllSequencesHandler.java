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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ClearSeqCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.SeqTypeUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SqlConvertAllSequences;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

public class ConvertAllSequencesHandler extends AbstractDalHandler {

    public ConvertAllSequencesHandler(IRepository repo) {
        super(repo);
    }

    @Override
    Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlConvertAllSequences stmt = (SqlConvertAllSequences) logicalPlan.getNativeSqlNode();

        Type fromType = stmt.getFromType();
        Type toType = stmt.getToType();
        String schemaName = stmt.getSchemaName();
        boolean allSchemata = stmt.isAllSchemata();

        ArrayResultCursor resultCursor = new ArrayResultCursor("CONVERT_ALL_SEQUENCES");
        resultCursor.addColumn("SCHEMA_NAME", DataTypes.StringType);
        resultCursor.addColumn("NUM_OF_CONVERTED_SEQUENCES", DataTypes.StringType);
        resultCursor.addColumn("REMARK", DataTypes.StringType);
        resultCursor.initMeta();

        final Set<String> userSchemata = new HashSet<>();
        List<SchemataRecord> schemataRecords = SchemataAccessor.getAllSchemata();
        schemataRecords.stream()
            .filter(s -> !SystemDbHelper.isDBBuildIn(s.schemaName))
            .forEach(s -> userSchemata.add(s.schemaName.toLowerCase()));

        if (allSchemata) {
            for (String schema : userSchemata) {
                convert(schema, fromType, toType, resultCursor);
            }
        } else if (TStringUtil.isNotBlank(schemaName)) {
            if (userSchemata.contains(schemaName)) {
                convert(schemaName, fromType, toType, resultCursor);
            } else {
                throw new SequenceException("Invalid schema name '" + schemaName + "'");
            }
        } else {
            throw new SequenceException("Schema name should not be empty");
        }

        return resultCursor;
    }

    private void convert(String schemaName, Type fromType, Type toType, ArrayResultCursor resultCursor) {
        int numSequencesConverted = 0;
        String remark = "";

        boolean newSeqNotInvolved = fromType != Type.NEW && toType != Type.NEW;
        if (SeqTypeUtil.isNewSeqSupported(schemaName) || newSeqNotInvolved) {
            try {
                numSequencesConverted = SequencesAccessor.change(schemaName, fromType, toType);
                SyncManagerHelper.sync(new ClearSeqCacheSyncAction(schemaName, null, true, false));
            } catch (Exception e) {
                remark = "Failed due to " + e.getMessage();
            }
        }

        resultCursor.addRow(new Object[] {schemaName, numSequencesConverted, remark});
    }

}
