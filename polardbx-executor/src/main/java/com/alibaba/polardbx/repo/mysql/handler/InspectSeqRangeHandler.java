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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.InspectSeqRangeSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import org.apache.calcite.sql.SqlInspectSeqRange;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.SEQ_NODE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.SEQ_RANGE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

/**
 * Created by chensr on 2017/10/31.
 */
public class InspectSeqRangeHandler extends AbstractDalHandler {

    public InspectSeqRangeHandler(IRepository repo) {
        super(repo);
    }

    @Override
    Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlInspectSeqRange stmt = (SqlInspectSeqRange) logicalPlan.getNativeSqlNode();

        String seqName = stmt.getName().toString();
        String schemaName = logicalPlan.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        Type seqType = SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        if (seqType != Type.GROUP && seqType != Type.NEW) {
            throw new SequenceException("Group/New Sequence (caching on CN) '" + seqName + "' doesn't exist.");
        }

        ArrayResultCursor resultCursor = new ArrayResultCursor("SEQ_RANGE");
        resultCursor.addColumn(SEQ_NODE, DataTypes.StringType);
        resultCursor.addColumn(SEQ_RANGE, DataTypes.StringType);
        resultCursor.initMeta();

        // Get current ranges from all servers.
        List<List<Map<String, Object>>> resultSets =
            SyncManagerHelper.sync(new InspectSeqRangeSyncAction(schemaName, seqName), schemaName);
        for (List<Map<String, Object>> resultSet : resultSets) {
            if (resultSet != null) {
                for (Map<String, Object> row : resultSet) {
                    resultCursor.addRow(new Object[] {row.get(SEQ_NODE), row.get(SEQ_RANGE)});
                }
            }
        }

        return resultCursor;
    }

}
