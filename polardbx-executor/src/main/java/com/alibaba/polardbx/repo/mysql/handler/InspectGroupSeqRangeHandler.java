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

import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.InspectGroupSeqRangeSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import org.apache.calcite.sql.SqlInspectGroupSeqRange;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_NODE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_RANGE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

/**
 * Created by chensr on 2017/10/31.
 */
public class InspectGroupSeqRangeHandler extends AbstractDalHandler {

    private static final Logger logger = LoggerFactory.getLogger(InspectGroupSeqRangeHandler.class);

    public InspectGroupSeqRangeHandler(IRepository repo) {
        super(repo);
    }

    @Override
    Cursor doHandle(LogicalDal logicalPlan, ExecutionContext executionContext) {
        SqlInspectGroupSeqRange stmt = (SqlInspectGroupSeqRange) logicalPlan.getNativeSqlNode();

        String seqName = stmt.getName().toString();
        String schenamName = logicalPlan.getSchemaName();
        if (StringUtils.isEmpty(schenamName)) {
            schenamName = executionContext.getSchemaName();
        }

        Type seqType = SequenceManagerProxy.getInstance().checkIfExists(schenamName, seqName);
        if (seqType != Type.GROUP) {
            throw new SequenceException("Group sequence '" + seqName + "' doesn't exist.");
        }

        ArrayResultCursor resultCursor = new ArrayResultCursor("GROUP_SEQ_RANGE");
        resultCursor.addColumn(GROUP_SEQ_NODE, DataTypes.StringType);
        resultCursor.addColumn(GROUP_SEQ_RANGE, DataTypes.StringType);
        resultCursor.initMeta();

        // Get current ranges from all servers.
        List<List<Map<String, Object>>> resultSets =
            SyncManagerHelper.sync(new InspectGroupSeqRangeSyncAction(schenamName, seqName), schenamName);
        for (List<Map<String, Object>> resultSet : resultSets) {
            if (resultSet != null) {
                for (Map<String, Object> row : resultSet) {
                    resultCursor.addRow(new Object[] {row.get(GROUP_SEQ_NODE), row.get(GROUP_SEQ_RANGE)});
                }
            }
        }

        return resultCursor;
    }

}
