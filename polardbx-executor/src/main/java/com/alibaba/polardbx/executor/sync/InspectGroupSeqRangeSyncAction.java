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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_NODE;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.GROUP_SEQ_RANGE;

/**
 * Created by chensr on 2017/10/31.
 */
public class InspectGroupSeqRangeSyncAction implements ISyncAction {

    private String seqName;
    private String schemaName;

    public InspectGroupSeqRangeSyncAction() {

    }

    public InspectGroupSeqRangeSyncAction(String schemaName, String seqName) {
        this.schemaName = schemaName;
        this.seqName = seqName;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("GROUP_SEQ_RANGE");
        resultCursor.addColumn(GROUP_SEQ_NODE, DataTypes.StringType);
        resultCursor.addColumn(GROUP_SEQ_RANGE, DataTypes.StringType);
        resultCursor.initMeta();

        String serverInfo = TddlNode.getNodeInfo();

        String currentRange = SequenceManagerProxy.getInstance().getCurrentSeqRange(schemaName, seqName);

        resultCursor.addRow(new Object[] {serverInfo, currentRange});

        return resultCursor;
    }

    public String getSeqName() {
        return seqName;
    }

    public void setSeqName(String seqName) {
        this.seqName = seqName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
