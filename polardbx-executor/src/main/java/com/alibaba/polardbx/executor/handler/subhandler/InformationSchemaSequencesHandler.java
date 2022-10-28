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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptNewAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequencesRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaSequences;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.STR_NA;

public class InformationSchemaSequencesHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaSequencesHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaSequences;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            sequencesAccessor.setConnection(metaDbConn);

            List<SequencesRecord> sequences = sequencesAccessor.show();

            for (SequencesRecord seq : sequences) {
                cursor.addRow(buildRow(seq));
            }

            return cursor;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    private Object[] buildRow(SequencesRecord seq) {
        String phySeqName = STR_NA;
        if (SequenceAttribute.Type.valueOf(seq.type) == SequenceAttribute.Type.NEW) {
            phySeqName = SequenceOptNewAccessor.genNameForNewSequence(seq.schemaName, seq.name);
        }
        return new Object[] {
            seq.id, seq.schemaName, seq.name, seq.value,
            seq.unitCount, seq.unitIndex, seq.innerStep,
            seq.incrementBy, seq.startWith, seq.maxValue, seq.cycle,
            seq.type, seq.status, phySeqName, seq.gmtCreated, seq.gmtModified
        };
    }

}


