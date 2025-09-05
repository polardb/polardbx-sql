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

package com.alibaba.polardbx.executor.ddl.newengine.sync;

import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineRequester;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

@Data
public class DdlResponseCollectSyncAction implements IGmsSyncAction {

    private String schemaName;
    private List<Long> jobIds;

    public DdlResponseCollectSyncAction(final String schemaName, final List<Long> jobIds) {
        this.schemaName = schemaName;
        this.jobIds = jobIds;
    }

    @Override
    public Object sync() {
        //fetch DDL response, filter by schemaName and jobId
        List<DdlResponse.Response> responseList = DdlEngineRequester.getResponse().stream().filter(
            e -> StringUtils.equalsIgnoreCase(e.getSchemaName(), schemaName)
        ).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(jobIds)) {
            responseList =
                responseList.stream().filter(e -> jobIds.contains(e.getJobId())).collect(Collectors.toList());
        }

        //return
        ArrayResultCursor resultCursor = buildResultCursor();
        responseList.stream().forEach(e -> resultCursor.addRow(buildRow(e)));
        return resultCursor;
    }

    public static ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("DDL_JOB_RESPONSE");
        resultCursor.addColumn("JOB_ID", DataTypes.StringType);
        resultCursor.addColumn("SCHEMA_NAME", DataTypes.StringType);
        resultCursor.addColumn("OBJECT_NAME", DataTypes.StringType);
        resultCursor.addColumn("DDL_TYPE", DataTypes.StringType);
        resultCursor.addColumn("RESULT_TYPE", DataTypes.StringType);
        resultCursor.addColumn("RESULT_CONTENT", DataTypes.StringType);
        resultCursor.addColumn("DDL_STMT", DataTypes.StringType);
        resultCursor.addColumn("START_TIME", DataTypes.StringType);
        resultCursor.addColumn("END_TIME", DataTypes.StringType);
        resultCursor.initMeta();
        return resultCursor;
    }

    private Object[] buildRow(DdlResponse.Response response) {
        return new Object[] {
            String.valueOf(response.getJobId()),
            response.getSchemaName(),
            response.getObjectName(),
            response.getDdlType(),
            response.getResponseType().name(),
            response.getResponseContent(),
            response.getDdlStmt(),
            response.getStartTime(),
            response.getEndTime()
        };
    }

}
