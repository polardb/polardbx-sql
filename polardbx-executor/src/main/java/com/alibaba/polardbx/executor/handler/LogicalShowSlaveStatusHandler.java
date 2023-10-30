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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowSlaveStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.entity.ContentType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author shicai.xsc 2021/3/5 14:32
 * @since 5.0.0.0
 */
public class LogicalShowSlaveStatusHandler extends LogicalReplicationBaseHandler {

    public LogicalShowSlaveStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDal dal = (LogicalDal) logicalPlan;
        SqlShowSlaveStatus sqlNode = (SqlShowSlaveStatus) dal.getNativeSqlNode();

        String daemonEndpoint = CdcTargetUtil.getReplicaDaemonMasterTarget();
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/replica/showSlaveStatus",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(sqlNode.getParams()), 10000);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, e);
        }
        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, httpResult.getMsg());
        }

        String jsonResponse = (String) httpResult.getData();
        List<LinkedHashMap<String, String>> responses = JSON.parseObject(jsonResponse,
            new TypeReference<List<LinkedHashMap<String, String>>>() {
            });

        ArrayResultCursor result = new ArrayResultCursor("SHOW SLAVE STATUS");
        if (CollectionUtils.isEmpty(responses)) {
            result.addColumn("Slave_IO_State", DataTypes.StringType, false);
            result.addColumn("Master_Host", DataTypes.StringType, false);
            result.addColumn("Master_User", DataTypes.StringType, false);
            result.addColumn("Master_Port", DataTypes.StringType, false);
            result.addColumn("Connect_Retry", DataTypes.StringType, false);
            result.addColumn("Master_Log_File", DataTypes.StringType, false);
            result.addColumn("Read_Master_Log_Pos", DataTypes.StringType, false);
            result.addColumn("Relay_Log_File", DataTypes.StringType, false);
            result.addColumn("Relay_Log_Pos", DataTypes.StringType, false);
            result.addColumn("Relay_Master_Log_File", DataTypes.StringType, false);
            result.addColumn("Slave_IO_Running", DataTypes.StringType, false);
            result.addColumn("Slave_SQL_Running", DataTypes.StringType, false);
            result.addColumn("Replicate_Do_DB", DataTypes.StringType, false);
            result.addColumn("Replicate_Ignore_DB", DataTypes.StringType, false);
            result.addColumn("Replicate_Do_Table", DataTypes.StringType, false);
            result.addColumn("Replicate_Ignore_Table", DataTypes.StringType, false);
            result.addColumn("Replicate_Wild_Do_Table", DataTypes.StringType, false);
            result.addColumn("Replicate_Wild_Ignore_Table", DataTypes.StringType, false);
            result.addColumn("Last_Errno", DataTypes.StringType, false);
            result.addColumn("Last_Error", DataTypes.StringType, false);
            result.addColumn("Skip_Counter", DataTypes.StringType, false);
            result.addColumn("Exec_Master_Log_Pos", DataTypes.StringType, false);
            result.addColumn("Relay_Log_Space", DataTypes.StringType, false);
            result.addColumn("Until_Condition", DataTypes.StringType, false);
            result.addColumn("Until_Log_File", DataTypes.StringType, false);
            result.addColumn("Until_Log_Pos", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Allowed", DataTypes.StringType, false);
            result.addColumn("Master_SSL_CA_File", DataTypes.StringType, false);
            result.addColumn("Master_SSL_CA_Path", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Cert", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Cipher", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Key", DataTypes.StringType, false);
            result.addColumn("Seconds_Behind_Master", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Verify_Server_Cert", DataTypes.StringType, false);
            result.addColumn("Last_IO_Errno", DataTypes.StringType, false);
            result.addColumn("Last_IO_Error", DataTypes.StringType, false);
            result.addColumn("Last_SQL_Errno", DataTypes.StringType, false);
            result.addColumn("Last_SQL_Error", DataTypes.StringType, false);
            result.addColumn("Replicate_Ignore_Server_Ids", DataTypes.StringType, false);
            result.addColumn("Master_Server_Id", DataTypes.StringType, false);
            result.addColumn("Master_UUID", DataTypes.StringType, false);
            result.addColumn("Master_Info_File", DataTypes.StringType, false);
            result.addColumn("SQL_Delay", DataTypes.StringType, false);
            result.addColumn("SQL_Remaining_Delay", DataTypes.StringType, false);
            result.addColumn("Slave_SQL_Running_State", DataTypes.StringType, false);
            result.addColumn("Master_Retry_Count", DataTypes.StringType, false);
            result.addColumn("Master_Bind", DataTypes.StringType, false);
            result.addColumn("Last_IO_Error_Timestamp", DataTypes.StringType, false);
            result.addColumn("Last_SQL_Error_Timestamp", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Crl", DataTypes.StringType, false);
            result.addColumn("Master_SSL_Crlpath", DataTypes.StringType, false);
            result.addColumn("Retrieved_Gtid_Set", DataTypes.StringType, false);
            result.addColumn("Executed_Gtid_Set", DataTypes.StringType, false);
            result.addColumn("Auto_Position", DataTypes.StringType, false);
            result.addColumn("Replicate_Rewrite_DB", DataTypes.StringType, false);
            result.addColumn("Channel_Name", DataTypes.StringType, false);
            result.addColumn("Master_TLS_Version", DataTypes.StringType, false);
            result.addColumn("Replicate_Mode", DataTypes.StringType, false);
            result.addColumn("Running_Stage", DataTypes.StringType, false);
            result.initMeta();
            return result;
        }
        for (Map.Entry<String, String> entry : responses.get(0).entrySet()) {
            result.addColumn(entry.getKey(), DataTypes.StringType, false);
        }

        result.initMeta();

        for (LinkedHashMap<String, String> response : responses) {
            Object[] values = new Object[result.getReturnColumns().size()];
            result.addRow(values);

            for (int i = 0; i < result.getReturnColumns().size(); i++) {
                ColumnMeta columnMeta = result.getReturnColumns().get(i);
                String value = response.getOrDefault(columnMeta.getName(), "");
                values[i] = value;
            }
        }

        return result;
    }
}

