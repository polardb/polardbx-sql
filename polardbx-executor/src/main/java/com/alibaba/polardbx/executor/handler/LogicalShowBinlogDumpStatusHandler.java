package com.alibaba.polardbx.executor.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.common.cdc.CdcConstants;
import com.alibaba.polardbx.common.cdc.ResultCode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.PooledHttpHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.net.util.CdcTargetUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowBinlogDumpStatus;

import org.apache.commons.collections.CollectionUtils;
import org.apache.http.entity.ContentType;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class LogicalShowBinlogDumpStatusHandler extends HandlerCommon {
    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalShowBinlogDumpStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        SqlShowBinlogDumpStatus sqlShowBinlogDumpStatus =
            (SqlShowBinlogDumpStatus) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode with = sqlShowBinlogDumpStatus.getWith();
        String groupName = with == null ? "" : RelUtils.lastStringValue(with);
        Map<String, String> params = new HashMap<>(1);
        String daemonEndpoint;
        if (StringUtils.isEmpty(groupName)) {
            daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget();
        } else {
            daemonEndpoint = CdcTargetUtil.getDaemonMasterTarget(groupName);
        }
        params.put("instId", InstIdUtil.getInstId());
        String res;
        try {
            res = PooledHttpHelper.doPost("http://" + daemonEndpoint + "/dumper/showBinlogDumpStatus",
                ContentType.APPLICATION_JSON,
                JSON.toJSONString(params), 10000);
        } catch (Exception e) {
            cdcLogger.error("show dump status error!", e);
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, e, e.getMessage());
        }

        ResultCode<?> httpResult = JSON.parseObject(res, ResultCode.class);
        if (httpResult.getCode() != CdcConstants.SUCCESS_CODE) {
            cdcLogger.warn("show slave status failed! code:" + httpResult.getCode() + ", msg:" + httpResult.getMsg());
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, httpResult.getMsg());
        }

        String jsonResponse = (String) httpResult.getData();
        List<LinkedHashMap<String, String>> responses = JSON.parseObject(jsonResponse,
            new TypeReference<List<LinkedHashMap<String, String>>>() {
            });

        ArrayResultCursor result = new ArrayResultCursor("SHOW BINLOG DUMP STATUS");
        if (CollectionUtils.isEmpty(responses)) {
            result.addColumn("Process_Id", DataTypes.StringType, false);
            result.addColumn("Trace_Id", DataTypes.StringType, false);
            result.addColumn("Dumper_Address", DataTypes.StringType, false);
            result.addColumn("Client_Ip", DataTypes.StringType, false);
            result.addColumn("Client_Port", DataTypes.StringType, false);
            result.addColumn("Filename", DataTypes.StringType, false);
            result.addColumn("Position", DataTypes.StringType, false);
            result.addColumn("Delay", DataTypes.StringType, false);
            result.addColumn("Bps", DataTypes.StringType, false);
            result.addColumn("Last_Sync_Timestamp", DataTypes.StringType, false);
            result.addColumn("Alive_Second", DataTypes.StringType, false);

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
