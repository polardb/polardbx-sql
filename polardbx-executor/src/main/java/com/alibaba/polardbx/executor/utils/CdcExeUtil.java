package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogStreamRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.util.List;

public class CdcExeUtil {

    public static String tryExtractStreamNameFromUser(ExecutionContext executionContext) {
        ParamManager paramManager = executionContext.getParamManager();
        boolean enableExtract = paramManager.getBoolean(ConnectionParams.ENABLE_EXTRACT_STREAM_NAME_FROM_USER);
        if (enableExtract) {
            String user = executionContext.getPrivilegeContext().getUser();
            return extractStreamNameFromUser(user);
        } else {
            return "";
        }
    }

    private static String extractStreamNameFromUser(String userName) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            BinlogStreamAccessor accessor = new BinlogStreamAccessor();
            accessor.setConnection(metaDbConn);
            List<BinlogStreamRecord> streamList = accessor.listAllStream();
            return streamList.stream()
                .map(BinlogStreamRecord::getStreamName)
                .filter(s -> StringUtils.equalsIgnoreCase(userName, s + "_cdc_user"))
                .findFirst()
                .orElse("");
        } catch (Throwable ex) {
            if (ex instanceof TddlRuntimeException) {
                throw (TddlRuntimeException) ex;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, ex.getMessage());
        }
    }
}
