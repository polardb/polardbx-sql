package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ImportTableSpaceInfoStatAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImportTableSpaceInfoStatAccessor.class);

    public static final String IMPORT_TABLESPACE_INFO_STAT = wrap(GmsSystemTables.IMPORT_TABLESPACE_INFO_STAT);
    private static final String INSERT_DATA = "replace into " + IMPORT_TABLESPACE_INFO_STAT
        + " (task_id, table_schema, table_name, physical_db, physical_table, data_size, start_time, end_time)"
        + " values (?, ?, ?, ?, ?, ?, ?, ?)";

    public int insert(List<ImportTableSpaceInfoStatRecord> recordList) {
        try {
            if (CollectionUtils.isEmpty(recordList)) {
                return 0;
            }
            List<Map<Integer, ParameterContext>> paramsBatch =
                recordList.stream().map(e -> e.buildParams()).collect(Collectors.toList());
            DdlMetaLogUtil.logSql(INSERT_DATA + " record count: " + recordList.size());
            int[] r = MetaDbUtil.insert(INSERT_DATA, paramsBatch, connection);
            return Arrays.stream(r).sum();
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into " + IMPORT_TABLESPACE_INFO_STAT, "insert into", e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            IMPORT_TABLESPACE_INFO_STAT, e.getMessage());
    }
}
