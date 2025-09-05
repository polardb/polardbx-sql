package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.ttl.TtlInfoAccessor;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlArchiveKind;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTtlInfo;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by guxu.
 *
 * @author guxu
 */
public class InformationSchemaTtlInfoHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaTtlInfoHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTtlInfo;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        List<TtlInfoRecord> ttlInfoRecs = new ArrayList<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            TtlInfoAccessor ttlInfoAccessor = new TtlInfoAccessor();
            ttlInfoAccessor.setConnection(metaDbConn);
            ttlInfoRecs = ttlInfoAccessor.queryAllTtlInfoList();
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.error(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex);
        }

        for (int i = 0; i < ttlInfoRecs.size(); i++) {
            TtlInfoRecord ttlInfoRec = ttlInfoRecs.get(i);
            cursor.addRow(new Object[] {
                ttlInfoRec.getTableSchema(),
                ttlInfoRec.getTableName(),
                ttlInfoRec.getTtlStatus() == TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE ?
                    TtlInfoRecord.TTL_STATUS_ENABLE_SCHEDULE_STR_VAL :
                    TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE_STR_VAL,
                ttlInfoRec.getTtlCol(),
                ttlInfoRec.getTtlExpr(),
                ttlInfoRec.getTtlCron(),
                TtlArchiveKind.of(ttlInfoRec.getArcKind()).getArchiveKindStr(),
                ttlInfoRec.getArcTblSchema(),
                ttlInfoRec.getArcTblName(),
                ttlInfoRec.getArcPrePartCnt(),
                ttlInfoRec.getArcPostPartCnt()
            });
        }

        return cursor;
    }
}

