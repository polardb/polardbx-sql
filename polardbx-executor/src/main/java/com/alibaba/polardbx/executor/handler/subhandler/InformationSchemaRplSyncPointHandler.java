package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaRplSyncPoint;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yudong
 * @since 2024/6/18 15:36
 **/
public class InformationSchemaRplSyncPointHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaRplSyncPointHandler.class);

    public InformationSchemaRplSyncPointHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaRplSyncPoint;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try {
            if (!MetaDbUtil.hasTable(GmsSystemTables.RPL_SYNC_POINT_TABLE)) {
                return cursor;
            }
        } catch (SQLException ex) {
            logger.error("get information schema rpl sync point failed!", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
        }

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement stmt = metaDbConn.prepareStatement(
                "SELECT * FROM " + GmsSystemTables.RPL_SYNC_POINT_TABLE);
            ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                cursor.addRow(new Object[] {
                    rs.getLong("ID"),
                    rs.getString("PRIMARY_TSO"),
                    rs.getString("SECONDARY_TSO"),
                    rs.getTimestamp("CREATE_TIME")
                });
            }
        } catch (SQLException ex) {
            logger.error("get information schema rpl sync point failed!", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex);
        }
        return cursor;
    }
}
