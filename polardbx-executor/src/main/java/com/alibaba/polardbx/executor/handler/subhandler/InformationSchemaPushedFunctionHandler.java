package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPushedFunction;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Set;

public class InformationSchemaPushedFunctionHandler extends BaseVirtualViewSubClassHandler {
    private static final String FIND_PUSHED_FUNCTIONS = String.format(
        "SELECT ROUTINE_NAME FROM information_schema.routines WHERE ROUTINE_SCHEMA = '%s' AND ROUTINE_TYPE = '%s' AND ROUTINE_COMMENT = '%s'",
        PlConstants.MYSQL, PlConstants.FUNCTION, PlConstants.POLARX_COMMENT);

    public InformationSchemaPushedFunctionHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPushedFunction;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Set<String> allDnId = ExecUtils.getAllDnStorageId();
        for (String dnId : allDnId) {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(FIND_PUSHED_FUNCTIONS)) {
                while (rs.next()) {
                    cursor.addRow(new Object[] {
                        dnId,
                        DataTypes.StringType.convertFrom(rs.getString("ROUTINE_NAME"))
                    });
                }
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to create function on " + dnId, e);
            }
        }
        return cursor;
    }
}
