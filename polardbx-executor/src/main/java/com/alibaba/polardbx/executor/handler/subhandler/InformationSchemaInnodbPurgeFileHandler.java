package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbPurgeFiles;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InformationSchemaInnodbPurgeFileHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaInnodbPurgeFileHandler.class);

    public InformationSchemaInnodbPurgeFileHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbPurgeFiles;
    }

    private String getQuery(VirtualView virtualView) {
        if (virtualView instanceof InformationSchemaInnodbPurgeFiles) {
            return "select * from information_schema.innodb_purge_files";
        } else {
            throw new RuntimeException(
                "Invalid class of " + virtualView + " for " + InformationSchemaInnodbPurgeFileHandler.class);
        }
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        List<RelDataTypeField> fieldList = virtualView.getRowType().getFieldList();

        int validRowCnt = 0;

        List<String> schemaNames = new ArrayList<>();

        schemaNames.add(executionContext.getSchemaName());

        Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(schemaNames);

        for (List<TGroupDataSource> groupDataSourceList : instId2GroupList.values()) {

            TGroupDataSource groupDataSource = groupDataSourceList.get(0);

            Connection conn = null;
            Statement stmt = null;
            ResultSet rs = null;
            try {
                conn = groupDataSource.getConnection();
                stmt = conn.createStatement();
                rs = stmt.executeQuery(getQuery(virtualView));
                while (rs.next()) {
                    List<Object> newRow = new LinkedList<>();

                    for (int i = 1; i <= fieldList.size(); i++) {
                        newRow.add(rs.getObject(i));
                    }

                    cursor.addRow(newRow.toArray());
                }
            } catch (Throwable t) {
                logger.error(t);
            } finally {
                JdbcUtils.close(rs);
                JdbcUtils.close(stmt);
                JdbcUtils.close(conn);
                validRowCnt++;
            }
        }

        return cursor;
    }
}
