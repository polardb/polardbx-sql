package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaSchemata;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;

public class InformationSchemaSchemataHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaSchemataHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaSchemata;
    }

    /**
     * CATALOG_NAME is always "def"
     * SQL_PATH is always NULL
     * DEFAULT_ENCRYPTION for compatibility with mysql8.0, value is "NO"
     */
    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<SchemataRecord> allSchemata = SchemataAccessor.getAllSchemata();
        for (SchemataRecord schemata : allSchemata) {
            cursor.addRow(new Object[] {
                "def", schemata.schemaName, schemata.defaultCharSetName, schemata.defaultCollationName,
                null, "NO"});
        }
        return cursor;
    }
}
