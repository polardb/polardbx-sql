package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ClearFileStoragePreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.ClearFileStorage;

public class LogicalClearFileStorage extends BaseDdlOperation {
    private ClearFileStoragePreparedData preparedData;

    public LogicalClearFileStorage(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public void preparedData() {
        ClearFileStorage clearFileStorage = (ClearFileStorage) relDdl;
        preparedData = new ClearFileStoragePreparedData(clearFileStorage.getFileStorageName());
    }

    public ClearFileStoragePreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalClearFileStorage create(DDL ddl) {
        return new LogicalClearFileStorage(ddl);
    }
}
