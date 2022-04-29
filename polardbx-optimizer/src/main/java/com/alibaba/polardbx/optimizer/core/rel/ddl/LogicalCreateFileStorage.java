package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateFileStorage;

public class LogicalCreateFileStorage extends BaseDdlOperation {
    private CreateFileStorage createFileStorage;

    public LogicalCreateFileStorage(DDL ddl) {
        super(ddl);
        this.createFileStorage = (CreateFileStorage) relDdl;
    }

    public CreateFileStorage getCreateFileStorage() {
        return createFileStorage;
    }

    public static LogicalCreateFileStorage create(DDL ddl) {
        return new LogicalCreateFileStorage(ddl);
    }
}
