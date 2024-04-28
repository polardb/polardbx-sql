package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

public class ClearFileStoragePreparedData {
    private String fileStorageName;

    public ClearFileStoragePreparedData(String fileStorageName) {
        this.fileStorageName = fileStorageName;
    }

    public String getFileStorageName() {
        return fileStorageName;
    }
}
