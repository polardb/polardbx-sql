package com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class StoragePoolTestCaseReader {
    String fileDir;

    StoragePoolTestCaseReader(String fileDir) {
        this.fileDir = fileDir;
    }

    public StoragePoolTestCaseBean readTestCase() throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(this.fileDir);
        return yaml.loadAs(inputStream, StoragePoolTestCaseBean.class);
    }
}
