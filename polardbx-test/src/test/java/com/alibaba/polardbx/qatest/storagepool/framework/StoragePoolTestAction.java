package com.alibaba.polardbx.qatest.storagepool.framework;

import java.sql.Connection;

public interface StoragePoolTestAction {
    public default void innerTest(Connection connection) {
    }
}
