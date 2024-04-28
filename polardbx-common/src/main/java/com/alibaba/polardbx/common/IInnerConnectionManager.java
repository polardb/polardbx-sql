package com.alibaba.polardbx.common;

import java.sql.Connection;
import java.sql.SQLException;

public interface IInnerConnectionManager {
    Connection getConnection() throws SQLException;

    Connection getConnection(String schema) throws SQLException;
}
