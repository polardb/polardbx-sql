package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.SQLException;

public class ColumnarReadBaseTestCase extends AutoReadBaseTestCase {

    public static final String DB_NAME = "columnar_test";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.createPartDatabase(tddlConnection, DB_NAME);
        }
    }

    @Before
    final public void before() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
    }

    public static Connection getColumnarConnection() throws SQLException {
        Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
        JdbcUtil.useDb(connection, DB_NAME);
        return connection;
    }
}
