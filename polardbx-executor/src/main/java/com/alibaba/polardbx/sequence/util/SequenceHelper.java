/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.sequence.util;

import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author chensr 2016年10月27日 下午2:16:20
 * @since 5.0.0
 */
public class SequenceHelper {

    private static final Logger logger = LoggerFactory.getLogger(SequenceHelper.class);

    public static void closeDbResources(ResultSet rs, Statement stmt, Connection conn) {
        close(rs);
        close(stmt);
        close(conn);
    }

    private static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC ResultSet.", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC ResultSet.", e);
            }
        }
    }

    private static void close(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC Statement.", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC Statement.", e);
            }
        }
    }

    private static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.debug("Could not close JDBC Connection.", e);
            } catch (Throwable e) {
                logger.debug("Unexpected exception on closing JDBC Connection.", e);
            }
        }
    }

    public static Connection getConnection(DataSource dataSource) throws SQLException {
        if (dataSource instanceof IDataSource) {
            return ((IDataSource) dataSource).getConnection(MasterSlave.MASTER_ONLY);
        } else {
            return dataSource.getConnection();
        }
    }
}
