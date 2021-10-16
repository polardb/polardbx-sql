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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Strip some code from GsiUtils in executor package
 *
 * @author Eric Fu
 */
public abstract class GsiUtils {

    public static List<String> getShardingKeys(TableMeta tableMeta, String schemaName) {
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        return or.getSharedColumns(tableMeta.getTableName())
            .stream()
            .map(String::toUpperCase)
            .collect(Collectors.toList());
    }

    public interface Consumer<R> {
        void accept(R r) throws Exception;
    }

    public static Connection getConnectionForWrite(DataSource dataSource) throws SQLException {
        if (dataSource instanceof IDataSource) {
            return ((IDataSource) dataSource).getConnection(MasterSlave.MASTER_ONLY);
        } else {
            return dataSource.getConnection();
        }
    }

    public static void wrapWithTransaction(DataSource dataSource, Consumer<Connection> caller,
                                           Function<Exception, RuntimeException> errorHandler) {
        Connection connection = null;
        try {
            connection = getConnectionForWrite(dataSource);
            connection.setAutoCommit(false);

            caller.accept(connection);

            connection.commit();
        } catch (Exception e) {
            try {
                if (null != connection) {
                    connection.rollback();
                }
            } catch (SQLException ignored) {
            }

            throw errorHandler.apply(e);
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }
}
