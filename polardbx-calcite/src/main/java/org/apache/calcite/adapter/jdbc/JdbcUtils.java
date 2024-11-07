/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.util.Pair;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Utilities for the JDBC provider.
 */
final class JdbcUtils {
    private JdbcUtils() {
        throw new AssertionError("no instances!");
    }

    /**
     * Pool of dialects.
     */
    static class DialectPool {
        final Map<DataSource, Map<SqlDialectFactory, SqlDialect>> map0 = new IdentityHashMap<>();
        final Map<List, SqlDialect> map = new HashMap<>();

        public static final DialectPool INSTANCE = new DialectPool();

        // TODO: Discuss why we need a pool. If we do, I'd like to improve performance
        synchronized SqlDialect get(SqlDialectFactory dialectFactory, DataSource dataSource) {
            Map<SqlDialectFactory, SqlDialect> dialectMap = map0.get(dataSource);
            if (dialectMap != null) {
                final SqlDialect sqlDialect = dialectMap.get(dialectFactory);
                if (sqlDialect != null) {
                    return sqlDialect;
                }
            }
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                DatabaseMetaData metaData = connection.getMetaData();
                String productName = metaData.getDatabaseProductName();
                String productVersion = metaData.getDatabaseProductVersion();
                List key = ImmutableList.of(productName, productVersion, dialectFactory);
                SqlDialect dialect = map.get(key);
                if (dialect == null) {
                    dialect = dialectFactory.create(metaData);
                    map.put(key, dialect);
                    if (dialectMap == null) {
                        dialectMap = new IdentityHashMap<>();
                        map0.put(dataSource, dialectMap);
                    }
                    dialectMap.put(dialectFactory, dialect);
                }
                connection.close();
                connection = null;
                return dialect;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }
    }

    /**
     * Builder that calls {@link ResultSet#getObject(int)} for every column,
     * or {@code getXxx} if the result type is a primitive {@code xxx},
     * and returns an array of objects for each row.
     */
    static class ObjectArrayRowBuilder implements Function0<Object[]> {
        private final ResultSet resultSet;
        private final int columnCount;
        private final ColumnMetaData.Rep[] reps;
        private final int[] types;

        ObjectArrayRowBuilder(ResultSet resultSet, ColumnMetaData.Rep[] reps,
                              int[] types)
            throws SQLException {
            this.resultSet = resultSet;
            this.reps = reps;
            this.types = types;
            this.columnCount = resultSet.getMetaData().getColumnCount();
        }

        public static Function1<ResultSet, Function0<Object[]>> factory(
            final List<Pair<ColumnMetaData.Rep, Integer>> list) {
            return new Function1<ResultSet, Function0<Object[]>>() {
                public Function0<Object[]> apply(ResultSet resultSet) {
                    try {
                        return new ObjectArrayRowBuilder(
                            resultSet,
                            Pair.left(list).toArray(new ColumnMetaData.Rep[list.size()]),
                            Ints.toArray(Pair.right(list)));
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        public Object[] apply() {
            try {
                final Object[] values = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    values[i] = value(i);
                }
                return values;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Gets a value from a given column in a JDBC result set.
         *
         * @param i Ordinal of column (1-based, per JDBC)
         */
        private Object value(int i) throws SQLException {
            // MySQL returns timestamps shifted into local time. Using
            // getTimestamp(int, Calendar) with a UTC calendar should prevent this,
            // but does not. So we shift explicitly.
            switch (types[i]) {
            case Types.TIMESTAMP:
                return shift(resultSet.getTimestamp(i + 1));
            case Types.TIME:
                return shift(resultSet.getTime(i + 1));
            case Types.DATE:
                return shift(resultSet.getDate(i + 1));
            }
            return reps[i].jdbcGet(resultSet, i + 1);
        }

        private static Timestamp shift(Timestamp v) {
            if (v == null) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset(time);
            return new Timestamp(time + offset);
        }

        private static Time shift(Time v) {
            if (v == null) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset(time);
            return new Time((time + offset) % DateTimeUtils.MILLIS_PER_DAY);
        }

        private static Date shift(Date v) {
            if (v == null) {
                return null;
            }
            long time = v.getTime();
            int offset = TimeZone.getDefault().getOffset(time);
            return new Date(time + offset);
        }
    }
}

// End JdbcUtils.java
