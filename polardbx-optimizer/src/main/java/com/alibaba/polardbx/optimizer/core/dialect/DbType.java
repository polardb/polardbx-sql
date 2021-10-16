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

package com.alibaba.polardbx.optimizer.core.dialect;

/**
 * Created by lingce.ldm on 2016/12/6.
 */
public enum DbType {
    /**
     * MYSQL
     */
    MYSQL(0, "MYSQL", new MySqlDialect());

    private final int code;
    private final String name;
    private final Dialect dialect;

    DbType(int code, String name) {
        this(code, name, null);
    }

    DbType(int code, String name, Dialect dialect) {
        this.code = code;
        this.name = name;
        this.dialect = dialect;
    }

    public int getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public Dialect getDialect() {
        return dialect;
    }

    public final void loadDriverClass() {
        if (dialect == null) {
            throw new RuntimeException("dialect not found " + getName());
        }
        try {
            Class.forName(dialect.getDriverName());
        } catch (Exception e) {
            throw new RuntimeException("load jdbc driver class fail : " + dialect.getDriverName(), e);
        }
    }

    public static DbType of(int code) {
        for (DbType e : DbType.values()) {
            if (e.getCode() == code) {
                return e;
            }
        }
        throw new UnsupportedOperationException("Unsupported database type code");
    }
}
