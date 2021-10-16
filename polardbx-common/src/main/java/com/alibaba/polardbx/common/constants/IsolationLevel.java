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

package com.alibaba.polardbx.common.constants;

import java.sql.Connection;

public enum IsolationLevel {

    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    private final int code;

    IsolationLevel(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static IsolationLevel fromInt(int code) {
        switch (code) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            return READ_UNCOMMITTED;
        case Connection.TRANSACTION_READ_COMMITTED:
            return READ_COMMITTED;
        case Connection.TRANSACTION_REPEATABLE_READ:
            return REPEATABLE_READ;
        case Connection.TRANSACTION_SERIALIZABLE:
            return SERIALIZABLE;
        }
        return null;
    }

    public String nameWithHyphen() {
        switch (this) {
        case READ_UNCOMMITTED:
            return "READ-UNCOMMITTED";
        case READ_COMMITTED:
            return "READ-COMMITTED";
        case REPEATABLE_READ:
            return "REPEATABLE-READ";
        case SERIALIZABLE:
            return "SERIALIZABLE";
        }

        throw new AssertionError("impossible");
    }

    public static IsolationLevel parse(String value) {
        if (value == null) {
            return null;
        }
        switch (value.toUpperCase()) {
        case "READ-UNCOMMITTED":
        case "READ_UNCOMMITTED":
            return READ_UNCOMMITTED;
        case "READ-COMMITTED":
        case "READ_COMMITTED":
            return READ_COMMITTED;
        case "REPEATABLE-READ":
        case "REPEATABLE_READ":
            return REPEATABLE_READ;
        case "SERIALIZABLE":
            return SERIALIZABLE;
        }
        return null;
    }
}