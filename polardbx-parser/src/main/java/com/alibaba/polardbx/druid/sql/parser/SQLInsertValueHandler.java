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

package com.alibaba.polardbx.druid.sql.parser;

import java.math.BigDecimal;
import java.sql.SQLException;

public interface SQLInsertValueHandler {
    Object newRow() throws SQLException;

    void processInteger(Object row, int index, Number value) throws SQLException;

    void processString(Object row, int index, String value) throws SQLException;

    void processDate(Object row, int index, String value) throws SQLException;
    void processDate(Object row, int index, java.util.Date value) throws SQLException;

    void processTimestamp(Object row, int index, String value) throws SQLException;
    void processTimestamp(Object row, int index, java.util.Date value) throws SQLException;

    void processTime(Object row, int index, String value) throws SQLException;

    void processDecimal(Object row, int index, BigDecimal value) throws SQLException;

    void processBoolean(Object row, int index, boolean value) throws SQLException;

    void processNull(Object row, int index) throws SQLException;

    void processFunction(Object row, int index, String funcName, long funcNameHashCode64, Object... values) throws SQLException;

    void processRow(Object row) throws SQLException;

    void processComplete() throws SQLException;
}
