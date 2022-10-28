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

package com.alibaba.polardbx.executor.ddl.job.task.basic.pl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;

public class PlConstants {
    public final static String PROCEDURE = "PROCEDURE";

    public final static String FUNCTION = "FUNCTION";

    public final static String MOCK_SQL_MODE =
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

    public final static String DEFINER = "DEFINER";

    public final static String MOCK_CHARACTER_SET_CLIENT = "utf8";

    public final static String MOCK_COLLATION_CONNECTION = "utf8_general_ci";

    public final static String MOCK_DATABASE_COLLATION = "utf8_general_ci";

    public final static String DEF_ROUTINE_CATALOG = "def";

    public final static String SQL = "SQL";

    public final static String NO_SQL = "NO SQL";

    public final static String MYSQL = "mysql";

    public final static String NOT_FOUND = "NOT FOUND";

    public final static String POLARX_COMMENT = "magic_polarx";

    public final static String PUSH_DOWN_UDF = "push_down_udf";

    public final static String INTERNAL_TRACE_ID = "-polarx_internal-";

    public final static String SPLIT_CHAR = "-";

    public final static int INIT_DEPTH = 0;

    public final static SQLCreateProcedureStatement PROCEDURE_STMT_HOLDER = new SQLCreateProcedureStatement();

    public final static Pair<SQLCreateProcedureStatement, Long> PROCEDURE_PLACE_HOLDER = Pair.of(PROCEDURE_STMT_HOLDER, 0L);

    public final static SQLCreateFunctionStatement FUNCTION_STMT_HOLDER = new SQLCreateFunctionStatement();

    public final static Pair<SQLCreateFunctionStatement, Long> FUNCTION_PLACE_HOLDER = Pair.of(FUNCTION_STMT_HOLDER, 0L);
}
