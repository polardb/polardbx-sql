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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * Base class for binary operators such as addition, subtraction, and
 * multiplication which are monotonic for the patterns <code>m op c</code> and
 * <code>c op m</code> where m is any monotonic expression and c is a constant.
 */
public class SqlMonotonicBinaryOperator extends SqlBinaryOperator {
    // ~ Constructors
    // -----------------------------------------------------------

    public SqlMonotonicBinaryOperator(String name, SqlKind kind, int prec, boolean isLeftAssoc,
                                      SqlReturnTypeInference returnTypeInference,
                                      SqlOperandTypeInference operandTypeInference,
                                      SqlOperandTypeChecker operandTypeChecker){
        super(name, kind, prec, isLeftAssoc, returnTypeInference, operandTypeInference, operandTypeChecker);
    }

    // ~ Methods
    // ----------------------------------------------------------------

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }
}

// End SqlMonotonicBinaryOperator.java
