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
package org.apache.calcite.rex;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlLikeOperator;

/**
 * Visitor pattern for traversing a tree of {@link RexNode} objects.
 *
 * @see org.apache.calcite.util.Glossary#VISITOR_PATTERN
 * @see RexShuttle
 * @see RexVisitorImpl
 *
 * @param <R> Return type
 */
public interface RexVisitor<R> {
  //~ Methods ----------------------------------------------------------------

  R visitInputRef(RexInputRef inputRef);

  R visitLocalRef(RexLocalRef localRef);

  R visitLiteral(RexLiteral literal);

  R visitCall(RexCall call);

  R visitOver(RexOver over);

  R visitCorrelVariable(RexCorrelVariable correlVariable);

  R visitDynamicParam(RexDynamicParam dynamicParam);

  R visitRangeRef(RexRangeRef rangeRef);

  R visitFieldAccess(RexFieldAccess fieldAccess);

  R visitSubQuery(RexSubQuery subQuery);

  void visit(SqlBinaryOperator operator, RexCall call);

  void visit(SqlOperator operator, RexCall call);

  void visit(AggregateCall aggregateCall);

  void visit(SqlFunction operator, RexCall call);

  void visit(SqlPrefixOperator operator, RexCall call);

  void visit(SqlPostfixOperator operator, RexCall call);

  void visit(SqlLikeOperator operator, RexCall call);

  void visit(SqlCaseOperator operator, RexCall call);

  R visitTableInputRef(RexTableInputRef fieldRef);

  R visitPatternFieldRef(RexPatternFieldRef fieldRef);

  R visitSystemVar(RexSystemVar systemVar);

  R visitUserVar(RexUserVar userVar);
}

// End RexVisitor.java
