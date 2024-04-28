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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A <code>SqlCall</code> is a call to an {@link SqlOperator operator}.
 * (Operators can be used to describe any syntactic construct, so in practice,
 * every non-leaf node in a SQL parse tree is a <code>SqlCall</code> of some
 * kind.)
 */
public abstract class SqlCall extends SqlNode {
  //~ Constructors -----------------------------------------------------------

  public SqlCall(SqlParserPos pos) {
    super(pos);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Whether this call was created by expanding a parentheses-free call to
   * what was syntactically an identifier.
   */
  public boolean isExpanded() {
    return false;
  }

  /**
   * Changes the value of an operand. Allows some rewrite by
   * {@link SqlValidator}; use sparingly.
   *
   * @param i Operand index
   * @param operand Operand value
   */
  public void setOperand(int i, SqlNode operand) {
    throw new UnsupportedOperationException();
  }

  @Override public SqlKind getKind() {
    return getOperator().getKind();
  }

  public abstract SqlOperator getOperator();

  public abstract List<SqlNode> getOperandList();

  @SuppressWarnings("unchecked")
  public <S extends SqlNode> S operand(int i) {
    return (S) getOperandList().get(i);
  }

  public int operandCount() {
    return getOperandList().size();
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    final List<SqlNode> operandList = getOperandList();
    return getOperator().createCall(getFunctionQuantifier(), pos,
        operandList.toArray(new SqlNode[operandList.size()]));
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    final SqlOperator operator = getOperator();
    final SqlDialect dialect = writer.getDialect();

    if (leftPrec > operator.getLeftPrec()
        || (operator.getRightPrec() <= rightPrec && (rightPrec != 0))
        || writer.isAlwaysUseParentheses() && isA(SqlKind.EXPRESSION)) {
      // values do not with "( )" at outer.
      if (operator.getKind().equals(SqlKind.VALUES)||operator.getKind().equals(SqlKind.ROW)||operator.getKind().equals(SqlKind.AS_OF)) {
        dialect.unparseCall(writer, this, 0, 0);
      }else {
        if (operandCount() == 2 ) {
          boolean parentUion = false;
          parentUion = isParentUion(writer, parentUion);
          pushOpt(writer);//if can puush, it must is a union statement
          final SqlNode operand1 = getOperandList().get(1);
          if (parentUion && (operand1 instanceof SqlOrderBy || operand1 instanceof SqlSelect) ) {
            dialect.unparseCall(writer, this, 0, 0);
          } else if ((this.getOperator() == SqlStdOperatorTable.UNION_ALL || this.getOperator() == SqlStdOperatorTable.UNION)
                  && writer instanceof SqlPrettyWriter && ((SqlPrettyWriter) writer).getListStack().isEmpty()) {
            dialect.unparseCall(writer, this, 0, 0);
          } else {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            dialect.unparseCall(writer, this, 0, 0);
            writer.endList(frame);
          }
          popOpt(writer);
        } else {
          final SqlWriter.Frame frame = writer.startList("(", ")");
          dialect.unparseCall(writer, this, 0, 0);
          writer.endList(frame);
        }
      }
    } else {
      dialect.unparseCall(writer, this, leftPrec, rightPrec);
    }
  }

  private boolean isParentUion(SqlWriter writer, boolean parentUion) {
    if (writer instanceof SqlPrettyWriter) {
      final SqlWriter.FrameTypeEnum frameTypeEnum = ((SqlPrettyWriter) writer).peekOptStack();
      if (frameTypeEnum == SqlWriter.FrameTypeEnum.SETOP) {
        parentUion = true;
      }
    }
    return parentUion;
  }

  private void pushOpt(SqlWriter writer) {
    if (this.getOperator() == SqlStdOperatorTable.UNION_ALL || this.getOperator() == SqlStdOperatorTable.UNION) {
      if (writer instanceof SqlPrettyWriter) {
        ((SqlPrettyWriter)writer).pushOptStack(SqlWriter.FrameTypeEnum.SETOP);
      }
    }
  }

  private void popOpt(SqlWriter writer) {
    if (this.getOperator() == SqlStdOperatorTable.UNION_ALL || this.getOperator() == SqlStdOperatorTable.UNION) {
      if (writer instanceof SqlPrettyWriter) {
        ((SqlPrettyWriter)writer).popOptStack();
      }
    }
  }

  /**
   * Validates this call.
   *
   * <p>The default implementation delegates the validation to the operator's
   * {@link SqlOperator#validateCall}. Derived classes may override (as do,
   * for example {@link SqlSelect} and {@link SqlUpdate}).
   */
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateCall(this, scope);
  }

  public void findValidOptions(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlParserPos pos,
      Collection<SqlMoniker> hintList) {
    for (SqlNode operand : getOperandList()) {
      if (operand instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) operand;
        SqlParserPos idPos = id.getParserPosition();
        if (idPos.toString().equals(pos.toString())) {
          ((SqlValidatorImpl) validator).lookupNameCompletionHints(
              scope, id.names, pos, hintList);
          return;
        }
      }
    }
    // no valid options
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
    if (node == this) {
      return true;
    }
    if (!(node instanceof SqlCall)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlCall that = (SqlCall) node;

    // Compare operators by name, not identity, because they may not
    // have been resolved yet. Use case insensitive comparison since
    // this may be a case insensitive system.
    if (!this.getOperator().getName().equalsIgnoreCase(that.getOperator().getName())) {
      return litmus.fail("{} != {}", this, node);
    }
    return equalDeep(this.getOperandList(), that.getOperandList(), litmus, context);
  }

  /**
   * Returns a string describing the actual argument types of a call, e.g.
   * "SUBSTR(VARCHAR(12), NUMBER(3,2), INTEGER)".
   */
  protected String getCallSignature(
      SqlValidator validator,
      SqlValidatorScope scope) {
    List<String> signatureList = new ArrayList<>();
    for (final SqlNode operand : getOperandList()) {
      final RelDataType argType = validator.deriveType(scope, operand);
      if (null == argType) {
        continue;
      }
      signatureList.add(argType.toString());
    }
    return SqlUtil.getOperatorSignature(getOperator(), signatureList);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    // Delegate to operator.
    final SqlCallBinding binding =
        new SqlCallBinding(scope.getValidator(), scope, this);
    return getOperator().getMonotonicity(binding);
  }

  /**
   * Test to see if it is the function COUNT(*)
   *
   * @return boolean true if function call to COUNT(*)
   */
  public boolean isCountStar() {
    if (getOperator().isName("COUNT") && operandCount() == 1) {
      final SqlNode parm = operand(0);
      if (parm instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) parm;
        if (id.isStar() && id.names.size() == 1) {
          return true;
        }
      }
    }

    return false;
  }

  public boolean isCountLiteral() {
    if (getOperator().isName("COUNT") && operandCount() == 1) {
      final SqlNode parm = operand(0);
      if (parm instanceof SqlNumericLiteral) {
        return true;
      }
    }

    return false;
  }

  /**
   * Test to see if it is the function CHECK_SUM(*)
   *
   * @return boolean true if function call to CHECK_SUM(*)
   */
  public boolean isCheckSumStar() {
    if (getOperator().isName("CHECK_SUM") && operandCount() == 1) {
      final SqlNode parm = operand(0);
      if (parm instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) parm;
        if (id.isStar() && id.names.size() == 1) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Test to see if it is the function CHECK_SUM_V2(*)
   *
   * @return boolean true if function call to CHECK_SUM_V2(*)
   */
  public boolean isCheckSumV2Star() {
    if (getOperator().isName("CHECK_SUM_V2") && operandCount() == 1) {
      final SqlNode parm = operand(0);
      if (parm instanceof SqlIdentifier) {
        SqlIdentifier id = (SqlIdentifier) parm;
        if (id.isStar() && id.names.size() == 1) {
          return true;
        }
      }
    }
    return false;
  }

  public SqlLiteral getFunctionQuantifier() {
    return null;
  }

  public String computeAttributesString() {return "";}
}

// End SqlCall.java
