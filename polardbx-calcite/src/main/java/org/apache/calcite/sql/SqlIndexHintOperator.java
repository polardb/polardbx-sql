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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.Objects;

/**
 *
 * A SqlInterval operator.
 *
 * @author hongxi.chx
 */
public class SqlIndexHintOperator extends SqlSpecialOperator {
  public static SqlIndexHintOperator INDEX_OPERATOR = new SqlIndexHintOperator("SELECT INDEX",
          SqlKind.SELECT_INDEX,
          0);
  //~ Constructors -----------------------------------------------------------

  public SqlIndexHintOperator(
      String name,
      SqlKind kind,
      int prec
      ) {
    super(name, kind, prec);
  }

  public SqlCall createCall(
          SqlCharStringLiteral node0,
          SqlCharStringLiteral node1,
          SqlNodeList operands,
          SqlParserPos pos) {
    assert node0 != null;
    return new SqlIndexHint(node0 , node1 ,operands ,pos);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public String getSignatureTemplate(final int operandsCount) {
    return "{0}{1}{2}";
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlMonotonicity.CONSTANT;
  }

  @Override public boolean validRexOperands(int count, Litmus litmus) {
    return litmus.succeed();
  }

  public void accept(RexVisitor visitor, RexCall call) {
    visitor.visit(this, call);
  }

  @Override
  public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
    assert operands.length == 3;
    return this.createCall((SqlCharStringLiteral)operands[0],(SqlCharStringLiteral)operands[1],(SqlNodeList)operands[2],pos);
  }



  @Override
  public void unparse(
          SqlWriter writer,
          SqlCall call,
          int leftPrec,
          int rightPrec) {
    assert call instanceof SqlIndexHint;
    assert call.operandCount() >= 2;
    final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    SqlIndexHint sqlIndexHintNode = (SqlIndexHint)call;
    List<SqlNode> operandList = sqlIndexHintNode.getOperandList();
    SqlCharStringLiteral node0 = (SqlCharStringLiteral)operandList.get(0);
    SqlCharStringLiteral node1 = (SqlCharStringLiteral)operandList.get(1);
    SqlNodeList sqlNodeList = (SqlNodeList)operandList.get(2);
    String value0 = node0.getNlsString().getValue();
    writer.sep(value0);
    if (node1 != null) {
      writer.sep("FOR");
      String value1 = node1.getNlsString().getValue();
      writer.sep(value1);
    }
    if (call.operandCount() > 2) {
      SqlWriter.Frame frameFunc = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      for (int i = 0; i < sqlNodeList.size(); i++) {
        SqlNode node = sqlNodeList.get(i);
        if (node instanceof SqlCharStringLiteral){
          SqlCharStringLiteral sqlNode = (SqlCharStringLiteral)node;
          String value2 = sqlNode.getNlsString().getValue();
          writer.sep(value2);
        } else if (node instanceof SqlIdentifier){
          SqlIdentifier sqlNode = (SqlIdentifier)node;
          String value2 = sqlNode.getLastName();
          writer.sep(value2);
        } else {
          throw new IllegalArgumentException("not support sql hint:" + node);
        }
        if(Objects.equals(value0, "FORCE INDEX")){
          // force index only support one index
          break;
        }
        if(Objects.equals(value0, "FORCE PAGING_INDEX")){
          // force index only support one index
          break;
        }
        if (i != sqlNodeList.size() - 1) {
          writer.sep(",");
        }
      }
      writer.endList(frameFunc);
    }
    writer.endList(frame);
  }

}

// End SqlPrefixOperator.java
