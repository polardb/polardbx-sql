package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OrToInTest {

    /**
     * test balanced tree
     */
    @Test
    public void testFindBucket() {
        int[] sizes = new int[] {3, 100, 10000, 100000};
        for (int size : sizes) {
            List<SqlNode> nodes = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                nodes.add(new SqlDynamicParam(0, SqlParserPos.ZERO));
            }
            SqlNode result = Util.SqlOperatorListToTree(SqlStdOperatorTable.OR, nodes, SqlParserPos.ZERO);

            Checker checker = new Checker();
            dfs(result, 0, checker);
            Assert.assertTrue(checker.child == size, "tree lost some children");
            Assert.assertTrue(size > Math.pow(2, checker.maxDepth - 1) && size <= Math.pow(2, checker.maxDepth),
                "not balanced");
        }
    }

    void dfs(SqlNode tree, int depth, Checker checker) {
        if (depth > checker.maxDepth) {
            checker.maxDepth = depth;
        }
        if (Util.isOrSqlNode(tree)) {
            SqlBasicCall call = (SqlBasicCall) tree;
            Assert.assertTrue(call.getOperands().length == 2, "or has two children");
            dfs(call.operand(0), depth + 1, checker);
            dfs(call.operand(1), depth + 1, checker);
            return;
        }

        checker.child++;
    }

    class Checker {
        int maxDepth;
        int child;

        public Checker() {
            this.maxDepth = 0;
            this.child = 0;
        }
    }

    private class MergePolicy {
        boolean first;
        double left;

        public MergePolicy(boolean first, double left) {
            this.first = first;
            this.left = left;
        }

        boolean chooseFirst() {
            return first;
        }

        boolean chooseLeft(Random r1) {
            return r1.nextDouble() < left;
        }
    }

    @Test
    public void testConcatOr() {
        ContextParameters context = new ContextParameters(false);
        ExecutionContext ec = new ExecutionContext();
        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);

        Random r1 = new Random();

        MergePolicy leftDepth = new MergePolicy(true, 1);
        MergePolicy rightDepth = new MergePolicy(true, 0);
        MergePolicy random = new MergePolicy(false, 0.5);

        MergePolicy[] policies = new MergePolicy[] {leftDepth, rightDepth, random};

        int[] sizes = new int[] {3, 100, 10000, 100000};
        // test left-deep, random and right-deep tree
        for (int size : sizes) {
            for (MergePolicy policy : policies) {
                SQLExpr[] exprs = new SQLExpr[size];
                for (int i = 0; i < size; i++) {
                    SQLVariantRefExpr var = new SQLVariantRefExpr("?");
                    var.setIndex(i);
                    exprs[i] = new SQLBinaryOpExpr(new SQLIdentifierExpr("x"), SQLBinaryOperator.Equality, var);
                }

                for (int i = size - 1; i > 0; i--) {
                    int loc = policy.chooseFirst() ? 0 : r1.nextInt(i);
                    SQLExpr left = exprs[loc];
                    SQLExpr right = exprs[i];
                    exprs[loc] = policy.chooseLeft(r1) ?
                        new SQLBinaryOpExpr(left, SQLBinaryOperator.BooleanOr, right) :
                        new SQLBinaryOpExpr(right, SQLBinaryOperator.BooleanOr, left);
                }
                visitor.concatOr((SQLBinaryOpExpr) exprs[0]);
                SqlNode result = Util.OrSqlNodeOpt(visitor.getSqlNode());

                SqlBasicCall call = (SqlBasicCall) result;
                Assert.assertTrue(call.getOperands().length == 2);
                Assert.assertTrue(call.getOperandList().get(1) instanceof SqlNodeList);
                // check in value
                SqlNodeList nodeList = (SqlNodeList) call.getOperandList().get(1);
                Assert.assertTrue(nodeList.size() == size);
            }
        }
    }
}
