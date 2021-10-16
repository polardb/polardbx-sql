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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionInfoMockerTest {

    //@Testn
    public void testPartitionInfoMocker() {

        String db = "";
        String tb = "";
        PartitionInfo partitionInfo =
            PartitionInfoMocker.buildPartitionInfoByTableName(db, tb, null, null);
    }

    @Test
    public void testPartColExpr() {
        String partExpr = "`pk`,unix_timestamp(`gmt_modified`),year(`gmt_create`)";
        MySqlExprParser parser = new MySqlExprParser(partExpr);
        Lexer lexer = parser.getLexer();
        List<SQLExpr>  exprs = new ArrayList<>();
        List<SqlNode> partExprList = new ArrayList<>();
        while(true) {
            SQLExpr expr = parser.expr();
            exprs.add(expr);

            System.out.print(expr.toString());

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.EOF) {
                break;
            }
        }


        try{
            List<SqlPartitionValueItem> partAst = PartitionInfoUtil
                .buildPartitionExprByString("(`pk`,UNIX_TIMESTAMP(`gmt_modified`),YEAR(`gmt_create`))");

            SqlParser psr = SqlParser.create("('pk',UNIX_TIMESTAMP('gmt_modified'),YEAR('gmt_create'))");
            SqlNode  astNode =  psr.parseExpression();
            System.out.print(astNode);
        } catch (Throwable ex) {
            ex.printStackTrace();
        }


//        ContextParameters context = new ContextParameters(false);
//        SqlNode sqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context);
//        partExprList.add(sqlNodePartExpr);

        //System.out.print(exprs.size());
    }


    //@Test
    public void testPartitionExpr() {
        String partExpr = "`pk`,`name`,`gmt_create`";
        MySqlExprParser parser = new MySqlExprParser(partExpr);
        Lexer lexer = parser.getLexer();
        List<SQLExpr>  exprs = new ArrayList<>();
        List<SqlNode> partExprList = new ArrayList<>();
        while(true) {
            SQLExpr expr = parser.expr();
            exprs.add(expr);

            System.out.print(expr.toString());

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.EOF) {
                break;
            }
        }

//        ContextParameters context = new ContextParameters(false);
//        SqlNode sqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context);
//        partExprList.add(sqlNodePartExpr);

        //System.out.print(exprs.size());
    }

    //@Test
    public void testPartitionExprForList() {
        String partExpr = "(3,'a,','2020-12-12 01:00:00'),(3,',b','2020-12-12 02:00:00'),(3,',c','2020-12-12 03:00:00')";
        MySqlExprParser parser = new MySqlExprParser(partExpr);
        Lexer lexer = parser.getLexer();
        List<SQLExpr>  exprs = new ArrayList<>();
        List<SqlNode> partExprList = new ArrayList<>();
        while(true) {
            SQLExpr expr = parser.expr();
            exprs.add(expr);

            System.out.print(expr.toString());

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.EOF) {
                break;
            }
        }

        ContextParameters context = new ContextParameters(false);
        for (int i = 0; i < exprs.size(); i++) {
            SQLExpr expr = exprs.get(i);
            SqlNode sqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context, null);
            partExprList.add(sqlNodePartExpr);
        }

        System.out.print(exprs.size());
    }

    @Test
    public void testPartitionExprForList2() {
        String partExpr = "(3,'a,','2020-12-12 01:00:00')";
        MySqlExprParser parser = new MySqlExprParser(partExpr);
        Lexer lexer = parser.getLexer();
        List<SQLExpr>  exprs = new ArrayList<>();
        List<SqlNode> partExprList = new ArrayList<>();
        while(true) {
            SQLExpr expr = parser.expr();
            exprs.add(expr);

            System.out.print(expr.toString());

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.EOF) {
                break;
            }
        }

        ContextParameters context = new ContextParameters(false);
        for (int i = 0; i < exprs.size(); i++) {
            SQLExpr expr = exprs.get(i);
            SqlNode sqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context, null);
            partExprList.add(sqlNodePartExpr);
        }

        System.out.print(exprs.size());
    }

    @Test
    public void testPartitionExprForRangeBigintUnsigned() {
        String partExpr = "18446744073709551615";
        MySqlExprParser parser = new MySqlExprParser(partExpr);
        Lexer lexer = parser.getLexer();
        List<SQLExpr>  exprs = new ArrayList<>();
        List<SqlNode> partExprList = new ArrayList<>();
        while(true) {
            SQLExpr expr = parser.expr();
            exprs.add(expr);

            System.out.print(expr.toString());

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
            }

            if (lexer.token() == Token.EOF) {
                break;
            }
        }

        ContextParameters context = new ContextParameters(false);
        for (int i = 0; i < exprs.size(); i++) {
            SQLExpr expr = exprs.get(i);
            SqlNode sqlNodePartExpr = FastSqlConstructUtils.convertToSqlNode(expr, context, null);
            partExprList.add(sqlNodePartExpr);
        }

        System.out.print(exprs.size());
    }
}
