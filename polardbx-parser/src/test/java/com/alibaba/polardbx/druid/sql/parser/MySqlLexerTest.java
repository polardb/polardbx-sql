package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author yudong
 * @since 2023/5/11 15:54
 **/
public class MySqlLexerTest {
    @Test
    public void testQueryEvent() {
        String queryEvent =
            "# POLARX_ORIGIN_SQL=CREATE TABLE test ( id int, _drds_implicit_id_ bigint AUTO_INCREMENT, PRIMARY KEY (_drds_implicit_id_) )\n"
                + "# POLARX_TSO=706157053851834784015926357519950192640000000000000000\n"
                + "CREATE TABLE test ( id int ) DEFAULT CHARACTER SET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci;";
        MySqlLexer lexer = new MySqlLexer(ByteString.from(queryEvent));
        assertEquals(Token.CREATE, lexer.getNextToken());
        assertEquals(Token.TABLE, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.LPAREN, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.COMMA, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.COMMA, lexer.getNextToken());
        assertEquals(Token.PRIMARY, lexer.getNextToken());
        assertEquals(Token.KEY, lexer.getNextToken());
        assertEquals(Token.LPAREN, lexer.getNextToken());
        assertEquals(Token.IDENTIFIER, lexer.getNextToken());
        assertEquals(Token.RPAREN, lexer.getNextToken());
        assertEquals(Token.RPAREN, lexer.getNextToken());
        assertEquals(Token.EOF, lexer.getNextToken());
    }

}
