package org.apache.calcite.sql;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class SqlCheckColumnarIndexTest {
    @Test
    public void testNormalCase() {
        SqlParserPos pos = SqlParserPos.ZERO;
        String indexNameStr = "test_index";
        SqlIdentifier indexName = new SqlIdentifier(indexNameStr, pos);
        String tableNameStr = "test_table";
        SqlIdentifier tableName = new SqlIdentifier(tableNameStr, pos);
        String extraCmd = "increment";
        List<Long> extras = new ArrayList<>();
        extras.add(100L);
        extras.add(200L);

        SqlCheckColumnarIndex sqlCheckColumnarIndex = new SqlCheckColumnarIndex(pos, indexName, tableName, extraCmd, extras);

        assertEquals(indexNameStr, sqlCheckColumnarIndex.getIndexName().toString());
        assertEquals(tableNameStr, sqlCheckColumnarIndex.getTableName().toString());
        assertEquals(extraCmd, sqlCheckColumnarIndex.getExtraCmd());
        assertEquals(extras, sqlCheckColumnarIndex.getExtras());

        SqlValidator validator = mock(SqlValidator.class);
        SqlValidatorScope scope = mock(SqlValidatorScope.class);
        sqlCheckColumnarIndex.validate(validator, scope);

        SqlPrettyWriter writer = new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
        sqlCheckColumnarIndex.unparse(writer, 0, 0);
        assertEquals("CHECK COLUMNAR INDEX \"test_index\"\nON \"test_table\"\nINCREMENT", writer.toString());
    }
}
