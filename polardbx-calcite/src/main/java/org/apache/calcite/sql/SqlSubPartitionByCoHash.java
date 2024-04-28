package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author chenghui.lch
 */
public class SqlSubPartitionByCoHash extends SqlSubPartitionBy {

    public SqlSubPartitionByCoHash(SqlParserPos sqlParserPos) {
        super(sqlParserPos);
    }

}
