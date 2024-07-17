package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MySqlCreateTableTest165_polardbx_cci extends MysqlTest {

    // Constraint XXX may not show in parsed sql, so removed it.
    private static final String CREATE_TABLE_BASE = "/*DDL_ID=7125328353610956864*/\n"
        + "/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/\n"
        + "CREATE TABLE IF NOT EXISTS `full_type_table` (\n"
        + "\tpk INT NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
        + "\tid1 INT,\n"
        + "\tid2 INT,\n"
        + "\tid3 VARCHAR(100),\n"
        + "\tvc1 VARCHAR(100),\n"
        + "\tvc3 VARCHAR(100),\n"
        + "\tINDEX idx1 USING HASH(id1),\n"
        + "\tKEY idx2 USING HASH (id2),\n"
        + "\tFULLTEXT KEY idx4 (id3(20)),\n"
        + "\tUNIQUE idx3 USING BTREE (vc1(20))";
    private static final String CREATE_TABLE_TAIL = "\n) ENGINE = INNODB AUTO_INCREMENT = 2 AVG_ROW_LENGTH = 100 "
        + "DEFAULT CHARACTER SET = utf8 DEFAULT COLLATE = utf8_bin CHECKSUM = 0 COMMENT 'abcd'\n"
        + "PARTITION BY HASH(id1);";

    private static final String FULL_TYPE_TABLE = CREATE_TABLE_BASE + CREATE_TABLE_TAIL;

    private static final List<String> CCI_DEFINITIONS = new ArrayList<String>();

    private static final List<String> CCI_DEF_HEAD = new ArrayList<String>();
    private static final List<String> CCI_DEF_COLUMN_DB = new ArrayList<String>();
    private static final List<String> CCI_DEF_SHARDING_DB = new ArrayList<String>();
    private static final List<String> CCI_DEF_INDEX_OPTION = new ArrayList<String>();

    static {

        CCI_DEF_HEAD.add("CLUSTERED COLUMNAR INDEX");

        CCI_DEF_COLUMN_DB.add("cci_id2(id2)");
        CCI_DEF_COLUMN_DB.add("cci_id2(id2) COVERING (vc1)");
        CCI_DEF_COLUMN_DB.add("cci_id2 USING BTREE (id2) COVERING (vc1, vc2)");

        CCI_DEF_SHARDING_DB.add("PARTITION BY HASH(id2)");
        CCI_DEF_SHARDING_DB.add("PARTITION BY HASH(id2) PARTITIONS 16");

        CCI_DEF_INDEX_OPTION.add("");
        CCI_DEF_INDEX_OPTION.add("ENGINE='COLUMNAR' KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' INVISIBLE");

        for (String head : CCI_DEF_HEAD) {
            for (String option : CCI_DEF_INDEX_OPTION) {
                for (String column : CCI_DEF_COLUMN_DB) {
                    buildGsiDef(head, option, column, CCI_DEF_SHARDING_DB);
                }
            }
        }
    }

    private static void buildGsiDef(String head, String option, String column, List<String> gsiDefShardingDb) {
        for (String sharding : gsiDefShardingDb) {
            CCI_DEFINITIONS.add("\t" + head + " " + column + " " + sharding + (StringUtils.isBlank(option) ? "" : " ")
                + option);
        }
    }

    private static void checkExplain(final String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());
        String a = StringUtils.replace(sql, " ", "");
        String b = StringUtils.replace(stmt.toString(), " ", "");
        assertEquals(a, b);
    }

    @Test
    public void test() {
        List<Throwable> errors = new ArrayList<Throwable>();
        List<String> errorGsiDefs = new ArrayList<String>();
        for (String gsiDef : CCI_DEFINITIONS) {
            final StringBuilder sqlBuilder = new StringBuilder(CREATE_TABLE_BASE);

            sqlBuilder.append(",\n").append(gsiDef);

            sqlBuilder.append(CREATE_TABLE_TAIL);
            try {
                checkExplain(sqlBuilder.toString());
            } catch (Throwable e) {
                // System.out.println(e.getMessage());
                System.out.println(sqlBuilder.toString());
                e.printStackTrace();
                errors.add(e);
                errorGsiDefs.add(gsiDef);
            }
        }

        if (errors.size() > 0) {
            for (String e : errorGsiDefs) {
                System.out.println(e);
            }
            Assert.fail(errors.size() + " out of " + CCI_DEFINITIONS.size() + " CREATE TABLE statement failed");
        } else {
            System.out.println(CCI_DEFINITIONS.size() + " CREATE TABLE statement success!");
        }
    }

}
