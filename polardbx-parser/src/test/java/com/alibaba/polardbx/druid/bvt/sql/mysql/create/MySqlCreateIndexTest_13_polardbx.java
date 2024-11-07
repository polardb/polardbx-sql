package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import org.junit.Test;

import java.util.List;

/**
 * @author chenmo.cm
 */
public class MySqlCreateIndexTest_13_polardbx extends MysqlTest {

    @Test
    public void testOne() {
        String sql = "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) "
            + "PARTITION BY HASH(seller_id) PARTITIONS 16;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) PARTITION BY HASH (seller_id) PARTITIONS 16;",
            output);
    }

    @Test
    public void testTwo() {
        String sql = "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) "
            + "PARTITION BY HASH(SELLER_ID) PARTITIONS 16 "
            + "COMMENT 'CREATE CCI TEST';";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) PARTITION BY HASH (SELLER_ID) PARTITIONS 16 COMMENT 'CREATE CCI TEST';",
            output);
    }

    @Test
    public void testThree() {
        String sql = "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) "
            + "PARTITION BY HASH(SELLER_ID) PARTITIONS 16 "
            + "ENGINE='COLUMNAR' "
            + "USING BTREE KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' ALGORITHM=DEFAULT LOCK=DEFAULT;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) PARTITION BY HASH (SELLER_ID) PARTITIONS 16 ENGINE = 'COLUMNAR' USING BTREE KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' ALGORITHM = DEFAULT LOCK = DEFAULT;",
            output);
    }

    @Test
    public void testFour() {
        String sql =
            "/*DDL_ID=7125328353610956864*//*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/"
                + "CREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) "
                + "PARTITION BY HASH(SELLER_ID) PARTITIONS 16 "
                + "ENGINE='COLUMNAR' "
                + "USING BTREE KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' ALGORITHM=DEFAULT LOCK=DEFAULT;";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "/*DDL_ID=7125328353610956864*/\n/*+TDDL:CMD_EXTRA(SKIP_DDL_TASKS=\"WaitColumnarTableCreationTask\")*/\nCREATE CLUSTERED COLUMNAR INDEX `cc_i_seller` ON t_order (`seller_id`) PARTITION BY HASH (SELLER_ID) PARTITIONS 16 ENGINE = 'COLUMNAR' USING BTREE KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' ALGORITHM = DEFAULT LOCK = DEFAULT;",
            output);
    }

    @Test
    public void testFive() {
        String sql =
            "create clustered columnar index `nation_col_index` on nation(`n_nationkey`) "
                + "partition by hash(`n_nationkey`) partitions 1 "
                + "ENGINE='COLUMNAR' "
                + "USING BTREE KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' ALGORITHM=DEFAULT LOCK=DEFAULT "
                + "DICTIONARY_COLUMNS='n_name,n_comment';";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "CREATE CLUSTERED COLUMNAR INDEX `nation_col_index` ON nation (`n_nationkey`) PARTITION BY HASH (`n_nationkey`) PARTITIONS 1 ENGINE = 'COLUMNAR' USING BTREE DICTIONARY_COLUMNS = 'n_name,n_comment' KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' ALGORITHM = DEFAULT LOCK = DEFAULT  \n"
                + "COLUMNAR_OPTIONS='{\n"
                + "\t\"DICTIONARY_COLUMNS\":\"N_NAME,N_COMMENT\",\n"
                + "}';",
            output);
    }

    @Test
    public void testSix() {
        String sql =
            "create clustered columnar index `nation_col_index` on nation(`n_nationkey`) "
                + "partition by hash(`n_nationkey`) partitions 1 "
                + "ENGINE='COLUMNAR' "
                + "USING BTREE KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' ALGORITHM=DEFAULT LOCK=DEFAULT "
                + "DICTIONARY_COLUMNS='n_name,n_comment' "
                + "COLUMNAR_OPTIONS='{"
                + " \"TYPE\":\"SNAPSHOT\","
                + " \"SNAPSHOT_RETENTION_DAYS\":\"7\""
                + "}';";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "CREATE CLUSTERED COLUMNAR INDEX `nation_col_index` ON nation (`n_nationkey`) PARTITION BY HASH (`n_nationkey`) PARTITIONS 1 ENGINE = 'COLUMNAR' USING BTREE DICTIONARY_COLUMNS = 'n_name,n_comment' KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' ALGORITHM = DEFAULT LOCK = DEFAULT  \n"
                + "COLUMNAR_OPTIONS='{\n"
                + "\t\"DICTIONARY_COLUMNS\":\"N_NAME,N_COMMENT\",\n"
                + "\t\"TYPE\":\"SNAPSHOT\",\n"
                + "\t\"SNAPSHOT_RETENTION_DAYS\":\"7\",\n"
                + "}';",
            output);
    }

    @Test
    public void testSeven() {
        String sql =
            "create clustered columnar index `nation_col_index` on nation(`n_nationkey`) "
                + "partition by hash(`n_nationkey`) partitions 1 "
                + "ENGINE='COLUMNAR' "
                + "USING BTREE KEY_BLOCK_SIZE=20 COMMENT 'CREATE CCI TEST' ALGORITHM=DEFAULT LOCK=DEFAULT "
                + "COLUMNAR_OPTIONS='{"
                + " \"TYPE\":\"SNAPSHOT\","
                + " \"DICTIONARY_COLUMNS\":\"N_NAME,N_COMMENT\",\n"
                + " \"SNAPSHOT_RETENTION_DAYS\":\"7\""
                + "}';";

        List<SQLStatement> stmtList = SQLUtils.toStatementList(sql, JdbcConstants.MYSQL);

        SQLStatement stmt = stmtList.get(0);
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals(
            "CREATE CLUSTERED COLUMNAR INDEX `nation_col_index` ON nation (`n_nationkey`) PARTITION BY HASH (`n_nationkey`) PARTITIONS 1 ENGINE = 'COLUMNAR' USING BTREE KEY_BLOCK_SIZE = 20 COMMENT 'CREATE CCI TEST' ALGORITHM = DEFAULT LOCK = DEFAULT  \n"
                + "COLUMNAR_OPTIONS='{\n"
                + "\t\"DICTIONARY_COLUMNS\":\"N_NAME,N_COMMENT\",\n"
                + "\t\"TYPE\":\"SNAPSHOT\",\n"
                + "\t\"SNAPSHOT_RETENTION_DAYS\":\"7\",\n"
                + "}';",
            output);
    }
}
