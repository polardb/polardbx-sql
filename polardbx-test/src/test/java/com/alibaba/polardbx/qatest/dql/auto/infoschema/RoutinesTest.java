package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.DEF_ROUTINE_CATALOG;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MOCK_CHARACTER_SET_CLIENT;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MOCK_COLLATION_CONNECTION;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.MOCK_DATABASE_COLLATION;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.PROCEDURE;
import static com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants.SQL;

@NotThreadSafe
public class RoutinesTest extends AutoReadBaseTestCase {
    private static final String MOCK_SCHEMA = "mock_schema";
    private static final String MOCK_PROCEDURE = "mock_procedure";

    static final String INSERT_PROCEDURE = String.format("INSERT INTO %s " +
            " (SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, "
            + "DATA_TYPE, ROUTINE_BODY, ROUTINE_DEFINITION, PARAMETER_STYLE, IS_DETERMINISTIC"
            + ", SQL_DATA_ACCESS, SECURITY_TYPE, CREATED, LAST_ALTERED, SQL_MODE, ROUTINE_COMMENT"
            + ", DEFINER, CHARACTER_SET_CLIENT, COLLATION_CONNECTION, DATABASE_COLLATION, ROUTINE_META) "
            + "VALUES (?, '%s', ?, ?, '%s', '' , '%s', ?, '%s', ?, ?, "
            + " ?, ?, ?, ?, ?, ?, '%s', '%s', '%s', ?);", GmsSystemTables.ROUTINES, DEF_ROUTINE_CATALOG,
        PROCEDURE, SQL, SQL, MOCK_CHARACTER_SET_CLIENT, MOCK_COLLATION_CONNECTION, MOCK_DATABASE_COLLATION);

    static final String DROP_PROCEDURE =
        String.format("DELETE FROM %s WHERE ROUTINE_NAME = ? AND ROUTINE_SCHEMA = ? AND ROUTINE_TYPE = '%s'",
            GmsSystemTables.ROUTINES, PROCEDURE);

    @Before
    public void prepareBadProcedure() throws SQLException {
        clearProcedure();
        try (Connection metaDbCon = getMetaConnection()) {
            Map<Integer, ParameterContext> params = new HashMap<>();
            String currentTime = PLUtils.getCurrentTime();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, MOCK_PROCEDURE);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, MOCK_SCHEMA);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, MOCK_PROCEDURE);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString,
                String.format("create procedure %s begin select 1;", MOCK_PROCEDURE));
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, "YES");
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, "no sql");
            MetaDbUtil.setParameter(7, params, ParameterMethod.setString,
                "definer");
            MetaDbUtil.setParameter(8, params, ParameterMethod.setString, currentTime);
            MetaDbUtil.setParameter(9, params, ParameterMethod.setString, currentTime);
            MetaDbUtil.setParameter(10, params, ParameterMethod.setString, PlConstants.MOCK_SQL_MODE);
            MetaDbUtil.setParameter(11, params, ParameterMethod.setString, "");
            MetaDbUtil.setParameter(12, params, ParameterMethod.setString, "");
            MetaDbUtil.setParameter(13, params, ParameterMethod.setString, "");
            MetaDbUtil.insert(INSERT_PROCEDURE, params, metaDbCon);
        }
    }

    @Test
    public void informationSchemaRoutines() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            String sql = "select * from information_schema.routines";
            JdbcUtil.executeFaied(connection, sql, "syntax error");
            JdbcUtil.executeSuccess(connection, "/*+TDDL: ORIGIN_CONTENT_IN_ROUTINES = true*/" + sql);
        }
    }

    @Test
    public void informationSchemaCheckRoutines() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            String sql = "select * from information_schema.check_routines";
            ResultSet rs = JdbcUtil.executeQuery(sql, connection);
            while (rs.next()) {
                if (rs.getString("ROUTINE_SCHEMA").equalsIgnoreCase(MOCK_SCHEMA)) {
                    rs.getString("PARSE_RESULT").contains("failed");
                } else {
                    rs.getString("PARSE_RESULT").contains("success");
                }
            }
        }
    }

    @After
    public void clearProcedure() throws SQLException {
        try (Connection metaDbCon = getMetaConnection()) {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, MOCK_PROCEDURE);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, MOCK_SCHEMA);
            MetaDbUtil.insert(DROP_PROCEDURE, params, metaDbCon);
        }
    }
}
