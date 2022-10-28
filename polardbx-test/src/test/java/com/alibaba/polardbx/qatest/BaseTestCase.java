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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.Lexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.oss.FileStorageTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;

import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.getConnectionProperties;

@RunWith(CommonCaseRunner.class)
public class BaseTestCase implements BaseTestMode {
    private static final Log log = LogFactory.getLog(BaseTestCase.class);

    protected static final String PK_COLUMN_NAME = "pk";

    private static Cache<Engine, Object> engines = CacheBuilder.newBuilder().build();

    private static ImmutableList<Class> fileStorageTestCases;

    public static final String PACKAGE_NAME = "com.alibaba.polardbx.qatest";

    static {
        // Scan all class in this package and register the unit case on file storage.
        ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {
            @Override
            public boolean filter(Class klass) {
                int mod = klass.getModifiers();
                return !Modifier.isAbstract(mod)
                    && !Modifier.isInterface(mod)
                    && BaseTestCase.class.isAssignableFrom(klass)
                    && (klass.getAnnotation(TestFileStorage.class) != null
                    || AutoReadBaseTestCase.class.isAssignableFrom(klass));
            }

            @Override
            public boolean preFilter(String classFulName) {
                return classFulName.endsWith("Test");
            }
        };

        List<Class> filtered = ClassFinder.findClassesInPackage(PACKAGE_NAME, filter);
        fileStorageTestCases = ImmutableList.<Class>builder().addAll(filtered).build();
    }

    private List<ConnectionWrap> polardbxConnections = new ArrayList<>();
    private List<ConnectionWrap> mysqlConnections = new ArrayList<>();
    private List<ConnectionWrap> mysqlConnectionsSecond = new ArrayList<>();
    private List<ConnectionWrap> metaDBConnections = new ArrayList<>();
    protected String hint;

    public BaseTestCase() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
    }

    @Before
    public void initializeFileStorage() {
        boolean useFileStorageMode = PropertiesUtil.useFileStorage()
            && usingNewPartDb()
            && ClassHelper.getFileStorageTestCases().contains(getClass());
        if (!useFileStorageMode) {
            return;
        }

        // Check the existence of archive database && archive data if necessary
        if (!checkInit()) {
            initArchiveDB();
        }
    }

    private void initArchiveDB() {
        Engine engine = PropertiesUtil.engine();
        try {
            engines.get(engine, () -> prepareArchivedData());
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private boolean checkInit() {
        boolean db1Archived;
        boolean db2Archived;
        String fileStorageDB1 = PropertiesUtil.polardbXAutoDBName1();
        String fileStorageDB2 = PropertiesUtil.polardbXAutoDBName2();

        String sourceDB1 = PropertiesUtil.polardbXAutoDBName1Innodb();
        String sourceDB2 = PropertiesUtil.polardbXAutoDBName2Innodb();
        try (Connection conn = getPolardbxConnection(sourceDB1)) {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(
                "SELECT SCHEMA_NAME   FROM INFORMATION_SCHEMA.SCHEMATA  WHERE SCHEMA_NAME = '" + fileStorageDB1 + "'");
            db1Archived = rs.next();
            rs = statement.executeQuery(
                "SELECT SCHEMA_NAME   FROM INFORMATION_SCHEMA.SCHEMATA  WHERE SCHEMA_NAME = '" + fileStorageDB2 + "'");
            db2Archived = rs.next();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return db1Archived && db2Archived;
    }

    private Object prepareArchivedData() {
        Engine engine = PropertiesUtil.engine();

        if (engine == Engine.LOCAL_DISK) {
            try (Connection connection = getMetaConnection()) {
                FileStorageTest.initLocalDisk(connection);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        String fileStorageDB1 = PropertiesUtil.polardbXAutoDBName1();
        String fileStorageDB2 = PropertiesUtil.polardbXAutoDBName2();

        String sourceDB1 = PropertiesUtil.polardbXAutoDBName1Innodb();
        String sourceDB2 = PropertiesUtil.polardbXAutoDBName2Innodb();

        // check file storage
        try (Connection conn = getPolardbxConnection(sourceDB1)) {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(String.format("show filestorage;"));

            boolean hasEngine = false;
            while (rs.next()) {
                String engineName = rs.getString(1);
                if (PropertiesUtil.engine().name().equalsIgnoreCase(engineName)) {
                    hasEngine = true;
                    break;
                }
            }

            String createFileStorageSql = PropertiesUtil.getCreateFileStorageSql();
            if (!hasEngine
                && createFileStorageSql != null
                && !createFileStorageSql.isEmpty()) {
                statement.execute(createFileStorageSql);
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        List<String> archiveSqlList1 = new ArrayList<>();
        try (Connection conn = getPolardbxConnection(sourceDB1)) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", fileStorageDB1));
            statement.execute(String.format("create database %s mode = 'partitioning'", fileStorageDB1));

            ResultSet rs = statement.executeQuery(
                "select table_name from information_schema.tables where table_schema = '" + sourceDB1 + "'");
            while (rs.next()) {
                String tableName = rs.getString(1);
                String archivedSql = String.format("create table %s like %s.%s engine = '%s' archive_mode = 'loading'",
                    tableName, sourceDB1, tableName, engine.name());
                archiveSqlList1.add(archivedSql);
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        archiveSqlList1.forEach(System.out::println);

        List<String> archiveSqlList2 = new ArrayList<>();
        try (Connection conn = getPolardbxConnection(sourceDB2)) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", fileStorageDB2));
            statement.execute(String.format("create database %s mode = 'partitioning'", fileStorageDB2));

            ResultSet rs = statement.executeQuery(
                "select table_name from information_schema.tables where table_schema = '" + sourceDB2 + "'");
            while (rs.next()) {
                String tableName = rs.getString(1);
                String archivedSql = String.format("create table %s like %s.%s engine = '%s' archive_mode = 'loading'",
                    tableName, sourceDB2, tableName, engine.name());
                archiveSqlList2.add(archivedSql);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        archiveSqlList2.forEach(System.out::println);

        // create all tables on file storage db1
        try (Connection conn = getPolardbxConnection(fileStorageDB1)) {
            Statement statement = conn.createStatement();
            for (String archiveSql : archiveSqlList1) {
                statement.execute(archiveSql);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        // create all tables on file storage db2
        try (Connection conn = getPolardbxConnection(fileStorageDB2)) {
            Statement statement = conn.createStatement();
            for (String archiveSql : archiveSqlList2) {
                statement.execute(archiveSql);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        return new Object();
    }

    @Before
    public void beforeBaseTestCase() {
        this.polardbxConnections = new ArrayList<>();
        this.mysqlConnections = new ArrayList<>();
        this.mysqlConnectionsSecond = new ArrayList<>();
        this.metaDBConnections = new ArrayList<>();
    }

    public synchronized Connection getPolardbxConnection() {
        return getPolardbxConnection(PropertiesUtil.polardbXDBName1(usingNewPartDb()));
    }

    public synchronized Connection getPolardbxConnection(String db) {
        try {
            Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.polardbxConnections.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getPolardbxMode(), connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("get PolardbxConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getPolardbxConnection2() {
        return getPolardbxConnection(PropertiesUtil.polardbXDBName2(usingNewPartDb()));
    }

    public synchronized Connection getPolardbxDirectConnection() {
        return getPolardbxDirectConnection(PropertiesUtil.polardbXDBName1(usingNewPartDb()));
    }

    public synchronized Connection getPolardbxDirectConnection(String db) {
        try {
            Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.polardbxConnections.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getPolardbxMode(), connectionWrap);
            return connectionWrap;
        } catch (Throwable t) {
            log.error("get PolardbxDirectConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getPolardbxConnectionWithExtraParams(String extraParams) {
        return getPolardbxDirectConnection(PropertiesUtil.polardbXDBName1(usingNewPartDb()), extraParams);
    }

    public synchronized Connection getPolardbxDirectConnection(String db, String extraParams) {
        try {
            Connection connection = ConnectionManager.getInstance().newPolarDBXConnectionWithExtraParams(extraParams);
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.polardbxConnections.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getPolardbxMode(), connectionWrap);
            return connectionWrap;
        } catch (Throwable t) {
            log.error("get PolardbxDirectConnection with extra params error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getMysqlConnection() {
        return getMysqlConnection(PropertiesUtil.mysqlDBName1());
    }

    public synchronized Connection getMysqlConnection(String db) {
        try {
            Connection connection = ConnectionManager.getInstance().getDruidMysqlConnection();
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.mysqlConnections.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getMysqlMode(), connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("get MysqlConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getMysqlConnectionSecond() {
        return getMysqlConnectionSecond(PropertiesUtil.mysqlDBName1());
    }

    public synchronized Connection getMysqlConnectionSecond(String db) {
        try {
            Connection connection = ConnectionManager.getInstance().getDruidMysqlConnectionSecond();
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.mysqlConnectionsSecond.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getMysqlMode(), connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("get MysqlConnectionSecond error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getMysqlConnectionByAddress(Set<String> fullAddress) {
        return getMysqlConnectionByAddress(fullAddress, PropertiesUtil.mysqlDBName1());
    }

    public synchronized Connection getMysqlConnectionByAddress(Set<String> fullAddress, String db) {
        String mysqlFullAddress = String.format("%s:%s", ConnectionManager.getInstance().getMysqlAddress(),
            ConnectionManager.getInstance().getMysqlPort());
        String mysqlFullAddressSecond = String.format("%s:%s", ConnectionManager.getInstance().getMysqlAddressSecond(),
            ConnectionManager.getInstance().getMysqlPortSecond());

        if (fullAddress.contains(mysqlFullAddress)) {
            return getMysqlConnection(db);
        } else if (fullAddress.contains(mysqlFullAddressSecond)) {
            return getMysqlConnectionSecond(db);
        } else {
            throw new RuntimeException(
                "fullAddress mismatched : " + fullAddress + "; Mysql address is not dn address. Can't create db[" + db
                    + "] connection");
        }
    }

    public synchronized Connection getMysqlDirectConnection() {
        return getMysqlDirectConnection(PropertiesUtil.mysqlDBName1());
    }

    public synchronized Connection getMysqlDirectConnection(String db) {
        try {
            Connection connection = ConnectionManager.getInstance().newMysqlConnection();
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.mysqlConnections.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getMysqlMode(), connectionWrap);
            return connectionWrap;
        } catch (Throwable t) {
            log.error("get MysqlDirectConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getMysqlConnectionWithExtraParams(String extraParams) {
        return getMysqlDirectConnection(PropertiesUtil.mysqlDBName1(), extraParams);
    }

    public synchronized Connection getMysqlDirectConnection(String db, String extraParams) {
        try {
            Connection connection = ConnectionManager.getInstance().newMysqlConnectionWithExtraParams(extraParams);
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.mysqlConnections.add(connectionWrap);
            useDb(connectionWrap, db);
            setSqlMode(ConnectionManager.getInstance().getMysqlMode(), connectionWrap);
            return connectionWrap;
        } catch (Throwable t) {
            log.error("get MysqlDirectConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getMetaConnection() {
        try {
            Connection connection = ConnectionManager.getInstance().getDruidMetaConnection();
            JdbcUtil.useDb(connection, PropertiesUtil.getMetaDB);
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            this.metaDBConnections.add(connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("getPolardbxConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getPolardbxDirectConnection(
        String server, String user, String password, String polardbxPort) {
        try {
            String url = String.format(
                ConfigConstant.URL_PATTERN + getConnectionProperties(), server, polardbxPort);
            Properties prop = new Properties();
            prop.setProperty("user", user);
            if (password != null) {
                prop.setProperty("password", password);
            }
            prop.setProperty("allowMultiQueries", String.valueOf(true));
            ConnectionWrap connectionWrap = new ConnectionWrap(DriverManager.getConnection(url, prop));
            this.polardbxConnections.add(connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("get PolardbxConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public synchronized Connection getPolardbxDirectConnection(
        String server, String user, String dbName, String password, String polardbxPort) {
        try {
            String url = String.format(
                ConfigConstant.URL_PATTERN_WITH_DB + getConnectionProperties(), server, polardbxPort, dbName);
            Properties prop = new Properties();
            prop.setProperty("user", user);
            if (password != null) {
                prop.setProperty("password", password);
            }
            prop.setProperty("allowMultiQueries", String.valueOf(true));
            ConnectionWrap connectionWrap = new ConnectionWrap(DriverManager.getConnection(url, prop));
            this.polardbxConnections.add(connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("get PolardbxConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    @After
    public void afterBaseTestCase() {
        Throwable throwable = null;
        for (ConnectionWrap connection : polardbxConnections) {
            if (!connection.isClosed()) {
                //确保所有连接都被正常关闭
                try {
                    //保险起见, 主动rollback
                    if (!connection.getAutoCommit()) {
                        connection.rollback();
                    }
                    connection.close();
                } catch (Throwable t) {
                    log.error("close the Connection!", t);
                    if (!"connection disabled".contains(t.getMessage())) {
                        if (throwable == null) {
                            throwable = t;
                        }
                    }
                }
            }
        }

        for (ConnectionWrap connection : mysqlConnections) {
            if (!connection.isClosed()) {
                //确保所有连接都被正常关闭
                try {
                    connection.close();
                } catch (Throwable t) {
                    log.error("close the Connection!", t);
                    if (throwable == null) {
                        throwable = t;
                    }
                }
            }
        }

        for (ConnectionWrap connection : mysqlConnectionsSecond) {
            if (!connection.isClosed()) {
                //确保所有连接都被正常关闭
                try {
                    connection.close();
                } catch (Throwable t) {
                    log.error("close the Connection!", t);
                    if (throwable == null) {
                        throwable = t;
                    }
                }
            }
        }

        for (ConnectionWrap connection : metaDBConnections) {
            if (!connection.isClosed()) {
                //确保所有连接都被正常关闭
                try {
                    connection.close();
                } catch (Throwable t) {
                    log.error("close the Connection!", t);
                    if (throwable == null) {
                        throwable = t;
                    }
                }
            }
        }

        if (throwable != null) {
            Assert.fail(throwable.getMessage());
        }
    }

    public List<List<String>> getTrace(Connection tddlConnection) {
        final ResultSet rs = JdbcUtil.executeQuery("show trace", tddlConnection);
        return JdbcUtil.getStringResult(rs, false);
    }

    public void setSqlMode(String mode, Connection conn) {
        String sql = "SET session sql_mode = '" + mode + "'";
        JdbcUtil.updateDataTddl(conn, sql, null);
    }

    public static boolean isMySQL80() {
        return PropertiesUtil.polardbXVersion().equalsIgnoreCase("8.0")
            || PropertiesUtil.polardbXVersion().equalsIgnoreCase("galaxy");
    }

    public static boolean isGalaxy() {
        return PropertiesUtil.polardbXVersion().equalsIgnoreCase("galaxy");
    }

    public int getExplainNum(Connection tddlConnection, String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql);
        try {
            final List<String> columnNameListToLowerCase = JdbcUtil.getColumnNameListToLowerCase(rs);
            if (columnNameListToLowerCase.contains("count")) {
                rs.next();
                return rs.getInt("COUNT");
            }
            return JdbcUtil.resultsSize(rs);
        } catch (Exception e) {
            Assert.fail("explain exception:explain " + sql + ", messsage is " + e.getMessage());
            return -1;
        } finally {
            JdbcUtil.close(rs);
        }

    }

    public String getExplainResult(Connection tddlConnection, String sql) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql);
        try {
            return JdbcUtil.resultsStr(rs);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    protected void checkPhySqlId(List<List<String>> trace) {
        final Map<String, List<Token>> phySqlGroups = new HashMap<>();
        for (List<String> traceRow : trace) {
            final String phySql = traceRow.get(11);
            final MySqlExprParser exprParser = new MySqlExprParser(ByteString.from(phySql), true);
            final Lexer lexer = exprParser.getLexer();
            final String headerHint = lexer.getComments().get(0);

            final EnumSet<Token> acceptToken =
                EnumSet.of(Token.SELECT, Token.INSERT, Token.REPLACE, Token.UPDATE, Token.DELETE);
            Token token = lexer.token();
            while (!acceptToken.contains(token)) {
                lexer.nextToken();
                token = lexer.token();
            }

            final String[] splited = StringUtils.split(headerHint, "/");
            Assert.assertEquals("Unexpected header hint for physical sql: " + headerHint, 6, splited.length);
            final String phySqlId = splited[3];

            phySqlGroups.computeIfAbsent(phySqlId, (k) -> new ArrayList<>()).add(token);
        }

        for (Map.Entry<String, List<Token>> entry : phySqlGroups.entrySet()) {
            final String phySqlId = entry.getKey();
            final List<Token> phySqlTypes = entry.getValue();

            final Token sqlType = phySqlTypes.get(0);
            for (Token phySqlType : phySqlTypes) {
                Assert.assertEquals("Different physical type with same phySqlId: " + phySqlId, sqlType, phySqlType);
            }
        }
    }

    protected static void checkPhySqlOrder(List<List<String>> trace) {
        int currentPhySqlId = 0;
        for (List<String> traceRow : trace) {
            final String phySql = traceRow.get(11);
            final MySqlExprParser exprParser = new MySqlExprParser(ByteString.from(phySql), true);
            final Lexer lexer = exprParser.getLexer();
            final String headerHint = lexer.getComments().get(0);

            final EnumSet<Token> acceptToken =
                EnumSet.of(Token.SELECT, Token.INSERT, Token.REPLACE, Token.UPDATE, Token.DELETE);
            Token token = lexer.token();
            while (!acceptToken.contains(token)) {
                lexer.nextToken();
                token = lexer.token();
            }

            final String[] splited = StringUtils.split(headerHint, "/");
            Assert.assertEquals("Unexpected header hint for physical sql: " + headerHint, 6, splited.length);
            final int phySqlId = Integer.valueOf(splited[3]);
            Assert.assertTrue(currentPhySqlId <= phySqlId);
            currentPhySqlId = phySqlId;
        }
    }

    public void useDb(Connection connection, String db) {
        JdbcUtil.executeQuery("use " + db, connection);
    }

    protected static String randomTableName(String prefix, int suffixLength) {
        String suffix = RandomStringUtils.randomAlphanumeric(suffixLength).toLowerCase();
        return String.format("%s_%s", prefix, suffix);
    }

    public static String buildInsertColumnsSQLWithTableName(List<ColumnEntity> columns, String tableName) {
        StringBuilder insert = new StringBuilder("INSERT INTO").append(" ").append(tableName).append(" (");
        StringBuilder values = new StringBuilder(" VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i).getName();
            if (i > 0) {
                insert.append(",");
                values.append(",");
            }
            insert.append(columnName);
            values.append("?");
        }

        insert.append(")");
        values.append(")");

        return insert.append(values).toString();
    }
}
