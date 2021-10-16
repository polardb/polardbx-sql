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

package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * @author mengshi.sunmengshi 2013-12-6 下午3:38:38
 * @since 5.0.0
 */
public class TDatabaseMetaData implements DatabaseMetaData {

    private TDataSource dataSource = null;

    public TDatabaseMetaData(TDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DatabaseMetaData getDatabaseMetaData() throws SQLException {
        Connection conn = null;
        DatabaseMetaData dbMa = null;
        try {
            // 找到默认的主库
            String defaultDbIndex = this.dataSource.getConfigHolder()
                .getOptimizerContext()
                .getRuleManager()
                .getDefaultDbIndex(null);

            // 找到对应的执行器
            IGroupExecutor groupExecutor = this.dataSource.getConfigHolder()
                .getExecutorContext()
                .getTopologyHandler()
                .get(defaultDbIndex);

            Object groupDataSource = groupExecutor.getDataSource();
            if (groupDataSource instanceof DataSource) {
                // 指定到主库上执行
                conn = ((DataSource) groupDataSource).getConnection();
                dbMa = conn.getMetaData();
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return dbMa;
    }

    // =============== 特定实现方法==================

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
        throws SQLException {
        return getDatabaseMetaData().getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    public Connection getConnection() throws SQLException {
        return getDatabaseMetaData().getConnection();
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return 5;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return 0;
    }

    public String getDatabaseProductName() throws SQLException {
        return "TDDL";
    }

    public String getDatabaseProductVersion() throws SQLException {
        return Version.getVersion();
    }

    public int getDriverMajorVersion() {
        return 5;
    }

    public int getDriverMinorVersion() {
        return 0;
    }

    public String getDriverName() throws SQLException {
        return "TDDL_JDBC";
    }

    public int getJDBCMajorVersion() throws SQLException {
        return 5;
    }

    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    public String getDriverVersion() throws SQLException {
        return Version.getVersion();
    }

    /**
     * Spring JdbcTemplate的batchUpdate等方法会调用这个判断，
     * 目前暂不支持（supportBatchUpdates返回false）
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /**
     * 不支持union
     */
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    /**
     * 不支持union
     */
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return getDatabaseMetaData().supportsSelectForUpdate();
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesLowerCaseIdentifiers();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesLowerCaseQuotedIdentifiers();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesMixedCaseIdentifiers();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesMixedCaseQuotedIdentifiers();
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesUpperCaseIdentifiers();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return getDatabaseMetaData().storesUpperCaseQuotedIdentifiers();
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
        throws SQLException {
        return getDatabaseMetaData().getIndexInfo(catalog, schema, table, unique, approximate);
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getPrimaryKeys(catalog, schema, table);
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        return getDatabaseMetaData().getProcedureColumns(catalog,
            schemaPattern,
            procedureNamePattern,
            columnNamePattern);
    }

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
        throws SQLException {
        return getDatabaseMetaData().getProcedures(catalog, schemaPattern, procedureNamePattern);
    }

    public ResultSet getSchemas() throws SQLException {
        return getDatabaseMetaData().getSchemas();
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return getDatabaseMetaData().getSuperTables(catalog, schemaPattern, tableNamePattern);
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return getDatabaseMetaData().getSuperTypes(catalog, schemaPattern, typeNamePattern);
    }

    public ResultSet getTableTypes() throws SQLException {
        return getDatabaseMetaData().getTableTypes();
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        throws SQLException {
        return getDatabaseMetaData().getTables(catalog, schemaPattern, tableNamePattern, types);
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
        throws SQLException {
        return getDatabaseMetaData().getFunctions(catalog, schemaPattern, functionNamePattern);
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
                                        String columnNamePattern) throws SQLException {
        return getDatabaseMetaData().getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getDatabaseMetaData().getSchemas(catalog, schemaPattern);
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        return getDatabaseMetaData().getClientInfoProperties();
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {
        return getDatabaseMetaData().getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
        throws SQLException {
        return getDatabaseMetaData().getBestRowIdentifier(catalog, schema, table, scope, nullable);
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
        throws SQLException {
        return getDatabaseMetaData().getColumnPrivileges(catalog, schema, table, columnNamePattern);
    }

    public ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
        throws SQLException {
        return getDatabaseMetaData().getCrossReference(primaryCatalog,
            primarySchema,
            primaryTable,
            foreignCatalog,
            foreignSchema,
            foreignTable);
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getExportedKeys(catalog, schema, table);
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getImportedKeys(catalog, schema, table);
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
        throws SQLException {
        return getDatabaseMetaData().getTablePrivileges(catalog, schemaPattern, tableNamePattern);
    }

    public ResultSet getTypeInfo() throws SQLException {
        return getDatabaseMetaData().getTypeInfo();
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
        throws SQLException {
        return getDatabaseMetaData().getUDTs(catalog, schemaPattern, typeNamePattern, types);
    }

    public ResultSet getCatalogs() throws SQLException {
        return getDatabaseMetaData().getCatalogs();
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return getDatabaseMetaData().getVersionColumns(catalog, schema, table);
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
                                      String columnNamePattern) throws SQLException {
        // return getDatabaseMetaData().getPseudoColumns(catalog, schemaPattern,
        // tableNamePattern, columnNamePattern);
        throw new NotSupportException("getPseudoColumns");
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        // return getDatabaseMetaData().generatedKeyAlwaysReturned();
        throw new NotSupportException("generatedKeyAlwaysReturned");
    }

    // ================ 不处理的方法 ==================

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return null;
    }

    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    public String getCatalogTerm() throws SQLException {
        return null;
    }

    public String getExtraNameCharacters() throws SQLException {
        return null;
    }

    public String getIdentifierQuoteString() throws SQLException {
        return null;
    }

    public String getSchemaTerm() throws SQLException {
        return null;
    }

    public String getSearchStringEscape() throws SQLException {
        return null;
    }

    public String getStringFunctions() throws SQLException {
        return null;
    }

    public String getSystemFunctions() throws SQLException {
        return null;
    }

    public String getTimeDateFunctions() throws SQLException {
        return null;
    }

    public String getURL() throws SQLException {
        return null;
    }

    public String getUserName() throws SQLException {
        return null;
    }

    public String getNumericFunctions() throws SQLException {
        return null;
    }

    public String getProcedureTerm() throws SQLException {
        return null;
    }

    public String getSQLKeywords() throws SQLException {
        return null;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    public int getMaxConnections() throws SQLException {
        return 0;
    }

    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    public int getMaxStatements() throws SQLException {
        return 0;
    }

    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    public int getSQLStateType() throws SQLException {
        return 0;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    public boolean supportsConvert() throws SQLException {
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

}
