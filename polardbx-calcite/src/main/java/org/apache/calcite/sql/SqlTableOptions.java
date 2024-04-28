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

package org.apache.calcite.sql;

import java.util.List;

import com.google.common.base.Optional;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * @author chenmo.cm
 */
public class SqlTableOptions extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("TABLE OPTIONS",
        SqlKind.TABLE_OPTIONS);
    private SqlIdentifier engine;
    private SqlNumericLiteral autoIncrement;
    private SqlNumericLiteral avgRowLength;
    private SqlIdentifier charSet;
    private Boolean defaultCharset;
    /**
     * 这里需要区分带charset的collate还是不带charset的collate
     */
    private SqlIdentifier collateWithCharset;
    private Boolean defaultCollateWithCharset;
    private SqlIdentifier collation;
    private Boolean defaultCollate;
    private SqlLiteral checkSum;
    private SqlCharStringLiteral comment;
    private SqlCharStringLiteral connection;
    private SqlCharStringLiteral dataDir;
    private SqlCharStringLiteral indexDir;
    private SqlLiteral delayKeyWrite;
    private InsertMethod insertMethod;
    private SqlCall keyBlockSize;
    private SqlNumericLiteral maxRows;
    private SqlNumericLiteral minRows;
    private PackKeys packKeys;
    private SqlCharStringLiteral password;
    private RowFormat rowFormat;
    private StatsAutoRecalc statsAutoRecalc;
    private StatsPersistent statsPersistent;
    private SqlCall statsSamplePages;
    private SqlIdentifier tablespaceName;
    private TableSpaceStorage tableSpaceStorage;
    private List<SqlIdentifier> union;
    private Boolean broadcast;
    private SqlIdentifier algorithm;

    // table_option:
    // ENGINE [=] engine_name
    // | AUTO_INCREMENT [=] value
    // | AVG_ROW_LENGTH [=] value
    // | [DEFAULT] CHARACTER SET [=] charset_name
    // | CHECKSUM [=] {0 | 1}
    // | [DEFAULT] COLLATE [=] collation_name
    // | COMMENT [=] 'string'
    // | CONNECTION [=] 'connect_string'
    // | DATA DIRECTORY [=] 'absolute path to directory'
    // | DELAY_KEY_WRITE [=] {0 | 1}
    // | INDEX DIRECTORY [=] 'absolute path to directory'
    // | INSERT_METHOD [=] { NO | FIRST | LAST }
    // | KEY_BLOCK_SIZE [=] value
    // | MAX_ROWS [=] value
    // | MIN_ROWS [=] value
    // | PACK_KEYS [=] {0 | 1 | DEFAULT}
    // | PASSWORD [=] 'string'
    // | ROW_FORMAT [=] {DEFAULT|DYNAMIC|FIXED|COMPRESSED|REDUNDANT|COMPACT}
    // | UNION [=] (tbl_name[,tbl_name]...)
    // | ALGORITHM [=] algorithm_name
    public SqlTableOptions(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(engine,
            autoIncrement,
            avgRowLength,
            charSet,
            SqlUtil.wrapSqlLiteralBoolean(defaultCharset),
            collateWithCharset,
            SqlUtil.wrapSqlLiteralBoolean(defaultCollateWithCharset),
            collation,
            SqlUtil.wrapSqlLiteralBoolean(defaultCollate),
            checkSum,
            comment,
            connection,
            dataDir,
            indexDir,
            delayKeyWrite,
            SqlUtil.wrapSqlLiteralSymbol(insertMethod),
            keyBlockSize,
            maxRows,
            minRows,
            SqlUtil.wrapSqlLiteralSymbol(packKeys),
            password,
            SqlUtil.wrapSqlLiteralSymbol(rowFormat),
            SqlUtil.wrapSqlLiteralSymbol(statsAutoRecalc),
            SqlUtil.wrapSqlLiteralSymbol(statsPersistent),
            statsSamplePages,
            tablespaceName,
            SqlUtil.wrapSqlLiteralSymbol(tableSpaceStorage),
            SqlUtil.wrapSqlNodeList(union),
            SqlUtil.wrapSqlLiteralBoolean(broadcast),
            algorithm
        );
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(optionToString());
        //super.unparse(writer, leftPrec, rightPrec);
    }

    public String optionToString() {
        StringBuilder sb = new StringBuilder();
        appendOption(sb, "ENGINE", engine);
        appendOption(sb, "AUTO_INCREMENT", autoIncrement);
        appendOption(sb, "AVG_ROW_LENGTH", avgRowLength);
        appendOption(sb, "CHARACTER SET", charSet);
        appendOption(sb, "DEFAULT CHARSET", defaultCharset);
        appendOption(sb, "COLLATE", collateWithCharset);
        appendOption(sb, "DEFAULT COLLATE", defaultCollateWithCharset);
        appendOption(sb, "COLLATION", collation);
        appendOption(sb, "DEFAULT COLLATE", defaultCollate);
        appendOption(sb, "CHECKSUM", checkSum);
        appendOption(sb, "COMMENT", comment, true);
        appendOption(sb, "CONNECTION", connection, true);
        appendOption(sb, "DATA DIRECTORY", dataDir, true);
        appendOption(sb, "INDEX DIRECTORY", indexDir, true);
        appendOption(sb, "DELAY_KEY_WRITE", delayKeyWrite);
        appendOption(sb, "INSERT_METHOD", insertMethod);
        appendOption(sb, "KEY_BLOCK_SIZE", keyBlockSize);
        appendOption(sb, "MAX_ROWS", maxRows);
        appendOption(sb, "MIN_ROWS", minRows);
        appendOption(sb, "PACK_KEYS", packKeys);
        appendOption(sb, "PASSWORD", password, true);
        appendOption(sb, "ROW_FORMAT", rowFormat);
        appendOption(sb, "STATS_AUTO_RECALC", statsAutoRecalc);
        appendOption(sb, "STATS_PERSISTENT", statsPersistent);
        appendOption(sb, "STATS_SAMPLE_PAGES", statsSamplePages);
        appendOption(sb, "TABLESPACE", tablespaceName);
        appendOption(sb, "STORAGE", tableSpaceStorage);
        appendOption(sb, "UNION", union);
        appendOption(sb, "BROADCAST", broadcast);
        appendOption(sb, "ALGORITHM", algorithm);

        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
        }

        return sb.toString();
    }

    private void appendOption(StringBuilder sb, String optionName, Object value) {
        appendOption(sb, optionName, value, false);
    }

    private void appendOption(StringBuilder sb, String optionName, Object value, boolean isString) {
        if (value != null) {
            sb.append(optionName).append(" = ");
            if (isString) {
                sb.append("'").append(value.toString().replace("'", "\\'")).append("'");
            } else {
                sb.append(value);
            }
            sb.append(", ");
        }
    }

    public SqlIdentifier getEngine() {
        return engine;
    }

    public void setEngine(SqlIdentifier engine) {
        this.engine = engine;
    }

    public SqlNumericLiteral getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(SqlNumericLiteral autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public SqlNumericLiteral getAvgRowLength() {
        return avgRowLength;
    }

    public void setAvgRowLength(SqlNumericLiteral avgRowLength) {
        this.avgRowLength = avgRowLength;
    }

    public SqlIdentifier getCharSet() {
        return charSet;
    }

    public void setCharSet(SqlIdentifier charSet) {
        this.charSet = charSet;
    }

    public boolean isDefaultCharset() {
        return defaultCharset;
    }

    public void setDefaultCharset(boolean defaultCharset) {
        this.defaultCharset = defaultCharset;
    }

    public SqlIdentifier getCollateWithCharset() {
        return collateWithCharset;
    }

    public void setCollateWithCharset(SqlIdentifier collateWithCharset) {
        this.collateWithCharset = collateWithCharset;
    }

    public boolean isDefaultCollateWithCharset() {
        return defaultCollateWithCharset;
    }

    public void setDefaultCollateWithCharset(boolean defaultCollateWithCharset) {
        this.defaultCollateWithCharset = defaultCollateWithCharset;
    }

    public SqlIdentifier getCollation() {
        return collation;
    }

    public void setCollation(SqlIdentifier collation) {
        this.collation = collation;
    }

    public boolean isDefaultCollate() {
        return defaultCollate;
    }

    public void setDefaultCollate(boolean defaultCollate) {
        this.defaultCollate = defaultCollate;
    }

    public SqlCharStringLiteral getComment() {
        return comment;
    }

    public void setComment(SqlCharStringLiteral comment) {
        this.comment = comment;
    }

    public SqlCharStringLiteral getConnection() {
        return connection;
    }

    public void setConnection(SqlCharStringLiteral connection) {
        this.connection = connection;
    }

    public SqlCharStringLiteral getDataDir() {
        return dataDir;
    }

    public void setDataDir(SqlCharStringLiteral dataDir) {
        this.dataDir = dataDir;
    }

    public SqlCharStringLiteral getIndexDir() {
        return indexDir;
    }

    public void setIndexDir(SqlCharStringLiteral indexDir) {
        this.indexDir = indexDir;
    }

    public InsertMethod getInsertMethod() {
        return insertMethod;
    }

    public void setInsertMethod(InsertMethod insertMethod) {
        this.insertMethod = insertMethod;
    }

    public SqlCall getKeyBlockSize() {
        return keyBlockSize;
    }

    public void setKeyBlockSize(SqlCall keyBlockSize) {
        this.keyBlockSize = keyBlockSize;
    }

    public SqlNumericLiteral getMaxRows() {
        return maxRows;
    }

    public void setMaxRows(SqlNumericLiteral maxRows) {
        this.maxRows = maxRows;
    }

    public SqlNumericLiteral getMinRows() {
        return minRows;
    }

    public void setMinRows(SqlNumericLiteral minRows) {
        this.minRows = minRows;
    }

    public PackKeys getPackKeys() {
        return packKeys;
    }

    public void setPackKeys(PackKeys packKeys) {
        this.packKeys = packKeys;
    }

    public SqlCharStringLiteral getPassword() {
        return password;
    }

    public void setPassword(SqlCharStringLiteral password) {
        this.password = password;
    }

    public RowFormat getRowFormat() {
        return rowFormat;
    }

    public void setRowFormat(RowFormat rowFormat) {
        this.rowFormat = rowFormat;
    }

    public StatsAutoRecalc getStatsAutoRecalc() {
        return statsAutoRecalc;
    }

    public void setStatsAutoRecalc(StatsAutoRecalc statsAutoRecalc) {
        this.statsAutoRecalc = statsAutoRecalc;
    }

    public StatsPersistent getStatsPersistent() {
        return statsPersistent;
    }

    public void setStatsPersistent(StatsPersistent statsPersistent) {
        this.statsPersistent = statsPersistent;
    }

    public SqlCall getStatsSamplePages() {
        return statsSamplePages;
    }

    public void setStatsSamplePages(SqlCall statsSamplePages) {
        this.statsSamplePages = statsSamplePages;
    }

    public SqlIdentifier getTablespaceName() {
        return tablespaceName;
    }

    public void setTablespaceName(SqlIdentifier tablespaceName) {
        this.tablespaceName = tablespaceName;
    }

    public TableSpaceStorage getTableSpaceStorage() {
        return tableSpaceStorage;
    }

    public void setTableSpaceStorage(TableSpaceStorage tableSpaceStorage) {
        this.tableSpaceStorage = tableSpaceStorage;
    }

    public List<SqlIdentifier> getUnion() {
        return union;
    }

    public void setUnion(List<SqlIdentifier> union) {
        this.union = union;
    }

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
    }

    public SqlLiteral getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(SqlLiteral checkSum) {
        this.checkSum = checkSum;
    }

    public SqlLiteral getDelayKeyWrite() {
        return delayKeyWrite;
    }

    public void setDelayKeyWrite(SqlLiteral delayKeyWrite) {
        this.delayKeyWrite = delayKeyWrite;
    }

    public SqlIdentifier getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(SqlIdentifier algorithm) {
        this.algorithm = algorithm;
    }

    public static enum InsertMethod {
        NO, FIRST, LAST
    }

    public static enum PackKeys {
        FALSE, TRUE, DEFAULT
    }

    public static enum StatsAutoRecalc {
        FALSE, TRUE, DEFAULT
    }

    public static enum StatsPersistent {
        FALSE, TRUE, DEFAULT
    }

    public static enum RowFormat {
        DEFAULT, DYNAMIC, FIXED, COMPRESSED, REDUNDANT, COMPACT
    }

    public static enum TableSpaceStorage {
        DISK, MEMORY, DEFAULT
    }
}
