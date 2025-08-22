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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class SqlIndexDefinition extends SqlCall {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("INDEX_DEF", SqlKind.INDEX_DEF);
    /**
     * [CONSTRAINT [symbol]] [GLOBAL|LOCAL] UNIQUE [INDEX|KEY]
     */
    private final boolean hasConstraint;
    private final boolean clusteredIndex;
    private final boolean columnarIndex;
    private final SqlIdentifier uniqueConstraint;
    private final SqlIndexResiding indexResiding;
    private final String type; // FULLTEXT/PRIMARY/UNIQUE/SPATIAL
    private final SqlIndexType indexType;
    private final SqlIdentifier indexName;
    private final SqlIdentifier originIndexName;
    private final SqlIdentifier table;
    private final List<SqlIndexColumnName> columns;
    private final List<SqlIndexColumnName> covering;
    private final List<SqlIndexColumnName> originCovering;
    private final List<SqlIndexColumnName> clusteredKeys;
    private final SqlNode dbPartitionBy;
    private final SqlNode dbPartitions = null;
    private final SqlNode tbPartitionBy;
    private final SqlNode tbPartitions;

    private final SqlNode partitioning;
    private final SqlNode originPartitioning;
    private final List<SqlIndexOption> options;
    private String primaryTableDefinition;
    private SqlCreateTable primaryTableNode;
    private boolean broadcast;
    private boolean single;

    /**
     * control index visibility
     * https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html
     */
    private boolean visible;
    private final SqlNode tableGroupName;
    private final SqlNode engineName;
    private final List<SqlIndexColumnName> dictColumns;
    private final boolean withImplicitTableGroup;
    // Key should be lowercase.
    private final Map<String, String> columnarOptions;

    public SqlIndexDefinition(SqlParserPos pos,
                              boolean hasConstraint,
                              SqlIdentifier uniqueConstraint,
                              SqlIndexResiding indexResiding,
                              String type,
                              SqlIndexType indexType,
                              SqlIdentifier indexName,
                              SqlIdentifier originIndexName,
                              SqlIdentifier table,
                              List<SqlIndexColumnName> columns,
                              List<SqlIndexColumnName> covering,
                              List<SqlIndexColumnName> originCovering,
                              SqlNode dbPartitionBy,
                              SqlNode tbPartitionBy,
                              SqlNode tbPartitions,
                              SqlNode partitioning,
                              SqlNode originPartitioning,
                              List<SqlIndexColumnName> clusteredKeys,
                              List<SqlIndexOption> options,
                              boolean clusteredIndex,
                              boolean columnarIndex,
                              SqlNode tableGroupName,
                              SqlNode engineName,
                              List<SqlIndexColumnName> dictColumns,
                              boolean withImplicitTableGroup,
                              boolean visible,
                              Map<String, String> columnarOptions) {
        super(pos);
        this.hasConstraint = hasConstraint;
        this.uniqueConstraint = uniqueConstraint;
        this.indexResiding = indexResiding;
        this.type = type;
        this.indexType = indexType;
        this.indexName = indexName;
        this.originIndexName = originIndexName;
        this.table = table;
        this.columns = columns;
        this.covering = covering;
        this.originCovering = originCovering;
        this.dbPartitionBy = dbPartitionBy;
        this.tbPartitionBy = tbPartitionBy;
        this.tbPartitions = tbPartitions;
        this.clusteredKeys = clusteredKeys;
        this.options = options;
        this.clusteredIndex = clusteredIndex;
        this.columnarIndex = columnarIndex;
        this.partitioning = partitioning;
        this.originPartitioning = originPartitioning;
        this.tableGroupName = tableGroupName;
        this.engineName = engineName;
        this.dictColumns = dictColumns;
        this.withImplicitTableGroup = withImplicitTableGroup;
        this.visible = visible;
        this.columnarOptions = columnarOptions;
    }

    public SqlIndexDefinition(SqlParserPos pos,
                              boolean hasConstraint,
                              SqlIdentifier uniqueConstraint,
                              SqlIndexResiding indexResiding,
                              String type,
                              SqlIndexType indexType,
                              SqlIdentifier indexName,
                              SqlIdentifier originIndexName,
                              SqlIdentifier table,
                              List<SqlIndexColumnName> columns,
                              List<SqlIndexColumnName> covering,
                              List<SqlIndexColumnName> originCovering,
                              SqlNode dbPartitionBy,
                              SqlNode tbPartitionBy,
                              SqlNode tbPartitions,
                              SqlNode partitioning,
                              SqlNode originPartitioning,
                              List<SqlIndexColumnName> clusteredKeys,
                              List<SqlIndexOption> options,
                              String primaryTableDefinition,
                              SqlCreateTable primaryTableNode,
                              boolean clusteredIndex,
                              boolean columnarIndex,
                              SqlNode tableGroupName,
                              SqlNode engineName,
                              List<SqlIndexColumnName> dictColumns,
                              boolean withImplicitTableGroup,
                              boolean visible,
                              Map<String, String> columnarOptions) {
        super(pos);
        this.hasConstraint = hasConstraint;
        this.uniqueConstraint = uniqueConstraint;
        this.indexResiding = indexResiding;
        this.type = type;
        this.indexType = indexType;
        this.indexName = indexName;
        this.originIndexName = originIndexName;
        this.table = table;
        this.columns = columns;
        this.covering = covering;
        this.originCovering = originCovering;
        this.dbPartitionBy = dbPartitionBy;
        this.tbPartitionBy = tbPartitionBy;
        this.tbPartitions = tbPartitions;
        this.clusteredKeys = clusteredKeys;
        this.options = options;
        this.primaryTableDefinition = primaryTableDefinition;
        this.primaryTableNode = primaryTableNode;
        this.clusteredIndex = clusteredIndex;
        this.columnarIndex = columnarIndex;
        this.partitioning = partitioning;
        this.originPartitioning = originPartitioning;
        this.tableGroupName = tableGroupName;
        this.engineName = engineName;
        this.dictColumns = dictColumns;
        this.withImplicitTableGroup = withImplicitTableGroup;
        this.visible = visible;
        this.columnarOptions = columnarOptions;
    }

    public static SqlIndexDefinition localIndex(SqlParserPos pos, boolean hasConstraint,
                                                SqlIdentifier uniqueConstraint, boolean explicit, String type,
                                                SqlIndexType indexType, SqlIdentifier indexName, SqlIdentifier table,
                                                List<SqlIndexColumnName> columns, List<SqlIndexOption> options) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            explicit ? SqlIndexResiding.LOCAL : null,
            type,
            indexType,
            indexName,
            indexName,
            table,
            columns,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            options,
            false,
            false,
            null,
            null,
            null,
            false,
            true,
            null);
    }

    public static SqlIndexDefinition localIndex(SqlParserPos pos, boolean hasConstraint,
                                                SqlIdentifier uniqueConstraint, boolean explicit, String type,
                                                SqlIndexType indexType, SqlIdentifier indexName, SqlIdentifier table,
                                                List<SqlIndexColumnName> columns, List<SqlIndexOption> options,
                                                SqlNode tableGroupName, boolean withImplicitTableGroup) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            explicit ? SqlIndexResiding.LOCAL : null,
            type,
            indexType,
            indexName,
            indexName,
            table,
            columns,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            options,
            false,
            false,
            tableGroupName,
            null,
            null,
            withImplicitTableGroup,
            true,
            null);
    }

    public static SqlIndexDefinition globalIndexForRebuild(SqlParserPos pos, boolean hasConstraint,
                                                           SqlIdentifier uniqueConstraint, String type,
                                                           SqlIndexType indexType, SqlIdentifier indexName,
                                                           SqlIdentifier table,
                                                           List<SqlIndexColumnName> columns,
                                                           List<SqlIndexColumnName> covering,
                                                           SqlNode dbPartitionBy, SqlNode tbPartitionBy,
                                                           SqlNode tbPartitions,
                                                           SqlNode partitioning, List<SqlIndexOption> options,
                                                           SqlNode tableGroupName, boolean withImplicitTablegroup,
                                                           boolean visible, boolean isClustered) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.GLOBAL,
            type,
            indexType,
            indexName,
            indexName,
            table,
            columns,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            null,
            options,
            isClustered,
            false,
            tableGroupName,
            null,
            null,
            withImplicitTablegroup,
            visible,
            null);
    }

    public static SqlIndexDefinition globalIndex(SqlParserPos pos, boolean hasConstraint,
                                                 SqlIdentifier uniqueConstraint, String type,
                                                 SqlIndexType indexType, SqlIdentifier indexName, SqlIdentifier table,
                                                 List<SqlIndexColumnName> columns, List<SqlIndexColumnName> covering,
                                                 SqlNode dbPartitionBy, SqlNode tbPartitionBy, SqlNode tbPartitions,
                                                 SqlNode partitioning, List<SqlIndexOption> options,
                                                 SqlNode tableGroupName, boolean withImplicitTablegroup,
                                                 boolean visible) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.GLOBAL,
            type,
            indexType,
            indexName,
            indexName,
            table,
            columns,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            null,
            options,
            false,
            false,
            tableGroupName,
            null,
            null,
            withImplicitTablegroup,
            visible,
            null);
    }

    public static SqlIndexDefinition clusteredIndex(SqlParserPos pos, boolean hasConstraint,
                                                    SqlIdentifier uniqueConstraint, String type,
                                                    SqlIndexType indexType, SqlIdentifier indexName,
                                                    SqlIdentifier table, List<SqlIndexColumnName> columns,
                                                    List<SqlIndexColumnName> covering, SqlNode dbPartitionBy,
                                                    SqlNode tbPartitionBy, SqlNode tbPartitions, SqlNode partitioning,
                                                    List<SqlIndexOption> options, SqlNode tableGroupName,
                                                    boolean withImplicitTablegroup, boolean visible) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.GLOBAL,
            type,
            indexType,
            indexName,
            indexName,
            table,
            columns,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            null,
            options,
            true,
            false,
            tableGroupName,
            null,
            null,
            withImplicitTablegroup,
            visible,
            null);
    }

    public static SqlIndexDefinition columnarIndex(SqlParserPos pos, boolean hasConstraint,
                                                   SqlIdentifier uniqueConstraint, String type,
                                                   SqlIndexType indexType, SqlIdentifier indexName,
                                                   SqlIdentifier table, List<SqlIndexColumnName> columns,
                                                   List<SqlIndexColumnName> covering, SqlNode dbPartitionBy,
                                                   SqlNode tbPartitionBy, SqlNode tbPartitions, SqlNode partitioning,
                                                   List<SqlIndexColumnName> clusteredKeys,
                                                   List<SqlIndexOption> options, SqlNode tableGroupName,
                                                   SqlNode engineName, List<SqlIndexColumnName> dictColumns,
                                                   boolean withImplicitTablegroup, boolean visible,
                                                   Map<String, String> columnarOptions) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.GLOBAL,
            type,
            indexType,
            indexName,
            indexName,
            table,
            columns,
            covering,
            covering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            partitioning,
            clusteredKeys,
            options,
            true,
            true,
            tableGroupName,
            engineName,
            dictColumns,
            withImplicitTablegroup,
            visible,
            columnarOptions);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(uniqueConstraint,
            SqlUtil.wrapSqlLiteralSymbol(indexResiding),
            SqlUtil.wrapSqlLiteralSymbol(indexType),
            indexName,
            table,
            SqlUtil.wrapSqlNodeList(columns),
            SqlUtil.wrapSqlNodeList(covering),
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            SqlUtil.wrapSqlNodeList(options));
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, false);
    }

    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, boolean withOriginNames) {
        final boolean isGlobal = SqlUtil.isGlobal(indexResiding);

        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "", "");

        if (null != indexType) {
            writer.keyword("USING");
            writer.keyword(indexType.name());
        }

        if (null != columns) {
            final Frame frame1 = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
            SqlUtil.wrapSqlNodeList(columns).commaList(writer);
            writer.endList(frame1);
        }

        if (isGlobal || columnarIndex) {
            final List<SqlIndexColumnName> coveringToShow = withOriginNames ? originCovering : covering;
            if (null != coveringToShow && !coveringToShow.isEmpty()) {
                writer.keyword("COVERING");
                final Frame frame2 = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
                SqlUtil.wrapSqlNodeList(coveringToShow).commaList(writer);
                writer.endList(frame2);
            }

            final boolean quoteAllIdentifiers = writer.isQuoteAllIdentifiers();
            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).setQuoteAllIdentifiers(false);
            }
            if (null != dbPartitionBy) {
                writer.keyword("DBPARTITION BY");
                dbPartitionBy.unparse(writer, leftPrec, rightPrec);
            }

            if (null != tbPartitionBy) {
                writer.keyword("TBPARTITION BY");
                tbPartitionBy.unparse(writer, leftPrec, rightPrec);
            }

            if (null != tbPartitions) {
                writer.keyword("TBPARTITIONS");
                tbPartitions.unparse(writer, leftPrec, rightPrec);
            }

            final SqlNode partitioningToShow = withOriginNames ? originPartitioning : partitioning;
            if (null != partitioningToShow) {
                partitioningToShow.unparse(writer, leftPrec, rightPrec);
            }

            if (writer instanceof SqlPrettyWriter) {
                ((SqlPrettyWriter) writer).setQuoteAllIdentifiers(quoteAllIdentifiers);
            }
        }

        if (columnarIndex && null != engineName) {
            writer.keyword("ENGINE");
            writer.keyword("=");
            engineName.unparse(writer, leftPrec, rightPrec);
        }

        if (null != options) {
            for (SqlIndexOption option : options) {
                option.unparse(writer, leftPrec, rightPrec);
            }
        }

        writer.endList(frame);
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    public boolean isBtree() {
        return null != indexType && SqlIndexType.BTREE == indexType;
    }

    public boolean isHash() {
        return null != indexType && SqlIndexType.HASH == indexType;
    }

    public boolean isGlobal() {
        return SqlUtil.isGlobal(this.indexResiding);
    }

    public boolean isLocal() {
        return null != indexResiding && SqlIndexResiding.LOCAL == indexResiding;
    }

    public boolean isClustered() {
        return clusteredIndex;
    }

    public boolean isColumnar() {
        return columnarIndex;
    }

    public boolean isHasConstraint() {
        return hasConstraint;
    }

    public SqlIdentifier getUniqueConstraint() {
        return uniqueConstraint;
    }

    public SqlIndexResiding getIndexResiding() {
        return indexResiding;
    }

    public String getType() {
        return type;
    }

    public SqlIndexType getIndexType() {
        return indexType;
    }

    public SqlIdentifier getIndexName() {
        return indexName;
    }

    public SqlIdentifier getTable() {
        return table;
    }

    public List<SqlIndexColumnName> getColumns() {
        return columns;
    }

    public List<SqlIndexColumnName> getCovering() {
        return covering;
    }

    public List<SqlIndexColumnName> getClusteredKeys() {
        return clusteredKeys;
    }

    public SqlNode getDbPartitionBy() {
        return dbPartitionBy;
    }

    public SqlNode getDbPartitions() {
        return dbPartitions;
    }

    public SqlNode getTbPartitionBy() {
        return tbPartitionBy;
    }

    public SqlNode getTbPartitions() {
        return tbPartitions;
    }

    public List<SqlIndexOption> getOptions() {
        return options;
    }

    public boolean isBroadcast() {
        return this.broadcast;
    }

    public void setBroadcast(final boolean broadcast) {
        this.broadcast = broadcast;
    }

    public boolean isSingle() {
        return this.single;
    }

    public void setSingle(final boolean single) {
        this.single = single;
    }

    public enum SqlIndexResiding {
        LOCAL(0), GLOBAL(1);

        private final int value;

        SqlIndexResiding(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static SqlIndexResiding from(int value) {
            switch (value) {
            case 0:
                return LOCAL;
            case 1:
                return GLOBAL;
            default:
                return null;
            }
        }
    }

    public enum SqlIndexType {
        BTREE, HASH, INVERSE;

        public static SqlIndexType from(String value) {
            try {
                return SqlIndexType.valueOf(value);
            } catch (Exception e) {
                return null;
            }
        }
    }

    public String getPrimaryTableDefinition() {
        return primaryTableDefinition;
    }

    public void setPrimaryTableDefinition(String primaryTableDefinition) {
        this.primaryTableDefinition = primaryTableDefinition;
    }

    public SqlCreateTable getPrimaryTableNode() {
        return primaryTableNode;
    }

    public void setPrimaryTableNode(SqlCreateTable primaryTableNode) {
        this.primaryTableNode = primaryTableNode;
    }

    public SqlIdentifier getOriginIndexName() {
        return originIndexName;
    }

    public List<SqlIndexColumnName> getOriginCovering() {
        return originCovering;
    }

    public SqlNode getOriginPartitioning() {
        return originPartitioning;
    }

    public Map<String, String> getColumnarOptions() {
        return columnarOptions;
    }

    public SqlIndexDefinition replacePartitioning(SqlNode newPartition) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            indexResiding,
            type,
            indexType,
            indexName,
            originIndexName,
            table,
            columns,
            covering,
            originCovering,
            null == newPartition ? dbPartitionBy : null,
            null == newPartition ? tbPartitionBy : null,
            null == newPartition ? tbPartitions : null,
            null == newPartition ? partitioning : newPartition,
            originPartitioning,
            columnarIndex ? clusteredKeys : null,
            options,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            engineName,
            dictColumns,
            withImplicitTableGroup,
            visible,
            columnarOptions);
    }

    public SqlIndexDefinition replaceCovering(Collection<String> coveringColumns) {
        if (GeneralUtil.isEmpty(coveringColumns)) {
            return this;
        }

        List<SqlIndexColumnName> tmpCovering = new ArrayList<>();
        for (String coveringColumn : coveringColumns) {
            tmpCovering.add(new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(coveringColumn,
                SqlParserPos.ZERO), null, null));
        }

        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            indexResiding,
            type,
            indexType,
            indexName,
            originIndexName,
            table,
            columns,
            tmpCovering,
            originCovering,
            dbPartitionBy,
            tbPartitionBy,
            tbPartitions,
            partitioning,
            originPartitioning,
            clusteredKeys,
            options,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            engineName,
            dictColumns,
            withImplicitTableGroup,
            this.visible,
            columnarOptions);
    }

    public SqlIndexDefinition mergeCovering(Collection<String> coveringColumns) {
        if (GeneralUtil.isEmpty(coveringColumns)) {
            return this;
        }

        Map<String, SqlIndexColumnName> current = new HashMap<>(
            Maps.uniqueIndex(this.getColumns(), SqlIndexColumnName::getColumnNameStr));

        if (null != this.getCovering()) {
            current.putAll(Maps.uniqueIndex(this.getCovering(), SqlIndexColumnName::getColumnNameStr));
        }

        List<SqlIndexColumnName> tmpCovering = new ArrayList<>();
        for (String coveringColumn : coveringColumns) {
            if (!current.containsKey(coveringColumn)) {
                tmpCovering.add(
                    new SqlIndexColumnName(SqlParserPos.ZERO, new SqlIdentifier(coveringColumn, SqlParserPos.ZERO),
                        null, null));
            }
        }

        if (null != this.getCovering()) {
            this.getCovering().addAll(tmpCovering);
            return this;
        } else {
            return new SqlIndexDefinition(pos,
                hasConstraint,
                uniqueConstraint,
                indexResiding,
                type,
                indexType,
                indexName,
                originIndexName,
                table,
                columns,
                tmpCovering,
                originCovering,
                dbPartitionBy,
                tbPartitionBy,
                tbPartitions,
                partitioning,
                originPartitioning,
                clusteredKeys,
                options,
                primaryTableDefinition,
                primaryTableNode,
                clusteredIndex,
                columnarIndex,
                tableGroupName,
                engineName,
                dictColumns,
                withImplicitTableGroup,
                visible,
                columnarOptions);
        }

    }

    /**
     * Rebuild gsi definition with new index name and full partition definition
     *
     * @param newName New index name, with random suffix
     * @param dbPartition Update with full partition definition, with DBPARTITION BY appended
     * @return Copied SqlIndexDefinition
     */
    public SqlIndexDefinition rebuildToGsi(SqlIdentifier newName, SqlNode dbPartition) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.GLOBAL,
            type,
            indexType,
            null == newName ? indexName : newName,
            originIndexName,
            table,
            columns,
            covering,
            originCovering,
            null == dbPartition ? dbPartitionBy : dbPartition,
            null == dbPartition ? tbPartitionBy : null,
            null == dbPartition ? tbPartitions : null,
            partitioning,
            originPartitioning,
            columnarIndex ? clusteredKeys : null,
            options,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            engineName,
            dictColumns,
            withImplicitTableGroup,
            visible,
            columnarOptions);
    }

    /**
     * Rebuild gsi definition with new index name and full partition definition
     *
     * @param newName New index name, with random suffix
     * @param newPartition Update with full partition definition, with PARTITION BY/PARTITIONS appended
     * @return Copied SqlIndexDefinition
     */
    public SqlIndexDefinition rebuildToGsiNewPartition(SqlIdentifier newName, SqlNode newPartition) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.GLOBAL,
            type,
            indexType,
            null == newName ? indexName : newName,
            originIndexName,
            table,
            columns,
            covering,
            originCovering,
            null == newPartition ? dbPartitionBy : null,
            null == newPartition ? tbPartitionBy : null,
            null == newPartition ? tbPartitions : null,
            null == newPartition ? partitioning : newPartition,
            originPartitioning,
            columnarIndex ? clusteredKeys : null,
            options,
            primaryTableDefinition,
            primaryTableNode,
            clusteredIndex,
            columnarIndex,
            tableGroupName,
            engineName,
            dictColumns,
            withImplicitTableGroup,
            visible,
            columnarOptions);
    }

    public SqlIndexDefinition rebuildToExplicitLocal(SqlIdentifier newName) {
        return new SqlIndexDefinition(pos,
            hasConstraint,
            uniqueConstraint,
            SqlIndexResiding.LOCAL,
            type,
            indexType,
            null == newName ? indexName : newName,
            originIndexName,
            table,
            columns,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            options,
            primaryTableDefinition,
            primaryTableNode,
            false,
            false,
            tableGroupName,
            engineName,
            dictColumns,
            withImplicitTableGroup,
            visible,
            columnarOptions);
    }

    public SqlNode getPartitioning() {
        return partitioning;
    }

    public SqlNode getTableGroupName() {
        return tableGroupName;
    }

    public SqlNode getEngineName() {
        return engineName;
    }

    public List<SqlIndexColumnName> getDictColumns() {
        return dictColumns;
    }

    public boolean withoutPartitionDef() {
        return null == partitioning && null == dbPartitionBy;
    }

    public boolean isPartitionIndex() {
        return !isSingle() && !isBroadcast();
    }

    public boolean isWithImplicitTableGroup() {
        return withImplicitTableGroup;
    }
}
