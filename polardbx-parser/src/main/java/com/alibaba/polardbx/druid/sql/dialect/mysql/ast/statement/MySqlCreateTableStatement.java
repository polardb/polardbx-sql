/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimeToLiveDefinitionExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterCharacter;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableConvertCharSet;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlShowColumnOutpuVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.polardbx.druid.util.CharsetNameForParser;
import com.alibaba.polardbx.druid.util.CollationNameForParser;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MySqlCreateTableStatement extends SQLCreateTableStatement implements MySqlStatement {
    private List<SQLCommentHint> hints = new ArrayList<SQLCommentHint>();
    private List<SQLCommentHint> optionHints = new ArrayList<SQLCommentHint>();
    private SQLName tableGroup; // for polarx
    private boolean withImplicitTablegroup = false; // for polarx
    private SQLName joinGroup; // for polarx
    protected boolean prefixPartition; // for polarx
    protected boolean prefixBroadcast; // for polarx
    protected boolean prefixSingle; // for polarx
    protected SQLExpr dbPartitionBy;//for drds
    protected SQLExpr dbPartitions;//for drds
    protected SQLExpr tablePartitionBy;//for drds
    protected SQLExpr tablePartitions;//for drds
    protected MySqlExtPartition exPartition; //for drds
    protected SQLName storedBy; // for ads
    protected SQLName distributeByType; // for ads
    protected List<SQLName> distributeBy = new ArrayList<SQLName>();
    protected List<SQLIdentifierExpr> routeBy = new ArrayList<SQLIdentifierExpr>();
    protected SQLIdentifierExpr effectedBy;
    protected boolean isBroadCast;
    protected boolean isSingle;
    protected SQLExpr locality; // for drds
    protected SQLExpr autoSplit; // for drds
    protected Map<String, SQLName> with = new HashMap<String, SQLName>(3); // for ads

    protected SQLName archiveBy; // adb
    protected Boolean withData;

    public MySqlCreateTableStatement() {
        super(DbType.mysql);
    }

    public void setEngine(String engineName) {
        if (this.tableOptions != null) {
            for (SQLAssignItem item : this.tableOptions) {
                if ("ENGINE".equalsIgnoreCase(item.getTarget().toString())) {
                    SQLCharExpr charExpr = new SQLCharExpr(engineName);
                    item.setValue(charExpr);
                    break;
                }
            }
        }
    }

    public SQLAssignItem getTtlDefinitionOption() {
        SQLAssignItem ttlDefinitionOption = null;
        for (int i = 0; i < tableOptions.size(); i++) {
            SQLAssignItem assignItem = tableOptions.get(i);
            SQLExpr val = assignItem.getValue();
            if (val instanceof SQLTimeToLiveDefinitionExpr) {
                ttlDefinitionOption = assignItem;
                break;
            }
        }
        return ttlDefinitionOption;
    }

    public void removeArchiveCciInfoForTtlDefinitionOptionIfNeed() {
        SQLAssignItem ttlDefinitionOption = null;
        for (int i = 0; i < tableOptions.size(); i++) {
            SQLAssignItem assignItem = tableOptions.get(i);
            SQLExpr val = assignItem.getValue();
            if (val instanceof SQLTimeToLiveDefinitionExpr) {
                ttlDefinitionOption = assignItem;
                break;
            }
        }
        if (ttlDefinitionOption == null) {
            return;
        }

        SQLExpr ttlDefExprAst = ttlDefinitionOption.getValue();
        SQLTimeToLiveDefinitionExpr ttlDefExpr = (SQLTimeToLiveDefinitionExpr) ttlDefExprAst;

        SQLExpr arcSchemaExpr = ttlDefExpr.getArchiveTableSchemaExpr();
        SQLExpr arcTblNameExpr = ttlDefExpr.getArchiveTableNameExpr();

        ttlDefExpr.setTtlEnableExpr(null);
        ttlDefExpr.setArchiveTableSchemaExpr(null);
        ttlDefExpr.setArchiveTableNameExpr(null);

        MySqlTableIndex targetArcCci = null;
        int elementIdx = -1;
        for (int i = 0; i < tableElementList.size(); i++) {
            SQLTableElement element = tableElementList.get(i);
            if (element instanceof MySqlTableIndex) {
                MySqlTableIndex idx = (MySqlTableIndex) element;
                if (idx.isColumnar()) {
                    targetArcCci = idx;
                    elementIdx = i;
                    break;
                }
            }
        }
        if (targetArcCci != null) {
            tableElementList.remove(elementIdx);
        }
    }

    public List<SQLCommentHint> getHints() {
        return hints;
    }

    public void setHints(List<SQLCommentHint> hints) {
        this.hints = hints;
    }

    @Deprecated
    public SQLSelect getQuery() {
        return select;
    }

    @Deprecated
    public void setQuery(SQLSelect query) {
        this.select = query;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof MySqlASTVisitor) {
            accept0((MySqlASTVisitor) visitor);
        } else {
            super.accept0(visitor);
        }
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            for (int i = 0; i < hints.size(); i++) {
                final SQLCommentHint hint = hints.get(i);
                if (hint != null) {
                    hint.accept(visitor);
                }
            }

            if (tableSource != null) {
                tableSource.accept(visitor);
            }

            for (int i = 0; i < tableElementList.size(); i++) {
                final SQLTableElement element = tableElementList.get(i);
                if (element != null) {
                    element.accept(visitor);
                }
            }

            if (like != null) {
                like.accept(visitor);
            }

            if (asTable != null) {
                asTable.accept(visitor);
            }

            if (select != null) {
                select.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public static class TableSpaceOption extends MySqlExprImpl {

        private SQLName name;
        private SQLExpr storage;

        public SQLName getName() {
            return name;
        }

        public void setName(SQLName name) {
            if (name != null) {
                name.setParent(this);
            }
            this.name = name;
        }

        public SQLExpr getStorage() {
            return storage;
        }

        public void setStorage(SQLExpr storage) {
            if (storage != null) {
                storage.setParent(this);
            }
            this.storage = storage;
        }

        @Override
        public void accept0(MySqlASTVisitor visitor) {
            if (visitor.visit(this)) {
                acceptChild(visitor, getName());
                acceptChild(visitor, getStorage());
            }
            visitor.endVisit(this);
        }

        public TableSpaceOption clone() {
            TableSpaceOption x = new TableSpaceOption();

            if (name != null) {
                x.setName(name.clone());
            }

            if (storage != null) {
                x.setStorage(storage.clone());
            }

            return x;
        }

        @Override
        public List<SQLObject> getChildren() {
            return null;
        }

    }

    public List<SQLCommentHint> getOptionHints() {
        return optionHints;
    }

    public void setOptionHints(List<SQLCommentHint> optionHints) {
        this.optionHints = optionHints;
    }

    public SQLName getTableGroup() {
        return tableGroup;
    }

    public void setTableGroup(SQLName tableGroup) {
        this.tableGroup = tableGroup;
    }

    public boolean isWithImplicitTablegroup() {
        return withImplicitTablegroup;
    }

    public void setWithImplicitTablegroup(boolean withImplicitTablegroup) {
        this.withImplicitTablegroup = withImplicitTablegroup;
    }

    public SQLName getJoinGroup() {
        return joinGroup;
    }

    public void setJoinGroup(SQLName joinGroup) {
        this.joinGroup = joinGroup;
    }

    @Override
    public void simplify() {
        tableOptions.clear();
        tblProperties.clear();
        super.simplify();
    }

    public void showCoumns(Appendable out) throws IOException {
        this.accept(new MySqlShowColumnOutpuVisitor(out));
    }

    public List<MySqlKey> getMysqlKeys() {
        List<MySqlKey> mySqlKeys = new ArrayList<MySqlKey>();
        for (SQLTableElement element : this.getTableElementList()) {
            if (element instanceof MySqlKey) {
                mySqlKeys.add((MySqlKey) element);
            }
        }
        return mySqlKeys;
    }

    public List<MySqlTableIndex> getMysqlIndexes() {
        List<MySqlTableIndex> indexList = new ArrayList<MySqlTableIndex>();
        for (SQLTableElement element : this.getTableElementList()) {
            if (element instanceof MySqlTableIndex) {
                indexList.add((MySqlTableIndex) element);
            }
        }
        return indexList;
    }

    public boolean apply(MySqlRenameTableStatement x) {
        for (MySqlRenameTableStatement.Item item : x.getItems()) {
            if (apply(item)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean apply(SQLAlterTableStatement alter) {
        if (!SQLUtils.nameEquals(alter.getName(), this.getName())) {
            return false;
        }

        int applyCount = 0;

        // alter collate pre handle if not exist charset
        boolean hasCollate = false;
        boolean hasCharset = false;
        SQLAssignItem collateItem = null;
        for (SQLAssignItem option : alter.getTableOptions()) {
            if (isCharsetLabel(option)) {
                hasCharset = true;
                break;
            }
            if (option.getTarget().toString().contains("COLLATE")) {
                hasCollate = true;
                collateItem = option;
            }
        }
        if (hasCollate && !hasCharset) {
            String dstCollateName = null;
            if (collateItem.getTarget() instanceof SQLIdentifierExpr) {
                dstCollateName = collateItem.getValue().toString();
            } else if (collateItem.getTarget() instanceof SQLBinaryOpExpr) {
                dstCollateName = ((SQLBinaryOpExpr) collateItem.getTarget()).getRight().toString();
            }
            CharsetNameForParser charsetNameForParser = CollationNameForParser.getCharsetOf(dstCollateName);
            alter.getTableOptions().add(new SQLAssignItem(new SQLIdentifierExpr("CHARSET"),
                new SQLIdentifierExpr(charsetNameForParser.name())));
        }

        // alter table charset before modify column
        if (hasCollate || hasCharset) {
            beforeAlterCharset();
            applyCount++;
        }

        List<SQLAlterTableItem> laterItemList = Lists.newArrayList();

        List<SQLAlterTableItem> changeList = Lists.newArrayList();

        // drop first
        for (SQLAlterTableItem item : alter.getItems()) {
            if (item instanceof SQLAlterTableDropColumnItem) {
                if (alterApply(item)) {
                    applyCount++;
                }
            } else {
                changeList.add(item);
            }
        }

        // modify or change column
        for (SQLAlterTableItem item : changeList) {

            if (item instanceof MySqlAlterTableChangeColumn) {
                // later apply first or after， split column def and seq
                MySqlAlterTableChangeColumn sortColumn = (MySqlAlterTableChangeColumn) item;
                MySqlAlterTableChangeColumn changeColumn = new MySqlAlterTableChangeColumn();
                prepareColumnDefinitionWithoutCharset(sortColumn.getNewColumnDefinition());
                changeColumn.setColumnName(sortColumn.getColumnName());
                changeColumn.setNewColumnDefinition(sortColumn.getNewColumnDefinition());
                item = changeColumn;
                sortColumn.setColumnName(sortColumn.getNewColumnDefinition().getName());
                laterItemList.add(sortColumn);
            } else if (item instanceof SQLAlterTableAddColumn) {
                SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) item;
                for (SQLColumnDefinition columnDefinition : addColumn.getColumns()) {
                    prepareColumnDefinitionWithoutCharset(columnDefinition);
                }
                laterItemList.add(item);
                continue;
            } else if (item instanceof MySqlAlterTableModifyColumn) {
                MySqlAlterTableModifyColumn sortColumn = (MySqlAlterTableModifyColumn) item;
                MySqlAlterTableModifyColumn modifyColumn = new MySqlAlterTableModifyColumn();
                prepareColumnDefinitionWithoutCharset(sortColumn.getNewColumnDefinition());
                modifyColumn.setSourceColumn(sortColumn.getSourceColumn());
                modifyColumn.setParent(sortColumn.getParent());
                modifyColumn.setHint(sortColumn.getHint());
                modifyColumn.setSourceLine(sortColumn.getSourceLine());
                modifyColumn.setNewColumnDefinition(sortColumn.getNewColumnDefinition());
                laterItemList.add(sortColumn);
                item = modifyColumn;
            } else if (item instanceof SQLAlterTableConvertCharSet) {
                SQLAlterTableConvertCharSet convertCharSet = (SQLAlterTableConvertCharSet) item;
                String targetCharset = null;
                String targetCollate = null;
                if (convertCharSet.getCharset() != null) {
                    targetCharset = convertCharSet.getCharset().toString();
                }
                if (convertCharSet.getCollate() != null) {
                    targetCollate = convertCharSet.getCollate().toString();
                }
                // clean and follow table charset
                setColumnCharacter(targetCharset, targetCollate, true);
                removeCharsetOptions();
                if (convertCharSet.getCharset() != null) {
                    tableOptions.add(
                        new SQLAssignItem(new SQLIdentifierExpr("CHARSET"), convertCharSet.getCharset()));
                }
                if (convertCharSet.getCollate() != null) {
                    tableOptions.add(new SQLAssignItem(new SQLIdentifierExpr("COLLATE"), convertCharSet.getCharset()));
                }
                applyCount++;
            }

            if (alterApply(item)) {
                applyCount++;
            }
        }

        // sort column and add new column
        for (SQLAlterTableItem item : laterItemList) {
            if (alterApply(item)) {
                applyCount++;
            }
        }

        // alter table charset
        for (SQLAssignItem option : alter.getTableOptions()) {
            if (isCharsetRelLabel(option)) {
                option.setParent(this);
                applyCount++;
                tableOptions.add(option);
            }
        }

        return applyCount > 0;
    }

    private void prepareColumnDefinitionWithoutCharset(SQLColumnDefinition newColumnDefinition) {
        String charset = null;
        String collate = null;
        final SQLExpr charsetExpr = newColumnDefinition.getCharsetExpr();
        if (charsetExpr != null) {
            if (charsetExpr instanceof SQLIdentifierExpr) {
                charset = ((SQLIdentifierExpr) charsetExpr).getName();
            } else if (charsetExpr instanceof SQLBinaryOpExpr) {
                charset = ((SQLBinaryOpExpr) charsetExpr).getRight().toString();
            }
        }
        final SQLExpr collateExpr = newColumnDefinition.getCollateExpr();
        if (collateExpr != null) {
            if (collateExpr instanceof SQLIdentifierExpr) {
                collate = ((SQLIdentifierExpr) collateExpr).getName();

            } else if (collateExpr instanceof SQLBinaryOpExpr) {
                collate = ((SQLBinaryOpExpr) collateExpr).getRight().toString();
            }
        }
        final SQLDataType dataType = newColumnDefinition.getDataType();
        if (dataType instanceof SQLCharacterDataType) {
            SQLCharacterDataType sqlCharacterDataType = (SQLCharacterDataType) dataType;
            if (collate == null) {
                collate = sqlCharacterDataType.getCollate();
            }
            if (charset == null) {
                charset = sqlCharacterDataType.getCharSetName();
            }
        }
        if (collate != null && charset == null) {
            CharsetNameForParser charsetNameForParser = CollationNameForParser.getCharsetOf(collate);
            if (dataType instanceof SQLCharacterDataType) {
                ((SQLCharacterDataType) dataType).setCharSetName(charsetNameForParser.name());
            } else {
                newColumnDefinition.setCharsetExpr(new SQLIdentifierExpr(charsetNameForParser.name()));
            }
        }
    }

    private void beforeAlterCharset() {
        String charset = null;
        String collate = null;
        for (SQLAssignItem option : tableOptions) {
            if (isCharsetLabel(option)) {
                charset = option.getValue().toString();
            } else if (StringUtils.equalsIgnoreCase(option.getTarget().toString(), "COLLATE")) {
                collate = option.getValue().toString();
            }
        }
        if (charset != null) {
            setColumnCharacter(charset, collate, false);
        }

        removeCharsetOptions();
    }

    private static SQLCharacterDataType getSqlCharacterDataType(SQLColumnDefinition column) {
        SQLCharacterDataType dataType = (SQLCharacterDataType) column.getDataType();
        SQLCharacterDataType newDataType = null;
        if (dataType.nameHashCode64() == FnvHash.Constants.NCHAR) {
            newDataType = new SQLCharacterDataType("CHAR", dataType.getLength());
            newDataType.setCharSetName("utf8");
        } else if (dataType.nameHashCode64() == FnvHash.Constants.NVARCHAR) {
            newDataType = new SQLCharacterDataType("VARCHAR", dataType.getLength());
            newDataType.setCharSetName("utf8");
        }
        return newDataType;
    }

    private void setColumnCharacter(String character, String collate, boolean forceConvert) {
        for (SQLColumnDefinition column : this.getColumnDefinitions()) {
            if (forceConvert && column.getDataType() instanceof SQLCharacterDataType) {
                // convert to character 语法会掉用到这里，所以这里要判断一下nchar/nvarchar，转换成普通类型)
                SQLCharacterDataType newDataType = getSqlCharacterDataType(column);
                if (newDataType != null) {
                    column.setDataType(newDataType);
                }
            }
            if (charsetSensitive(column.getDataType())) {
                SQLCharacterDataType characterDataType = null;
                if (column.getDataType() instanceof SQLCharacterDataType) {
                    characterDataType = (SQLCharacterDataType) column.getDataType();
                }
                if (!isCanChangeCharset(column, forceConvert)) {
                    continue;
                }
                if (forceConvert && characterDataType != null) {
                    // 强制转换编码，text 视编码情况升级类型
                    doSpecialConvertDataType(characterDataType,
                        column.getCharsetExpr() == null ? null : column.getCharsetExpr().toString(), character);
                }
                if (StringUtils.isNotBlank(character)) {
                    column.setCharsetExpr(new SQLIdentifierExpr(character));
                    setCharacterSet(characterDataType, character);
                } else {
                    column.setCharsetExpr(null);
                    setCharacterSet(characterDataType, null);
                }
                if (StringUtils.isNotBlank(collate)) {
                    column.setCollateExpr(new SQLIdentifierExpr(collate));
                    setCollate(characterDataType, collate);
                } else {
                    column.setCollateExpr(null);
                    setCollate(characterDataType, null);
                }
            }
        }
    }

    private void doSpecialConvertDataType(SQLCharacterDataType characterDataType, String srcCharsetName,
                                          String targetCharsetName) {
        long nameHashCode = characterDataType.nameHashCode64();
        if (nameHashCode == FnvHash.Constants.TINYTEXT ||
            nameHashCode == FnvHash.Constants.TEXT ||
            nameHashCode == FnvHash.Constants.MEDIUMTEXT) {
            String oldCharsetName = characterDataType.getCharSetName();
            if (StringUtils.isBlank(oldCharsetName)) {
                oldCharsetName = srcCharsetName;
            }
            if (StringUtils.isBlank(oldCharsetName)) {
                oldCharsetName = getCharsetOptions();
            }
            CharsetNameForParser oldCharset = CharsetNameForParser.of(SQLUtils.normalize(oldCharsetName));
            CharsetNameForParser targetCharset = CharsetNameForParser.of(SQLUtils.normalize(targetCharsetName));
            if (oldCharset.getMaxLen() < targetCharset.getMaxLen()) {
                if (nameHashCode == FnvHash.Constants.TINYTEXT) {
                    characterDataType.setName("TEXT");
                } else if (nameHashCode == FnvHash.Constants.TEXT) {
                    characterDataType.setName("MEDIUMTEXT");
                } else {
                    characterDataType.setName("LONGTEXT");
                }
            }
        }
    }

    private String getCharsetOptions() {
        for (SQLAssignItem option : tableOptions) {
            if (isCharsetLabel(option)) {
                SQLExpr expr = option.getValue();
                if (expr instanceof SQLBinaryOpExpr) {
                    SQLBinaryOpExpr opExpr = (SQLBinaryOpExpr) expr;
                    return opExpr.getLeft().toString();
                }
                return option.getValue().toString();
            }
        }
        return null;
    }

    private void setCharacterSet(SQLCharacterDataType characterDataType, String charset) {
        if (characterDataType != null && StringUtils.isNotBlank(characterDataType.getCharSetName())) {
            characterDataType.setCharSetName(charset);
        }
    }

    private void setCollate(SQLCharacterDataType characterDataType, String collate) {
        if (characterDataType != null && StringUtils.isNotBlank(characterDataType.getCollate())) {
            characterDataType.setCollate(collate);
        }
    }

    private boolean isCanChangeCharset(SQLColumnDefinition column, boolean force) {
        if (force) {
            return true;
        }
        if (column.getCharsetExpr() != null) {
            return false;
        }
        if (column.getDataType() instanceof SQLCharacterDataType) {
            SQLCharacterDataType characterDataType = (SQLCharacterDataType) column.getDataType();
            if (StringUtils.isNotBlank(characterDataType.getCharSetName())) {
                return false;
            }
        }

        return true;
    }

    private boolean charsetSensitive(SQLDataType dataType) {
        String dataTypeName = dataType.getName();
        // 除了character外， enum 和 set 是为了兼容mysql返回值
        if (StringUtils.equalsIgnoreCase(dataTypeName, "enum") ||
            StringUtils.equalsIgnoreCase(dataTypeName, "set")) {
            return true;
        }
        if (!(dataType instanceof SQLCharacterDataType)) {
            return false;
        }
        SQLCharacterDataType characterDataType = (SQLCharacterDataType) dataType;
        long nameHash = characterDataType.nameHashCode64();
        return nameHash != FnvHash.Constants.NCHAR && nameHash != FnvHash.Constants.NVARCHAR;
    }

    private void removeCharsetOptions() {
        tableOptions.removeIf(this::isCharsetRelLabel);
    }

    private boolean isCharsetLabel(SQLAssignItem option) {
        for (String charsetLabel : CHARSET_LABEL) {
            if (StringUtils.contains(option.getTarget().toString(), charsetLabel)) {
                return true;
            }
        }
        return false;
    }

    private boolean isCharsetRelLabel(SQLAssignItem option) {
        for (String charsetLabel : CHARSET_REL_LABEL) {
            if (StringUtils.contains(option.getTarget().toString(), charsetLabel)) {
                return true;
            }
        }
        return false;
    }

    private static final String[] CHARSET_REL_LABEL = new String[] {"CHARSET", "CHARACTER", "COLLATE"};
    private static final String[] CHARSET_LABEL = new String[] {"CHARSET", "CHARACTER"};

    @Override
    protected boolean alterApply(SQLAlterTableItem item) {
        if (item instanceof MySqlAlterTableAlterColumn) {
            return apply((MySqlAlterTableAlterColumn) item);

        } else if (item instanceof MySqlAlterTableChangeColumn) {
            return apply((MySqlAlterTableChangeColumn) item);

        } else if (item instanceof SQLAlterCharacter) {
            return apply((SQLAlterCharacter) item);

        } else if (item instanceof MySqlAlterTableModifyColumn) {
            return apply((MySqlAlterTableModifyColumn) item);

        } else if (item instanceof MySqlAlterTableOption) {
            return apply((MySqlAlterTableOption) item);
        }

        return super.alterApply(item);
    }

    public boolean apply(SQLAlterTableAddIndex item) {
        SQLName name = item.getIndexDefinition().getName();
        if (name != null) {
            long nameHashCode = name.nameHashCode64();
            for (int i = 0; i < tableElementList.size(); i++) {
                SQLTableElement e = tableElementList.get(i);
                if (e instanceof MySqlTableIndex) {
                    SQLName name1 = ((MySqlTableIndex) e).getName();
                    if (name1 != null && name1.nameHashCode64() == nameHashCode) {
                        return false;
                    }
                }
            }
        }

        if (item.isUnique()) {
            MySqlUnique x = new MySqlUnique();
            item.cloneTo(x);
            x.setParent(this);
            this.tableElementList.add(x);
            return true;
        }

        if (item.isKey()) {
            MySqlKey x = new MySqlKey();
            item.cloneTo(x);
            x.setParent(this);
            this.tableElementList.add(x);
            return true;
        }

        MySqlTableIndex x = new MySqlTableIndex();
        item.cloneTo(x);
        x.setParent(this);
        this.tableElementList.add(x);
        return true;
    }

    public boolean apply(MySqlAlterTableOption item) {
        if (StringUtils.equalsIgnoreCase(item.getName(), "algorithm")) {
            return false;
        }
        addOption(item.getName(), (SQLExpr) item.getValue());
        return true;
    }

    public boolean apply(SQLAlterCharacter item) {
        SQLExpr charset = item.getCharacterSet();
        if (charset != null) {
            addOption("CHARACTER SET", charset);
        }

        SQLExpr collate = item.getCollate();
        if (collate != null) {
            addOption("COLLATE", collate);
        }
        return true;
    }

    public boolean apply(MySqlRenameTableStatement.Item item) {
        if (!SQLUtils.nameEquals((SQLName) item.getName(), this.getName())) {
            return false;
        }
        this.setName((SQLName) item.getTo().clone());
        return true;
    }

    public boolean apply(MySqlAlterTableAlterColumn x) {
        int columnIndex = columnIndexOf(x.getColumn());
        if (columnIndex == -1) {
            return false;
        }

        SQLExpr defaultExpr = x.getDefaultExpr();
        SQLColumnDefinition column = (SQLColumnDefinition) tableElementList.get(columnIndex);

        if (x.isDropDefault()) {
            column.setDefaultExpr(null);
        } else if (defaultExpr != null) {
            column.setDefaultExpr(defaultExpr);
        }

        return true;
    }

    public boolean apply(MySqlAlterTableChangeColumn item) {
        SQLName columnName = item.getColumnName();
        int columnIndex = columnIndexOf(columnName);
        if (columnIndex == -1) {
            return false;
        }

        int afterIndex = columnIndexOf(item.getAfterColumn());
        int beforeIndex = columnIndexOf(item.getFirstColumn());

        int insertIndex = -1;
        if (beforeIndex != -1) {
            insertIndex = beforeIndex;
        } else if (afterIndex != -1) {
            insertIndex = afterIndex + 1;
        } else if (item.isFirst()) {
            insertIndex = 0;
        }

        SQLColumnDefinition column = item.getNewColumnDefinition().clone();
        column.setParent(this);
        if (insertIndex == -1 || insertIndex == columnIndex) {
            tableElementList.set(columnIndex, column);
        } else {
            if (insertIndex > columnIndex) {
                tableElementList.add(insertIndex, column);
                tableElementList.remove(columnIndex);
            } else {
                tableElementList.remove(columnIndex);
                tableElementList.add(insertIndex, column);
            }
        }

        for (int i = 0; i < tableElementList.size(); i++) {
            SQLTableElement e = tableElementList.get(i);
            if (e instanceof MySqlTableIndex) {
                ((MySqlTableIndex) e).applyColumnRename(columnName, column);
            } else if (e instanceof SQLUnique) {
                SQLUnique unique = (SQLUnique) e;
                unique.applyColumnRename(columnName, column);
            }
        }

        return true;
    }

    public boolean apply(MySqlAlterTableModifyColumn item) {
        SQLColumnDefinition column = item.getNewColumnDefinition().clone();
        SQLName columnName = column.getName();

        int columnIndex = columnIndexOf(columnName);
        if (columnIndex == -1) {
            return false;
        }

        int afterIndex = columnIndexOf(item.getAfterColumn());
        int beforeIndex = columnIndexOf(item.getFirstColumn());

        int insertIndex = -1;
        if (beforeIndex != -1) {
            insertIndex = beforeIndex;
        } else if (afterIndex != -1) {
            insertIndex = afterIndex + 1;
        }

        if (item.isFirst()) {
            insertIndex = 0;
        }

        column.setParent(this);
        if (insertIndex == -1 || insertIndex == columnIndex) {
            tableElementList.set(columnIndex, column);
            return true;
        } else {
            if (insertIndex > columnIndex) {
                tableElementList.add(insertIndex, column);
                tableElementList.remove(columnIndex);
            } else {
                tableElementList.remove(columnIndex);
                tableElementList.add(insertIndex, column);
            }
        }

        // Check key length like change.
        // Just old name -> old name.
        for (int i = 0; i < tableElementList.size(); i++) {
            SQLTableElement e = tableElementList.get(i);
            if (e instanceof MySqlTableIndex) {
                ((MySqlTableIndex) e).applyColumnRename(columnName, column);
            } else if (e instanceof SQLUnique) {
                SQLUnique unique = (SQLUnique) e;
                unique.applyColumnRename(columnName, column);
            }
        }

        return true;
    }

    public void cloneTo(MySqlCreateTableStatement x) {
        super.cloneTo(x);
        if (partitioning != null) {
            x.setPartitioning(partitioning.clone());
        }
        if (localPartitioning != null) {
            x.setLocalPartitioning(localPartitioning.clone());
        }
        for (SQLCommentHint hint : hints) {
            SQLCommentHint h2 = hint.clone();
            h2.setParent(x);
            x.hints.add(h2);
        }
        for (SQLCommentHint hint : optionHints) {
            SQLCommentHint h2 = hint.clone();
            h2.setParent(x);
            x.optionHints.add(h2);
        }
        if (like != null) {
            x.setLike(like.clone());
        }
        if (tableGroup != null) {
            x.setTableGroup(tableGroup.clone());
        }

        if (joinGroup != null) {
            x.setJoinGroup(joinGroup.clone());
        }

        x.setPrefixPartition(prefixPartition);
        x.setPrefixBroadcast(prefixBroadcast);
        x.setPrefixSingle(prefixSingle);

        x.setBroadCast(isBroadCast);
        x.setSingle(isSingle);

        if (dbPartitionBy != null) {
            x.setDbPartitionBy(dbPartitionBy.clone());
        }

        if (dbPartitions != null) {
            x.setDbPartitionBy(dbPartitions.clone());
        }

        if (tablePartitionBy != null) {
            x.setTablePartitionBy(tablePartitionBy.clone());
        }

        if (tablePartitions != null) {
            x.setTablePartitions(tablePartitions.clone());
        }

        if (exPartition != null) {
            x.setExPartition(exPartition.clone());
        }

        if (archiveBy != null) {
            x.setArchiveBy(archiveBy.clone());
        }

        if (distributeByType != null) {
            x.setDistributeByType(distributeByType.clone());
        }

        if (routeBy != null) {
            for (SQLIdentifierExpr expr : routeBy) {
                x.getRouteBy().add(expr.clone());
            }

            if (effectedBy != null) {
                x.setEffectedBy(effectedBy.clone());
            }
        }

        if (distributeByType != null) {
            for (SQLName sqlName : distributeBy) {
                x.getDistributeBy().add(sqlName.clone());
            }
        }

    }

    public MySqlCreateTableStatement clone() {
        MySqlCreateTableStatement x = new MySqlCreateTableStatement();
        cloneTo(x);
        return x;
    }

    public boolean isPrefixPartition() {
        return prefixPartition;
    }

    public void setPrefixPartition(boolean prefixPartition) {
        this.prefixPartition = prefixPartition;
    }

    public boolean isPrefixBroadcast() {
        return prefixBroadcast;
    }

    public void setPrefixBroadcast(boolean prefixBroadcast) {
        this.prefixBroadcast = prefixBroadcast;
    }

    public boolean isPrefixSingle() {
        return prefixSingle;
    }

    public void setPrefixSingle(boolean prefixSingle) {
        this.prefixSingle = prefixSingle;
    }

    public SQLExpr getDbPartitionBy() {
        return dbPartitionBy;
    }

    public void setDbPartitionBy(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.dbPartitionBy = x;
    }

    public SQLExpr getTablePartitionBy() {
        return tablePartitionBy;
    }

    public void setTablePartitionBy(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.tablePartitionBy = x;
    }

    public SQLName getDistributeByType() {
        return distributeByType;
    }

    public void setDistributeByType(SQLName distributeByType) {
        this.distributeByType = distributeByType;
    }

    public List<SQLName> getDistributeBy() {
        return distributeBy;
    }

    public SQLExpr getTbpartitions() {
        return tablePartitions;
    }

    public SQLExpr getTablePartitions() {
        return tablePartitions;
    }

    public void setTablePartitions(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.tablePartitions = x;
    }

    public SQLExpr getDbpartitions() {
        return dbPartitions;
    }

    public void setDbPartitions(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.dbPartitions = x;
    }

    public MySqlExtPartition getExtPartition() {
        return exPartition;
    }

    public void setExPartition(MySqlExtPartition x) {
        if (x != null) {
            x.setParent(this);
        }
        this.exPartition = x;
    }

    public List<SQLIdentifierExpr> getRouteBy() {
        return routeBy;
    }

    public SQLIdentifierExpr getEffectedBy() {
        return effectedBy;
    }

    public void setEffectedBy(SQLIdentifierExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.effectedBy = x;
    }

    public SQLExpr getDbPartitions() {
        return dbPartitions;
    }

    public SQLName getStoredBy() {
        return storedBy;
    }

    public void setStoredBy(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.storedBy = x;
    }

    public Map<String, SQLName> getWith() {
        return with;
    }

    public boolean isBroadCast() {
        return isBroadCast;
    }

    public void setBroadCast(boolean broadCast) {
        isBroadCast = broadCast;
    }

    public boolean isSingle() {
        return isSingle;
    }

    public void setSingle(boolean single) {
        isSingle = single;
    }

    public SQLName getArchiveBy() {
        return archiveBy;
    }

    public void setArchiveBy(SQLName x) {
        if (x != null) {
            x.setParent(this);
        }
        this.archiveBy = x;
    }

    public Boolean getWithData() {
        return withData;
    }

    public void setWithData(Boolean withData) {
        this.withData = withData;
    }

    public void setLocality(SQLExpr locality) {
        this.locality = locality;
    }

    public SQLExpr getLocality() {
        return this.locality;
    }

    public void setAutoSplit(SQLExpr autoSplit) {
        this.autoSplit = autoSplit;
    }

    public SQLExpr getAutoSplit() {
        return this.autoSplit;
    }

    public void setPageChecksum(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        addOption("PAGE_CHECKSUM", x);
    }

    public void setTransactional(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        addOption("TRANSACTIONAL", x);
    }


    private final Set<String> VALID_TABLE_OPTIONS = new HashSet<String>(Arrays.asList(
        "AUTO_INCREMENT", "AVG_ROW_LENGTH", "CHARACTER SET", "DEFAULT CHARACTER SET", "CHARSET", "DEFAULT CHARSET",
        "CHECKSUM", "COLLATE", "DEFAULT COLLATE", "COMMENT", "COMPRESSION", "CONNECTION", "DATA DIRECTORY",
        "DATA DIRECTORY", "DELAY_KEY_WRITE", "ENCRYPTION", "ENGINE", "ENGINE_ATTRIBUTE", "INSERT_METHOD",
        "KEY_BLOCK_SIZE", "MAX_ROWS", "MIN_ROWS", "PACK_KEYS", "PASSWORD", "ROW_FORMAT", "SECONDARY_ENGINE_ATTRIBUTE",
        "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES", "TABLESPACE", "UNION",
        // Extra added.
        "BLOCK_FORMAT", "STORAGE_TYPE", "STORAGE_POLICY", "SECURITY_POLICY"));

    public void normalizeTableOptions() {
        final Iterator<SQLAssignItem> iterator = tableOptions.iterator();
        while (iterator.hasNext()) {
            final SQLAssignItem item = iterator.next();
            if (!VALID_TABLE_OPTIONS.contains(((SQLIdentifierExpr) item.getTarget()).getName().toUpperCase())) {
                iterator.remove();
            }
        }
    }

}
