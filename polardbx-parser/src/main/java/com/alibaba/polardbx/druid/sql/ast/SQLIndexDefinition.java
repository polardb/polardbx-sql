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

package com.alibaba.polardbx.druid.sql.ast;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 */
public class SQLIndexDefinition extends SQLObjectImpl implements SQLIndex {

    // for polardb-x partition manage
    protected SQLPartitionBy partitioning;
    /**
     * [CONSTRAINT [symbol]] [GLOBAL|LOCAL|CLUSTERED] [FULLTEXT|SPATIAL|UNIQUE|PRIMARY] [INDEX|KEY]
     * [index_name] [index_type] (key_part,...) [COVERING (col_name,...)] [index_option] ...
     */

    private boolean hasConstraint;
    private SQLName symbol;
    private boolean global;
    private boolean local;
    private boolean clustered;
    private boolean columnar;
    private String type;
    private boolean hashMapType; //for ads
    private boolean hashType; //for ads
    private boolean index;
    private boolean key;
    private SQLName name;
    private SQLTableSource table;
    private List<SQLSelectOrderByItem> columns = new ArrayList<SQLSelectOrderByItem>();
    private SQLIndexOptions options;
    // DRDS
    private SQLExpr dbPartitionBy;
    private SQLExpr tbPartitionBy;
    private SQLExpr tbPartitions;
    private List<SQLName> covering = new ArrayList<>();
    private List<SQLName> clusteredKeys = new ArrayList<>();
    private SQLName tableGroup;
    private boolean withImplicitTablegroup;
    private boolean visible = true;

    // For fulltext index when create table.
    private SQLName analyzerName;
    private SQLName indexAnalyzerName;
    private SQLName queryAnalyzerName;
    private SQLName withDicName;
    // for columnar index
    private SQLName engineName;
    // Compatible layer.
    private List<SQLAssignItem> compatibleOptions = new ArrayList<SQLAssignItem>();
    private final Map<String, String> columnarOptions = new HashMap<>();

    public Map<String, String> getColumnarOptions() {
        return columnarOptions;
    }

    public SQLName getEngineName() {
        return engineName;
    }

    public void setEngineName(SQLName engineName) {
        this.engineName = engineName;
    }

    public boolean hasConstraint() {
        return hasConstraint;
    }

    public void setHasConstraint(boolean hasConstraint) {
        this.hasConstraint = hasConstraint;
    }

    public SQLName getSymbol() {
        return symbol;
    }

    public void setSymbol(SQLName symbol) {
        if (symbol != null) {
            if (getParent() != null) {
                symbol.setParent(getParent());
            } else {
                symbol.setParent(this);
            }
        }
        this.symbol = symbol;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setVisible(boolean visible) {
        this.visible = visible;
    }

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }

    public boolean isClustered() {
        return clustered;
    }

    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

    public boolean isColumnar() {
        return columnar;
    }

    public void setColumnar(boolean columnar) {
        this.columnar = columnar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isHashMapType() {
        return hashMapType;
    }

    public void setHashMapType(boolean hashMapType) {
        this.hashMapType = hashMapType;
    }

    public boolean isHashType() {
        return hashType;
    }

    public void setHashType(boolean hashType) {
        this.hashType = hashType;
    }

    public boolean isIndex() {
        return index;
    }

    public void setIndex(boolean index) {
        this.index = index;
    }

    public boolean isKey() {
        return key;
    }

    public void setKey(boolean key) {
        this.key = key;
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            if (getParent() != null) {
                name.setParent(getParent());
            } else {
                name.setParent(this);
            }
        }
        this.name = name;
    }

    public SQLTableSource getTable() {
        return table;
    }

    public void setTable(SQLTableSource table) {
        if (table != null) {
            if (getParent() != null) {
                table.setParent(getParent());
            } else {
                table.setParent(this);
            }
        }
        this.table = table;
    }

    @Override
    public List<SQLSelectOrderByItem> getColumns() {
        return columns;
    }

    public void setColumns(List<SQLSelectOrderByItem> columns) {
        this.columns = columns;
    }

    public boolean hasOptions() {
        return options != null;
    }

    public SQLIndexOptions getOptions() {
        if (null == options) {
            options = new SQLIndexOptions();
            options.setParent(this);
        }
        return options;
    }

    public SQLExpr getDbPartitionBy() {
        return dbPartitionBy;
    }

    public void setDbPartitionBy(SQLExpr dbPartitionBy) {
        if (dbPartitionBy != null) {
            if (getParent() != null) {
                dbPartitionBy.setParent(getParent());
            } else {
                dbPartitionBy.setParent(this);
            }
        }
        this.dbPartitionBy = dbPartitionBy;
    }

    public SQLExpr getTbPartitionBy() {
        return tbPartitionBy;
    }

    public void setTbPartitionBy(SQLExpr tbPartitionBy) {
        if (tbPartitionBy != null) {
            if (getParent() != null) {
                tbPartitionBy.setParent(getParent());
            } else {
                tbPartitionBy.setParent(this);
            }
        }
        this.tbPartitionBy = tbPartitionBy;
    }

    public SQLExpr getTbPartitions() {
        return tbPartitions;
    }

    public void setTbPartitions(SQLExpr tbPartitions) {
        if (tbPartitions != null) {
            if (getParent() != null) {
                tbPartitions.setParent(getParent());
            } else {
                tbPartitions.setParent(this);
            }
        }
        this.tbPartitions = tbPartitions;
    }

    @Override
    public List<SQLName> getCovering() {
        return covering;
    }

    public void setCovering(List<SQLName> covering) {
        this.covering = covering;
    }

    @Override
    public List<SQLName> getClusteredKeys() {
        return clusteredKeys;
    }

    public void setClusteredKeys(List<SQLName> clusteredKeys) {
        this.clusteredKeys = clusteredKeys;
    }

    public SQLName getAnalyzerName() {
        return analyzerName;
    }

    public void setAnalyzerName(SQLName analyzerName) {
        if (analyzerName != null) {
            if (getParent() != null) {
                analyzerName.setParent(getParent());
            } else {
                analyzerName.setParent(this);
            }
        }
        this.analyzerName = analyzerName;
    }

    public SQLName getIndexAnalyzerName() {
        return indexAnalyzerName;
    }

    public void setIndexAnalyzerName(SQLName indexAnalyzerName) {
        if (indexAnalyzerName != null) {
            if (getParent() != null) {
                indexAnalyzerName.setParent(getParent());
            } else {
                indexAnalyzerName.setParent(this);
            }
        }
        this.indexAnalyzerName = indexAnalyzerName;
    }

    public SQLName getQueryAnalyzerName() {
        return queryAnalyzerName;
    }

    public void setQueryAnalyzerName(SQLName queryAnalyzerName) {
        if (queryAnalyzerName != null) {
            if (getParent() != null) {
                queryAnalyzerName.setParent(getParent());
            } else {
                queryAnalyzerName.setParent(this);
            }
        }
        this.queryAnalyzerName = queryAnalyzerName;
    }

    public SQLName getWithDicName() {
        return withDicName;
    }

    public void setWithDicName(SQLName withDicName) {
        if (withDicName != null) {
            if (getParent() != null) {
                withDicName.setParent(getParent());
            } else {
                withDicName.setParent(this);
            }
        }
        this.withDicName = withDicName;
    }

    public List<SQLAssignItem> getCompatibleOptions() {
        return compatibleOptions;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (name != null) {
                name.accept(visitor);
            }

            for (final SQLSelectOrderByItem item : columns) {
                if (item != null) {
                    item.accept(visitor);
                }
            }
            for (final SQLName item : covering) {
                if (item != null) {
                    item.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

    public void cloneTo(SQLIndexDefinition definition) {
        SQLObject parent;
        if (definition.getParent() != null) {
            parent = definition.getParent();
        } else {
            parent = definition;
        }
        definition.hasConstraint = hasConstraint;
        if (symbol != null) {
            definition.symbol = symbol.clone();
            definition.symbol.setParent(parent);
        }
        definition.global = global;
        definition.local = local;
        definition.clustered = clustered;
        definition.type = type;
        definition.hashMapType = hashMapType;
        definition.index = index;
        definition.key = key;
        definition.visible = visible;
        definition.columnar = columnar;
        if (name != null) {
            definition.name = name.clone();
            definition.name.setParent(parent);
        }
        if (table != null) {
            definition.table = table.clone();
            definition.table.setParent(parent);
        }
        for (SQLSelectOrderByItem item : columns) {
            SQLSelectOrderByItem item1 = item.clone();
            item1.setParent(parent);
            definition.columns.add(item1);
        }
        if (options != null) {
            options.cloneTo(definition.getOptions());
        }
        if (dbPartitionBy != null) {
            definition.dbPartitionBy = dbPartitionBy.clone();
            definition.dbPartitionBy.setParent(parent);
        }
        if (tbPartitionBy != null) {
            definition.tbPartitionBy = tbPartitionBy.clone();
            definition.tbPartitionBy.setParent(parent);
        }
        if (tbPartitions != null) {
            definition.tbPartitions = tbPartitions.clone();
            definition.tbPartitions.setParent(parent);
        }
        if (partitioning != null) {
            definition.partitioning = partitioning.clone();
            definition.partitioning.setParent(parent);
        }
        if (tableGroup != null) {
            definition.tableGroup = tableGroup.clone();
            if (withImplicitTablegroup) {
                definition.setWithImplicitTablegroup(withImplicitTablegroup);
            }
        }
        for (SQLName name : covering) {
            SQLName name1 = name.clone();
            name1.setParent(parent);
            definition.covering.add(name1);
        }
        if (analyzerName != null) {
            definition.analyzerName = analyzerName.clone();
            definition.analyzerName.setParent(parent);
        }
        if (indexAnalyzerName != null) {
            definition.indexAnalyzerName = indexAnalyzerName.clone();
            definition.indexAnalyzerName.setParent(parent);
        }
        if (withDicName != null) {
            definition.withDicName = withDicName.clone();
            definition.withDicName.setParent(parent);
        }
        if (queryAnalyzerName != null) {
            definition.queryAnalyzerName = queryAnalyzerName.clone();
            definition.queryAnalyzerName.setParent(parent);
        }
        for (SQLAssignItem item : compatibleOptions) {
            SQLAssignItem item2 = item.clone();
            item2.setParent(parent);
            definition.compatibleOptions.add(item2);
        }
    }

    //
    // Function for compatibility.
    //

    public void addOption(String name, SQLExpr value) {
        SQLAssignItem assignItem = new SQLAssignItem(new SQLIdentifierExpr(name), value);
        if (getParent() != null) {
            assignItem.setParent(getParent());
        } else {
            assignItem.setParent(this);
        }
        getCompatibleOptions().add(assignItem);
    }

    public void addColumnarOption(String name, String value) {
        getColumnarOptions().put(name.toUpperCase(), value.toUpperCase());
    }

    public void addColumnarOption(Map<String, String> options) {
        getColumnarOptions().putAll(options);
    }

    public SQLExpr getOption(String name) {
        if (name == null) {
            return null;
        }

        return getOption(
            FnvHash.hashCode64(name));
    }

    public SQLExpr getOption(long hash64) {
        // Search in compatible list first.
        for (SQLAssignItem item : compatibleOptions) {
            final SQLExpr target = item.getTarget();
            if (target instanceof SQLIdentifierExpr) {
                if (((SQLIdentifierExpr) target).hashCode64() == hash64) {
                    return item.getValue();
                }
            }
        }

        // Now search in new options.
        if (null == options) {
            return null;
        }

        if (hash64 == FnvHash.Constants.KEY_BLOCK_SIZE) {
            return options.getKeyBlockSize();
        } else if (hash64 == FnvHash.Constants.ALGORITHM) {
            if (options.getAlgorithm() != null) {
                return new SQLIdentifierExpr(options.getAlgorithm());
            }
            return null;
        } else if (hash64 == FnvHash.hashCode64("LOCK")) {
            if (options.getLock() != null) {
                return new SQLIdentifierExpr(options.getLock());
            }
            return null;
        }

        for (SQLAssignItem item : options.getOtherOptions()) {
            final SQLExpr target = item.getTarget();
            if (target instanceof SQLIdentifierExpr) {
                if (((SQLIdentifierExpr) target).hashCode64() == hash64) {
                    return item.getValue();
                }
            }
        }

        return null;
    }

    public String getDistanceMeasure() {
        SQLExpr expr = getOption(FnvHash.Constants.DISTANCEMEASURE);
        if (expr == null) {
            return null;
        }

        return expr.toString();
    }

    public String getAlgorithm() {
        if (options != null && options.getAlgorithm() != null) {
            return options.getAlgorithm();
        }

        SQLExpr expr = getOption(FnvHash.Constants.ALGORITHM);
        if (expr == null) {
            return null;
        }

        return expr.toString();
    }

    public SQLPartitionBy getPartitioning() {
        return partitioning;
    }

    public void setPartitioning(SQLPartitionBy partitioning) {
        this.partitioning = partitioning;
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

    public boolean isUnique() {
        return type != null && type.equalsIgnoreCase("UNIQUE");
    }
}
