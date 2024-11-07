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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 每一个索引（或者叫KV更直白点）的描述
 *
 */
public class IndexMeta implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;
    /**
     * 表名+列名
     */
    private final String name;
    /**
     * 列名字，mysql 80 有可能为空list，函数索引没有列名。
     */
    private final List<ColumnMeta> keyColumns; // Keep this for legacy code.
    private final List<IndexColumnMeta> keyColumnsExt;

    /**
     * 值名字
     */
    private final List<ColumnMeta> valueColumns;

    /**
     * 当前index的类型 @IndexType
     */
    private final IndexType indexType;

    /**
     * 关系，用来处理多对多关系，暂时没有用到。 see IndexType
     */
    private final Relationship relationship;

    /**
     * 是否强同步,目前只支持主key
     */
    private final boolean isStronglyConsistent;
    private final boolean isPrimaryKeyIndex;

    /**
     * 是否是 unique 索引
     */
    private final boolean isUniqueIndex;

    /**
     * 物理表上的 index name
     */
    private final String physicalIndexName;

    // ================== 冗余字段 ==============

    /**
     * 表名+index名的方式进行命名的。 在查询时，会先根据.之前的，拿到表名，然后找到对应的schema。
     * 然后再根据.之后的，找到对应的schema
     */
    private final String tableName;

    /**
     * 保存了所有列，方便查找
     */
    private Map<String, ColumnMeta> columnsMap;

    public IndexMeta(String tableName, List<ColumnMeta> keys, List<ColumnMeta> values, IndexType indexType,
                     Relationship relationship, boolean isStronglyConsistent, boolean isPrimaryKeyIndex,
                     boolean isUniqueIndex, String physicalIndexName) {
        this.tableName = tableName;
        this.keyColumns = uniq(keys);
        this.keyColumnsExt = new ArrayList<>(this.keyColumns.size());
        for (ColumnMeta meta : this.keyColumns) {
            this.keyColumnsExt.add(new IndexColumnMeta(meta, 0)); // Default no sub-part.
        }
        this.valueColumns = uniq(values);
        this.indexType = indexType;
        this.relationship = relationship;
        this.isPrimaryKeyIndex = isPrimaryKeyIndex;
        this.isStronglyConsistent = isStronglyConsistent;
        this.isUniqueIndex = isUniqueIndex;
        this.physicalIndexName = physicalIndexName;
        this.name = buildName(tableName, keys);
        this.columnsMap = buildColumnsMap();
    }

    public IndexMeta(String tableName, List<IndexColumnMeta> keys, List<ColumnMeta> values, IndexType indexType,
                     Relationship relationship, boolean isStronglyConsistent, boolean isUniqueIndex,
                     String physicalIndexName) {
        this.tableName = tableName;
        this.keyColumnsExt = uniqExt(keys);
        this.keyColumns = new ArrayList<>(this.keyColumnsExt.size());
        for (IndexColumnMeta meta : this.keyColumnsExt) {
            this.keyColumns.add(meta.getColumnMeta());
        }
        this.valueColumns = uniq(values);
        this.indexType = indexType;
        this.relationship = relationship;
        this.isPrimaryKeyIndex = false;
        this.isStronglyConsistent = isStronglyConsistent;
        this.isUniqueIndex = isUniqueIndex;
        this.physicalIndexName = physicalIndexName;
        this.name = buildName(tableName, this.keyColumns);
        this.columnsMap = buildColumnsMap();
    }

    private Map<String, ColumnMeta> buildColumnsMap() {
        Map<String, ColumnMeta> columnsMap = new HashMap<String, ColumnMeta>();
        if (valueColumns != null) {
            for (ColumnMeta cm : valueColumns) {
                String name = cm.getName();
                columnsMap.put(name.toUpperCase(), cm);
                columnsMap.put(name.toLowerCase(), cm);
            }
        }

        if (keyColumns != null) {
            for (ColumnMeta cm : keyColumns) {
                String name = cm.getName();
                columnsMap.put(name.toUpperCase(), cm);
                columnsMap.put(name.toLowerCase(), cm);
            }
        }

        return columnsMap;
    }

    private String buildName(String tableName, List<ColumnMeta> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(".");
        for (ColumnMeta column : columns) {
            sb.append("_" + column.getName());
        }
        return sb.toString();
    }

    public ColumnMeta getColumnMeta(String name) {
        ColumnMeta column = columnsMap.get(name);
        if (column == null) {
            column = columnsMap.get(name.toUpperCase());
        }
        return column;
    }

    public String getName() {
        if (isPrimaryKeyIndex) {
            return tableName; // 如果是主键索引，直接返回逻辑表名
        } else {
            return name; // 否则返回索引名，逻辑表名+字段
        }
    }

    public List<ColumnMeta> getKeyColumns() {
        return keyColumns;
    }

    public List<IndexColumnMeta> getKeyColumnsExt() {
        return keyColumnsExt;
    }

    public List<ColumnMeta> getValueColumns() {
        return valueColumns;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public Relationship getRelationship() {
        return relationship;
    }

    public boolean isStronglyConsistent() {
        return isStronglyConsistent;
    }

    public boolean isPrimaryKeyIndex() {
        return isPrimaryKeyIndex;
    }

    public boolean isUniqueIndex() {
        return isUniqueIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPhysicalIndexName() {
        return physicalIndexName;
    }

    public String getNameWithOutDot() {
        return Util.replace(name, ".", "$");
    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    /**
     * 根据列名获取对应的index key column
     */
    public ColumnMeta getKeyColumn(String name) {
        for (ColumnMeta column : keyColumns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }

        return null;
    }

    /**
     * 根据列名获取对应的index value column
     */
    public ColumnMeta getValueColumn(String name) {
        for (ColumnMeta column : valueColumns) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }

        return null;
    }

    private List<ColumnMeta> uniq(List<ColumnMeta> s) {
        if (s == null) {
            return null;
        }

        List<ColumnMeta> uniqList = new ArrayList<ColumnMeta>(s.size());
        for (ColumnMeta cm : s) {
            if (!uniqList.contains(cm)) {
                uniqList.add(cm);
            }
        }

        return uniqList;
    }

    private List<IndexColumnMeta> uniqExt(List<IndexColumnMeta> s) {
        if (s == null) {
            return null;
        }

        List<IndexColumnMeta> uniqList = new ArrayList<>(s.size());
        for (IndexColumnMeta cm : s) {
            if (!uniqList.contains(cm)) {
                uniqList.add(cm);
            }
        }

        return uniqList;
    }

    public String toStringWithInden(int inden) {
        StringBuilder sb = new StringBuilder();
        String tabTittle = GeneralUtil.getTab(inden);
        sb.append(tabTittle).append("[");
        sb.append("indexMeta name : ").append(name).append("\n");
        buildMetas(inden, sb, keyColumns, "keyColumn :");
        buildMetas(inden, sb, valueColumns, "valueColumn :");
        sb.append(tabTittle).append("]");
        return sb.toString();
    }

    private void buildMetas(int inden, StringBuilder sb, List<ColumnMeta> metas, String keyName) {
        if (metas != null) {
            String tabContent = GeneralUtil.getTab(inden + 1);
            sb.append(tabContent).append(keyName).append("\n");
            String content = GeneralUtil.getTab(inden + 2);
            sb.append(content);
            for (ColumnMeta meta : metas) {
                sb.append(meta.toStringWithInden(0));
                sb.append(" ");
            }

            sb.append("\n");
        }
    }

    public boolean isCoverShardKey(List<String> shardKeys) {
        if (shardKeys.size() > keyColumns.size()) {
            return false;
        }

        for (int i = 0; i < shardKeys.size(); ++i) {
            if (!StringUtils.equalsIgnoreCase(keyColumns.get(i).getName(), shardKeys.get(i))) {
                return false;
            }
        }
        return true;
    }

}
