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

import com.google.common.base.Preconditions;
import org.apache.orc.TypeDescription;

import java.util.Arrays;
import java.util.List;

/**
 * orc schema = {column schema + redundant schema}
 * <p>
 * <p>
 * ------------------------------------------
 * PolarDB-X : |         column metas        |
 * --------------------------------------------------------------
 * ORC       : |         column schema       |  redundant schema  |
 * --------------------------------------------------------------
 * <p>
 * -----------------------------------
 * PolarDB-X : |    bf column metas   |
 * --------------------------------------------------------
 * ORC       : |    bf column schema  | redundant bf schema |
 * ---------------------------------------------------------
 */
public class PolarDBXOrcSchema {
    /**
     * Schema = {column schema + redundant schema}
     */
    private TypeDescription schema;
    /**
     * Subset of schema with bloom filter
     * BF Schema = {column bf schema + redundant bf schema}
     */
    private TypeDescription bfSchema;
    /**
     * Column Metas (mapping to column schema)
     */
    private List<ColumnMeta> columnMetas;
    /**
     * Column Metas with bloom filter (mapping to bloom filter schema)
     */
    private List<ColumnMeta> bfColumnMetas;
    /**
     * Subset of column Metas, with redundant column.
     */
    private List<ColumnMeta> redundantColumnMetas;
    /**
     * The column id, from which the child schema represent the sort key.
     */
    private int redundantId;
    /**
     * Mapping from original col pos to redundant col pos.
     * Redundant col pos = -1 means no redundant col for original col.
     */
    private int[] redundantMap;

    public PolarDBXOrcSchema(TypeDescription schema, TypeDescription bfSchema,
                             List<ColumnMeta> columnMetas, List<ColumnMeta> bfColumnMetas,
                             List<ColumnMeta> redundantColumnMetas,
                             int redundantId, int[] redundantMap) {
        Preconditions.checkArgument(schema.getChildren().size() == columnMetas.size() + redundantColumnMetas.size());
        Preconditions.checkArgument(
            bfSchema.getChildren().size() == bfColumnMetas.size() + redundantColumnMetas.size());
        Preconditions.checkArgument(redundantId == columnMetas.size() + 1);
        this.schema = schema;
        this.bfSchema = bfSchema;
        this.columnMetas = columnMetas;
        this.bfColumnMetas = bfColumnMetas;
        this.redundantColumnMetas = redundantColumnMetas;
        this.redundantId = redundantId;
        this.redundantMap = redundantMap;
    }

    public boolean[] buildNoRedundantSchema() {
        boolean[] noRedundantSchemaBitMap = new boolean[schema.getMaximumId() + 1];
        Arrays.fill(noRedundantSchemaBitMap, false);
        for (int columnId = 0; columnId < this.redundantId; columnId++) {
            noRedundantSchemaBitMap[columnId] = true;
        }
        return noRedundantSchemaBitMap;
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public void setSchema(TypeDescription schema) {
        this.schema = schema;
    }

    public TypeDescription getBfSchema() {
        return bfSchema;
    }

    public void setBfSchema(TypeDescription bfSchema) {
        this.bfSchema = bfSchema;
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public List<ColumnMeta> getBfColumnMetas() {
        return bfColumnMetas;
    }

    public void setBfColumnMetas(List<ColumnMeta> bfColumnMetas) {
        this.bfColumnMetas = bfColumnMetas;
    }

    public int getRedundantId() {
        return redundantId;
    }

    public void setRedundantId(int redundantId) {
        this.redundantId = redundantId;
    }

    public int[] getRedundantMap() {
        return redundantMap;
    }

    public void setRedundantMap(int[] redundantMap) {
        this.redundantMap = redundantMap;
    }

    public List<ColumnMeta> getRedundantColumnMetas() {
        return redundantColumnMetas;
    }

    public void setRedundantColumnMetas(
        List<ColumnMeta> redundantColumnMetas) {
        this.redundantColumnMetas = redundantColumnMetas;
    }

    @Override
    public String toString() {
        return "PolarDBXOrcSchema{" +
            "schema=" + schema +
            ", bfSchema=" + bfSchema +
            ", dataTypes=" + columnMetas +
            ", bfDataTypes=" + bfColumnMetas +
            ", redundantId=" + redundantId +
            ", redundantMap=" + Arrays.toString(redundantMap) +
            '}';
    }
}
