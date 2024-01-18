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

package com.alibaba.polardbx.executor.archive.reader;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.pruning.PruningResult;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import org.apache.orc.TypeDescription;
import org.apache.orc.sarg.SearchArgument;

import java.util.Arrays;
import java.util.List;

public class OSSReadOption {
    private TypeDescription readSchema;

    private OSSColumnTransformer ossColumnTransformer;

    private SearchArgument searchArgument;

    private String physicalTableName;
    private Engine engine;
    private String[] columns;

    private List<String> tableFileList;
    private List<FileMeta> phyTableFileMetas;
    private List<PruningResult> pruningResultList;

    private long maxMergeDistance;

    public OSSReadOption(TypeDescription readSchema, OSSColumnTransformer ossColumnMapping,
                         SearchArgument searchArgument, String[] columns, String tableName, Engine engine,
                         List<String> tableFileList, List<FileMeta> phyTableFileMetas,
                         List<PruningResult> pruningResultList,
                         long maxMergeDistance) {
        this.readSchema = readSchema;
        this.ossColumnTransformer = ossColumnMapping;
        this.searchArgument = searchArgument;
        this.columns = columns;
        this.physicalTableName = tableName;
        this.engine = engine;
        this.tableFileList = tableFileList;
        this.phyTableFileMetas = phyTableFileMetas;
        this.pruningResultList = pruningResultList;
        this.maxMergeDistance = maxMergeDistance;
    }

    public Engine getEngine() {
        return engine;
    }

    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    public List<String> getTableFileList() {
        return tableFileList;
    }

    public String getPhysicalTableName() {
        return physicalTableName;
    }

    public TypeDescription getReadSchema() {
        return readSchema;
    }

    public void setReadSchema(TypeDescription readSchema) {
        this.readSchema = readSchema;
    }

    public SearchArgument getSearchArgument() {
        return searchArgument;
    }

    public void setSearchArgument(SearchArgument searchArgument) {
        this.searchArgument = searchArgument;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public void setPhysicalTableName(String physicalTableName) {
        this.physicalTableName = physicalTableName;
    }

    public void setTableFileList(List<String> tableFileList) {
        this.tableFileList = tableFileList;
    }

    public List<FileMeta> getPhyTableFileMetas() {
        return phyTableFileMetas;
    }

    public void setPhyTableFileMetas(List<FileMeta> phyTableFileMetas) {
        this.phyTableFileMetas = phyTableFileMetas;
    }

    public OSSColumnTransformer getOssColumnTransformer() {
        return ossColumnTransformer;
    }

    public List<PruningResult> getPruningResultList() {
        return pruningResultList;
    }

    public long getMaxMergeDistance() {
        return maxMergeDistance;
    }

    @Override
    public String toString() {
        return String.format(
            "[fields: %s, searchArg: %s, columns: %s, table: %s]",
            readSchema.toString(),
            searchArgument.toString(),
            Arrays.toString(columns),
            physicalTableName
        );
    }
}