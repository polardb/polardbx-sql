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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;

import java.io.Serializable;
import java.util.List;

public class LoadData implements Serializable {
    private String sql;
    private boolean isLocal;
    private String charset;
    private String lineTerminatedBy;
    private String fieldTerminatedBy;
    private String enclose;
    private String escape;
    private List<String> columnsList;
    private boolean ignore;
    private int ignoreLineNumber;
    private String linesStartingBy;
    private boolean replace;
    private String originLineTerminatedBy;
    private List<Integer> outputColumnsIndex;
    private String originFieldTerminatedBy;
    //laze init
    private long skippedEmptyLines;
    private ByteString templateSql;

    private List<ColumnMeta> columnMetas;

    /**
     * -1 means no need to auto fill auto increment column
     */
    private int autoFillColumnIndex = -1;

    private boolean swapColumns;

    public LoadData(String sql) {
        this.sql = sql;
    }

    public String getOriginFieldTerminatedBy() {
        return originFieldTerminatedBy;
    }

    public void setOriginFieldTerminatedBy(String originFieldTerminatedBy) {
        this.originFieldTerminatedBy = originFieldTerminatedBy;
    }

    public List<Integer> getOutputColumnsIndex() {
        return outputColumnsIndex;
    }

    public void setOutputColumnsIndex(List<Integer> outputColumnsIndex) {
        this.outputColumnsIndex = outputColumnsIndex;
    }

    public String getOriginLineTerminatedBy() {
        return originLineTerminatedBy;
    }

    public void setOriginLineTerminatedBy(String originLineTerminatedBy) {
        this.originLineTerminatedBy = originLineTerminatedBy;
    }

    public boolean isReplace() {
        return replace;
    }

    public void setReplace(boolean replace) {
        this.replace = replace;
    }

    public String getLinesStartingBy() {
        return linesStartingBy;
    }

    public void setLinesStartingBy(String linesStartingBy) {
        this.linesStartingBy = linesStartingBy;
    }

    public int getIgnoreLineNumber() {
        return ignoreLineNumber;
    }

    public void setIgnoreLineNumber(int ignoreLineNumber) {
        this.skippedEmptyLines = ignoreLineNumber;
        this.ignoreLineNumber = ignoreLineNumber;
    }

    public boolean isIgnore() {
        return ignore;
    }

    public void setIgnore(boolean ignore) {
        this.ignore = ignore;
    }

    public List<String> getColumnsList() {
        return columnsList;
    }

    public void setColumnsList(List<String> columnsList) {
        this.columnsList = columnsList;
    }

    public String getEscape() {
        return escape;
    }

    public void setEscape(String escape) {
        this.escape = escape;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public void setLocal(boolean local) {
        this.isLocal = local;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getLineTerminatedBy() {
        return lineTerminatedBy;
    }

    public void setLineTerminatedBy(String lineTerminatedBy) {
        this.lineTerminatedBy = lineTerminatedBy;
    }

    public String getFieldTerminatedBy() {
        return fieldTerminatedBy;
    }

    public void setFieldTerminatedBy(String fieldTerminatedBy) {
        this.fieldTerminatedBy = fieldTerminatedBy;
    }

    public String getEnclose() {
        return enclose;
    }

    public void setEnclose(String enclose) {
        this.enclose = enclose;
    }

    public void incrementEmptyLine() {
        skippedEmptyLines += 1;
    }

    public long getSkippedEmptyLines() {
        return skippedEmptyLines;
    }

    public ByteString getTemplateSql() {
        return templateSql;
    }

    public void setTemplateSql(ByteString templateSql) {
        this.templateSql = templateSql;
    }

    public String getSql() {
        return sql;
    }

    public List<ColumnMeta> getColumnMetas() {
        return columnMetas;
    }

    public void setColumnMetas(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public int getAutoFillColumnIndex() {
        return autoFillColumnIndex;
    }

    public void setAutoFillColumnIndex(int autoFillColumnIndex) {
        this.autoFillColumnIndex = autoFillColumnIndex;
    }

    public boolean isSwapColumns() {
        return swapColumns;
    }

    public void setSwapColumns(boolean swapColumns) {
        this.swapColumns = swapColumns;
    }
}
