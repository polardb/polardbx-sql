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

package com.alibaba.polardbx.executor.mpp.split;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.SerializeUtils;
import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class JdbcSplit implements ConnectorSplit {

    private final String catalogName;
    private final String schemaName;
    private final String dbIndex;
    private List<List<ParameterContext>> params;
    private byte[] paramsBytes;
    private String hostAddress;
    private List<List<String>> tableNames;
    private ITransaction.RW rw;
    private boolean containSelect;

    protected final String hint;
    protected final String sqlTemplate;
    protected final String orderBy;
    protected transient List<ParameterContext> flattenParams;
    protected transient String hintSql;

    public JdbcSplit(
        String catalogName,
        String schemaName,
        String dbIndex,
        String hint,
        String sqlTemplate,
        String orderBy,
        List<List<ParameterContext>> params,
        String hostAddress,
        List<List<String>> tableNames,
        ITransaction.RW rw,
        boolean containSelect) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.dbIndex = dbIndex;
        this.hint = hint;
        this.params = params;
        this.hostAddress = hostAddress;
        this.tableNames = tableNames;
        this.rw = rw;
        this.sqlTemplate = sqlTemplate;
        this.orderBy = orderBy;
        this.containSelect = containSelect;
    }

    @JsonCreator
    public JdbcSplit(
        @JsonProperty("catalogName") String catalogName,
        @JsonProperty("schemaName") String schemaName,
        @JsonProperty("dbIndex") String dbIndex,
        @JsonProperty("hint") String hint,
        @JsonProperty("sqlTemplate") String sqlTemplate,
        @JsonProperty("orderBy") String orderBy,
        @JsonProperty("paramsBytes") byte[] paramsBytes,
        @JsonProperty("hostAddress") String hostAddress,
        @JsonProperty("tableNames") List<List<String>> tableNames,
        @JsonProperty("rw") String rw,
        @JsonProperty("containSelect") boolean containSelect) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.dbIndex = dbIndex;
        this.hint = hint;
        this.paramsBytes = paramsBytes;
        this.hostAddress = hostAddress;
        this.tableNames = tableNames;
        this.rw = ITransaction.RW.valueOf(rw.toUpperCase());
        this.sqlTemplate = sqlTemplate;
        this.orderBy = orderBy;
        this.containSelect = containSelect;
    }

    public JdbcSplit(JdbcSplit jdbcSplit) {
        this(jdbcSplit.getCatalogName(), jdbcSplit.getSchemaName(), jdbcSplit.getDbIndex(), jdbcSplit.getHint(),
            jdbcSplit.getSqlTemplate(), jdbcSplit.getOrderBy(),
            jdbcSplit.getParams(), jdbcSplit.getHostAddress(), jdbcSplit.getTableNames(), jdbcSplit.getTransactionRw(),
            jdbcSplit.isContainSelect());
    }

    @JsonProperty("rw")
    public String getRw() {
        return rw.toString();
    }

    public ITransaction.RW getTransactionRw() {
        return rw;
    }

    @Override
    @JsonProperty
    public String getHostAddress() {
        return hostAddress;
    }

    @Override
    @JsonIgnore
    public Object getInfo() {
        return this;
    }

    @JsonProperty
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getDbIndex() {
        return dbIndex;
    }

    @JsonProperty
    public String getHint() {
        return hint;
    }

    @JsonProperty
    public String getSqlTemplate() {
        return sqlTemplate;
    }

    @JsonProperty
    public String getOrderBy() {
        return orderBy;
    }

    @JsonIgnore
    public String getHintSql(boolean ignore) {
        if (hintSql == null) {
            hintSql = PhyTableScanBuilder.buildPhysicalQuery(
                tableNames.size(), sqlTemplate, orderBy, hint);
        }
        return hintSql;
    }

    @JsonProperty
    public boolean isContainSelect() {
        return containSelect;
    }

    @JsonIgnore
    public List<ParameterContext> getFlattedParams() {
        if (flattenParams == null) {
            List<List<ParameterContext>> params = getParams();
            flattenParams = new ArrayList<>(params.size() * (params.size() > 0 ? params.get(0).size() : 0));
            for (List<ParameterContext> param : params) {
                flattenParams.addAll(param);
            }
        }
        return flattenParams;
    }

    @JsonIgnore
    public List<List<ParameterContext>> getParams() {
        if (params == null && paramsBytes != null) {
            params = (List<List<ParameterContext>>) SerializeUtils.deFromBytes(paramsBytes, List.class);
        }
        return params;
    }

    @JsonProperty
    public byte[] getParamsBytes() {
        if (paramsBytes == null && params != null) {
            paramsBytes = SerializeUtils.getBytes((Serializable) params);
        }
        return paramsBytes;
    }

    @JsonProperty
    public List<List<String>> getTableNames() {
        return tableNames;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("hostAddress", hostAddress)
            .add("catalogName", catalogName)
            .add("schemaName", schemaName)
            .add("dbIndex", dbIndex)
            .add("hint", hint)
            .add("sqlTemplate", sqlTemplate)
            .add("orderBy", orderBy)
            .add("params", params)
            .toString();
    }

    /**
     * clear tmp objects when query is done
     */
    public void reset() {

    }
}
