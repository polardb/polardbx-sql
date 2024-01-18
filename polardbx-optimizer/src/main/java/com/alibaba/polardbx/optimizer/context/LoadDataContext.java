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

package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.rel.SimpleShardProcessor;
import com.alibaba.polardbx.optimizer.utils.LoadDataCacheManager;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class LoadDataContext {

    public static final List<String> END = new ArrayList<>();

    private final LoadDataCacheManager dataCacheManager;
    private final BlockingQueue<List<String>> parameters;
    private final long batchInsertNum;
    private String loadDataSql;
    private AtomicLong loadDataAffectRows = new AtomicLong(0);
    private List<SqlTypeName> valueTypes;
    private Charset charset;
    private String fieldTerminatedBy;
    private List<ColumnMeta> metaList;
    private SimpleShardProcessor shardProcessor;
    private volatile boolean isFinish = false;
    private volatile Throwable throwable;
    private boolean useBatch;
    private int autoFillColumnIndex = -1;
    private boolean inSingleDb;
    private String tableName;
    private boolean swapColumns;
    private boolean gsiInsertTurn;

    /**
     * param value of load data sql should use this, rather than get result from execution context
     */
    private ParamManager paramManager = new ParamManager(new HashMap());

    public LoadDataContext(
        LoadDataCacheManager dataCacheManager,
        BlockingQueue<List<String>> parameters,
        long batchInsertNum, String loadDataSql, List<SqlTypeName> valueTypes,
        String fieldTerminatedBy, Charset character, List<ColumnMeta> metaList, String tableName,
        Map<String, Object> extraCmds,
        int autoFillColumnIndex) {
        this.dataCacheManager = dataCacheManager;
        this.parameters = parameters;
        this.batchInsertNum = batchInsertNum;
        this.loadDataSql = loadDataSql;
        this.valueTypes = valueTypes;
        this.fieldTerminatedBy = fieldTerminatedBy;
        this.charset = character;
        this.metaList = metaList;
        this.tableName = tableName;
        this.paramManager = new ParamManager(extraCmds);
        this.autoFillColumnIndex = autoFillColumnIndex;
    }

    public LoadDataCacheManager getDataCacheManager() {
        return dataCacheManager;
    }

    public BlockingQueue<List<String>> getParameters() {
        return parameters;
    }

    public long getBatchInsertNum() {
        return batchInsertNum;
    }

    public String getLoadDataSql() {
        return loadDataSql;
    }

    public void clear() {
        dataCacheManager.close();
        finish(null);
    }

    public void finish(Throwable t) {
        if (throwable == null && t != null) {
            this.throwable = t;
        }
        this.isFinish = true;
        parameters.clear();
        try {
            parameters.put(END);
        } catch (Throwable e) {
            //ignore
        }
    }

    public boolean isFinish() {
        return isFinish;
    }

    public AtomicLong getLoadDataAffectRows() {
        return loadDataAffectRows;
    }

    public List<SqlTypeName> getValueTypes() {
        return valueTypes;
    }

    public Charset getCharset() {
        return charset;
    }

    public String getFieldTerminatedBy() {
        return fieldTerminatedBy;
    }

    public List<ColumnMeta> getMetaList() {
        return metaList;
    }

    public SimpleShardProcessor getShardProcessor() {
        return shardProcessor;
    }

    public void setShardProcessor(SimpleShardProcessor shardProcessor) {
        this.shardProcessor = shardProcessor;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public boolean isUseBatch() {
        return useBatch;
    }

    public void setUseBatch(boolean useBatch) {
        this.useBatch = useBatch;
    }

    public int getAutoFillColumnIndex() {
        return autoFillColumnIndex;
    }

    public boolean isInSingleDb() {
        return inSingleDb;
    }

    public void setInSingleDb(boolean inSingleDb) {
        this.inSingleDb = inSingleDb;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isSwapColumns() {
        return swapColumns;
    }

    public void setSwapColumns(boolean swapColumns) {
        this.swapColumns = swapColumns;
    }

    public boolean isGsiInsertTurn() {
        return gsiInsertTurn;
    }

    public void setGsiInsertTurn(boolean gsiInsertTurn) {
        this.gsiInsertTurn = gsiInsertTurn;
    }

    public ParamManager getParamManager() {
        return paramManager;
    }
}
