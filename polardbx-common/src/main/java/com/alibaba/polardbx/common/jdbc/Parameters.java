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

package com.alibaba.polardbx.common.jdbc;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class Parameters implements Serializable {

    private List<Map<Integer, ParameterContext>> batchParams = null;
    private boolean batch = false;
    private Map<Integer, ParameterContext> params = new HashMap<Integer, ParameterContext>();
    private Map<Integer, ParameterContext> listBatchParameters;
    /**
     * Insert 的 Batch 数量
     */
    private int batchSize = 0;

    private int batchIndex = 0;

    private AtomicInteger sequenceSize = new AtomicInteger(0);

    private AtomicInteger sequenceIndex = new AtomicInteger(0);

    private Long sequenceBeginVal = null;

    public Parameters() {
    }

    public Parameters(Map<Integer, ParameterContext> currentParameter) {
        this(currentParameter, false);
    }

    public Parameters(List<Map<Integer, ParameterContext>> batchParams) {
        this.params = batchParams.get(0);
        this.batchParams = batchParams;
        this.batch = true;
        this.batchSize = batchParams.size();
    }

    @JsonCreator
    public Parameters(@JsonProperty("currentParameter") Map<Integer, ParameterContext> currentParameter,
                      @JsonProperty("isBatch") boolean isBatch) {
        this.params = currentParameter;
        this.batch = isBatch;
    }

    @JsonProperty
    public Map<Integer, ParameterContext> getCurrentParameter() {
        return params;
    }

    @JsonIgnore
    @JSONField(serialize = false)
    public Map<Integer, ParameterContext> getFirstParameter() {
        if (!batch) {
            return params;
        }
        return batchParams.get(0);
    }

    @JsonIgnore
    @JSONField(serialize = false)
    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        if (isBatch() && this.batchParams != null) {
            return this.batchParams;
        } else {
            return Collections.singletonList(params);
        }
    }

    public void addParams(Map<Integer, ParameterContext> params) {
        if (this.params != null) {
            this.params.putAll(params);
        } else {
            this.params = params;
        }
    }

    /**
     * used in:
     * 1. X-Driver batch prepare insert/update
     * 2. jdbc-driver prepare batch insert
     */
    public Map<Integer, ParameterContext> getBatchPreparedParameters() {
        if (!batch) {
            return params;
        } else {
            return convertBatchPreparedParameters();
        }
    }

    private synchronized Map<Integer, ParameterContext> convertBatchPreparedParameters() {
        if (this.listBatchParameters != null) {
            return this.listBatchParameters;
        }
        if (batchParams.isEmpty()) {
            this.listBatchParameters = new HashMap<>();
            return this.listBatchParameters;
        }
        final float loadFactor = 0.8f;
        int expectedSize = batchParams.size() * batchParams.get(0).size();
        this.listBatchParameters = new HashMap<>((int) (expectedSize * 1.3 + 1), loadFactor);
        int newIndex = 1;
        for (Map<Integer, ParameterContext> map : batchParams) {
            for (Map.Entry<Integer, ParameterContext> entry : map.entrySet()) {
                ParameterContext oldPc = entry.getValue();
                ParameterContext newPc = new ParameterContext();
                newPc.setParameterMethod(oldPc.getParameterMethod());
                Object[] args = oldPc.getArgs();
                Object[] newArgs = Arrays.copyOf(args, args.length);
                newArgs[0] = newIndex;
                newPc.setArgs(newArgs);
                listBatchParameters.put(newIndex, newPc);
                newIndex++;
            }
        }
        return this.listBatchParameters;
    }

    public void setBatchParams(List<Map<Integer, ParameterContext>> batchParams) {
        this.batch = true;
        this.batchSize = batchParams.size();
        this.batchParams = batchParams;
    }

    public Parameters cloneByBatchIndex(int batchIndex) {
        List<Map<Integer, ParameterContext>> batchs = getBatchParameters();
        if (batchIndex >= batchs.size()) {
            throw new IllegalArgumentException("batchIndex is invalid");
        }

        Parameters parameters = new Parameters(batchs.get(batchIndex), isBatch());
        parameters.batchSize = batchSize;
        parameters.setBatchIndex(batchIndex);
        parameters.setSequenceSize(sequenceSize);
        return parameters;
    }

    public void addBatch() {
        if (batchParams == null) {
            batchParams = new ArrayList();
        }

        batchParams.add(this.params);
        batchSize = batchParams.size();
        params = new HashMap();
        this.batch = true;
    }

    @JsonProperty
    public boolean isBatch() {
        return this.batch;
    }

    public void setBatchIndex(int batchIndex) {
        this.batchIndex = batchIndex;
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public AtomicInteger getSequenceSize() {
        return sequenceSize;
    }

    public void setSequenceSize(AtomicInteger sequenceSize) {
        this.sequenceSize = sequenceSize;
    }

    public static void setParameters(PreparedStatement ps, Map<Integer, ParameterContext> parameterSettings)
        throws SQLException {
        ParameterMethod.setParameters(ps, parameterSettings);
    }

    public void clear() {
        if (batchParams != null) {
            batchParams.clear();
            batch = false;
        }
        batchSize = 0;
        if (params != null) {
            params.clear();
        }
    }

    @Override
    public String toString() {
        if (!this.isBatch()) {
            if (this.params == null || this.params.isEmpty()) {
                return null;
            } else {
                return params.values().toString();
            }
        } else if (null != this.batchParams) {
            return batchParams.toString();
        }
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public AtomicInteger getSequenceIndex() {
        return sequenceIndex;
    }

    public void setSequenceIndex(AtomicInteger sequenceIndex) {
        this.sequenceIndex = sequenceIndex;
    }

    public Long getSequenceBeginVal() {
        return sequenceBeginVal;
    }

    public void setSequenceBeginVal(Long sequenceBeginVal) {
        this.sequenceBeginVal = sequenceBeginVal;
    }

    public void setParams(Map<Integer, ParameterContext> params) {
        this.params = params;
    }

    public Parameters clone() {
        Map<Integer, ParameterContext> newParams = new HashMap<Integer, ParameterContext>(params);
        Parameters parameters = new Parameters(newParams, batch);

        parameters.batchSize = batchSize;
        parameters.batchIndex = batchIndex;
        parameters.sequenceSize = new AtomicInteger(sequenceSize.get());
        parameters.sequenceIndex = new AtomicInteger(sequenceIndex.get());
        parameters.sequenceBeginVal = sequenceBeginVal;

        List<Map<Integer, ParameterContext>> newBatchParams = null;
        if (batchParams != null) {
            newBatchParams = new ArrayList<>(batchParams);
            for (int i = 0; i < newBatchParams.size(); i++) {
                newBatchParams.set(i, new HashMap<>(newBatchParams.get(i)));
            }
        }
        parameters.batchParams = newBatchParams;
        return parameters;
    }
}
