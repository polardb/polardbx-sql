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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.Maps;
import org.apache.calcite.util.NlsString;
import org.apache.commons.collections.map.CaseInsensitiveMap;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 解析环境上下文
 *
 * @author hongxi.chx
 */
public class ContextParameters {

    private HashMap<ContextParameterKey, Object> contextParameters;

    private List<SQLCommentHint> headHints;
    private boolean prepareMode = false;
    final private Deque<Boolean> underSet = new ArrayDeque<>();

    private PrivilegeContext privilegeContext;

    private boolean useHint;

    private Map<String, String> tb2TestNames = new CaseInsensitiveMap(new HashMap<>());

    private boolean testMode;

    public Map<Integer, NlsString> parameterNlsStrings;

    public ContextParameters() {
    }

    public ContextParameters(boolean testMode) {
        contextParameters = Maps.newHashMap();
        this.testMode = testMode;
    }

    public <T> void putParameter(ContextParameterKey contextParametersKey, T value) {
        contextParameters.put(contextParametersKey, value);
    }

    public <T> T getParameter(ContextParameterKey contextParametersKey) {
        return (T) contextParameters.get(contextParametersKey);
    }

    public <T> T removeParameter(ContextParameterKey contextParametersKey) {
        return (T) contextParameters.remove(contextParametersKey);
    }

    public void setHeadHints(List<SQLCommentHint> headHints) {
        this.headHints = headHints;
    }

    public List<SQLCommentHint> getHeadHints() {
        return headHints;
    }

    public boolean isPrepareMode() {
        return prepareMode;
    }

    public void setPrepareMode(boolean prepareMode) {
        this.prepareMode = prepareMode;
    }

    public Deque<Boolean> getUnderSet() {
        return underSet;
    }

    public boolean isUnderSet() {
        return null != underSet.peek() && underSet.peek();
    }

    public boolean hasInExpr(){
        return contextParameters.get(ContextParameterKey.HAS_IN_EXPR)!=null;
    }

    public PrivilegeContext getPrivilegeContext() {
        return privilegeContext;
    }

    public void setPrivilegeContext(PrivilegeContext privilegeContext) {
        this.privilegeContext = privilegeContext;
    }

    public boolean isUseHint() {
        return useHint;
    }

    public void setUseHint(boolean useHint) {
        this.useHint = useHint;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public Map<String, String> getTb2TestNames() {
        return tb2TestNames;
    }

    public Map<Integer, NlsString> getParameterNlsStrings() {
        return parameterNlsStrings;
    }

    public void setParameterNlsStrings(Map<Integer, NlsString> parameterNlsStrings) {
        this.parameterNlsStrings = parameterNlsStrings;
    }
}
