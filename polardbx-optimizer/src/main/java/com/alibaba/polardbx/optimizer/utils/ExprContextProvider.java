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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * A proxy to provide ExecutionContext to others
 *
 * @author chenghui.lch
 */
public class ExprContextProvider {

    protected ExecutionContext context;

    protected DynamicParamProvider dynamicParamProvider;

    protected static class DynamicParamDefaultProvider extends DynamicParamProvider {

        protected ExprContextProvider contextHolder;

        public DynamicParamDefaultProvider(ExprContextProvider contextHolder) {
            this.contextHolder = contextHolder;
        }

        @Override
        public Map<Integer, ParameterContext> getParams() {
            ExecutionContext context = this.contextHolder.getContext();
            Map<Integer, ParameterContext> params = context.getParams() == null ? new HashMap<>() :
                context.getParams().getCurrentParameter();
            return params;
        }
    }

    public ExprContextProvider() {
        this.dynamicParamProvider = new DynamicParamDefaultProvider(this);
    }

    public ExprContextProvider(ExecutionContext context) {
        this.dynamicParamProvider = new DynamicParamDefaultProvider(this);
        this.context = context;
    }

    public ExecutionContext getContext() {
        return context;
    }

    public DynamicParamProvider getDynamicParamProvider() {
        return dynamicParamProvider;
    }

    public void setContext(ExecutionContext context) {
        this.context = context;
    }
}
