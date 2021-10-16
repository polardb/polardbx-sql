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

package com.alibaba.polardbx.transaction.log;

import java.util.List;
import java.util.Map;

public final class ConnectionContext {

    private String              encoding;
    private String              sqlMode;
    private Map<String, Object> variables;

    private List<String> otherSchemas; // Schemas involved in this transaction besides current one

    public ConnectionContext(){
    }

    public ConnectionContext(String encoding, String sqlMode, Map<String, Object> variables){
        this.encoding = encoding;
        this.sqlMode = sqlMode;
        this.variables = variables;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public List<String> getOtherSchemas() {
        return otherSchemas;
    }

    public void setOtherSchemas(List<String> otherSchemas) {
        this.otherSchemas = otherSchemas;
    }
}
