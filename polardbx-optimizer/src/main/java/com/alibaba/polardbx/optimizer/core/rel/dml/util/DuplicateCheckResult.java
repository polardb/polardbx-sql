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

package com.alibaba.polardbx.optimizer.core.rel.dml.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class DuplicateCheckResult {
    public boolean duplicated = false;
    public boolean doInsert = false;
    public boolean doReplace = false;
    public boolean doUpdate = false;
    /**
     * True if this.before equals to this.after
     */
    public boolean trivial = false;
    /**
     * For DELETE in UPSERT modify partition key
     */
    public List<Object> before = new ArrayList<>();
    /**
     * For INSERT in UPSERT modify partition key
     */
    public List<Object> after = new ArrayList<>();
    /**
     * For UPDATE in UPSERT
     */
    public List<Object> updateSource = new ArrayList<>();
    /**
     * For INSERT in UPSERT
     */
    public Map<Integer, ParameterContext> insertParam = new HashMap<>();
    public Map<Integer, ParameterContext> onDuplicateKeyUpdateParam = new HashMap<>();
    public int affectedRows = 0;

    public boolean insertThenUpdate() {
        return this.doInsert && this.duplicated;
    }

    public boolean updateOnly() {
        return !this.doInsert && this.duplicated;
    }

    public boolean skipUpdate() {
        return !this.doInsert && this.trivial;
    }
}
