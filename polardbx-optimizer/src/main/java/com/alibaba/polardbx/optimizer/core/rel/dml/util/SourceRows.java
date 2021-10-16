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

import java.util.List;

/**
 * @author chenmo.cm
 */
public class SourceRows {
    public List<List<Object>> selectedRows;
    public List<DuplicateCheckResult> valueRows;

    public static SourceRows createFromSelect(List<List<Object>> selectedRows) {
        final SourceRows sourceRows = new SourceRows();
        sourceRows.selectedRows = selectedRows;
        return sourceRows;
    }

    public static SourceRows createFromValues(List<DuplicateCheckResult> classifiedRows) {
        final SourceRows sourceRows = new SourceRows();
        sourceRows.valueRows = classifiedRows;
        return sourceRows;
    }
}
