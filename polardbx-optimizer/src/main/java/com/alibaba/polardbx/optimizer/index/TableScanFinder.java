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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */
public class TableScanFinder extends RelShuttleImpl {

    private List<Pair<String, TableScan>> result = new ArrayList<>();

    private String schemaName;

    public TableScanFinder() {

    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            this.schemaName = scan.getSchemaName();
            ((LogicalView) scan).getPushedRelNode().accept(this);
        }
        result.add(Pair.of(schemaName, scan));
        return scan;
    }

    public List<Pair<String, TableScan>> getResult() {
        return result;
    }
}
