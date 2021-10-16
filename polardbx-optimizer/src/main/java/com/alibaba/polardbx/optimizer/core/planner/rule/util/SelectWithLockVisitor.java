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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;

/**
 * @author chenmo.cm
 */
public class SelectWithLockVisitor extends RelShuttleImpl {
    private final boolean exclusiveLock;

    public SelectWithLockVisitor() {
        this(false);
    }

    public SelectWithLockVisitor(boolean exclusiveLock) {
        this.exclusiveLock = exclusiveLock;
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView && ((LogicalView) scan).getLockMode() == SqlSelect.LockMode.UNDEF) {
            ((LogicalView) scan)
                .setLockMode(exclusiveLock ? SqlSelect.LockMode.EXCLUSIVE_LOCK : SqlSelect.LockMode.SHARED_LOCK);
        }
        return super.visit(scan);
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof HepRelVertex) {
            return ((HepRelVertex) other).getCurrentRel().accept(this);
        }
        if (other instanceof RelSubset) {
            return Util.first(((RelSubset) other).getBest(), ((RelSubset) other).getOriginal()).accept(this);
        }

        return super.visit(other);
    }
}
