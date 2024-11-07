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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdMaxRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Map;

import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.getParametersMapForOptimizer;

public class DrdsRelMdMaxRowCount extends RelMdMaxRowCount {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.MAX_ROW_COUNT.method, new DrdsRelMdMaxRowCount());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdMaxRowCount.class);

    @Override
    public Double getMaxRowCount(RelSubset rel, RelMetadataQuery mq) {
        if (rel.getOriginal() == null) {
            return Double.POSITIVE_INFINITY;
        }

        Double lowest = Double.POSITIVE_INFINITY;
        Double maxRowCount = mq.getMaxRowCount(rel.getOriginal());
        if (maxRowCount == null) {
            return null;
        }
        if (lowest.compareTo(maxRowCount) > 0) {
            lowest = maxRowCount;
        }

        return lowest;
    }

    @Override
    public Double getMaxRowCount(Sort rel, RelMetadataQuery mq) {
        Double rowCount = mq.getMaxRowCount(rel.getInput());
        if (rowCount == null) {
            rowCount = Double.POSITIVE_INFINITY;
        }
        Map<Integer, ParameterContext> params = getParametersMapForOptimizer(rel);

        long offset = 0;
        if (rel.offset != null) {
            offset = CBOUtil.getRexParam(rel.offset, params);
        }

        rowCount = Math.max(rowCount - offset, 0D);

        if (rel.fetch != null) {
            long limit = CBOUtil.getRexParam(rel.fetch, params);
            if (limit < rowCount) {
                return (double) limit;
            }
        }
        return rowCount;
    }

    public Double getMaxRowCount(TableLookup rel, RelMetadataQuery mq) {
        if (rel.isRelPushedToPrimary()) {
            return mq.getMaxRowCount(rel.getProject());
        } else {
            return mq.getMaxRowCount(rel.getJoin().getLeft());
        }
    }

    public Double getMaxRowCount(ViewPlan rel, RelMetadataQuery mq) {
        return mq.getMaxRowCount(rel.getPlan());
    }

    public Double getMaxRowCount(LogicalExpand rel, RelMetadataQuery mq) {
        return mq.getMaxRowCount(rel.getInput());
    }

    public Double getMaxRowCount(RuntimeFilterBuilder rel, RelMetadataQuery mq) {
        return mq.getMaxRowCount(rel.getInput());
    }

    public Double getMaxRowCount(MysqlTableScan rel, RelMetadataQuery mq) {
        return mq.getMaxRowCount(rel.getNodeForMetaQuery());
    }
}
