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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Set;

public class DrdsRelMdColumnOrigins extends RelMdColumnOrigins {
    /**
     * make sure you have overridden the SOURCE
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.COLUMN_ORIGIN.method, new DrdsRelMdColumnOrigins());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdColumnOrigins.class);

    public Set<RelColumnOrigin> getColumnOrigins(LogicalView rel, RelMetadataQuery mq, int iOutputColumn) {
        return rel.getColumnOrigins(mq, iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(RelSubset rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getOriginal(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(TableLookup tableLookup, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(tableLookup.getProject(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(ViewPlan viewPlan, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(viewPlan.getPlan(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(MysqlTableScan mysqlTableScan, RelMetadataQuery mq,
                                                 int iOutputColumn) {
        return mq.getColumnOrigins(mysqlTableScan.getNodeForMetaQuery(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(Window rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        if (iOutputColumn < rel.getInput().getRowType().getFieldCount()) {
            return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
        }
        return null;
    }
}