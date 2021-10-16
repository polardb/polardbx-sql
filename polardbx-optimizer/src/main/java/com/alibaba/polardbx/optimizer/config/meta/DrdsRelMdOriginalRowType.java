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

import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import java.util.List;

public class DrdsRelMdOriginalRowType implements MetadataHandler<BuiltInMetadata.OriginalRowType> {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ORIGINAL_ROW_TYPE.method,
            new DrdsRelMdOriginalRowType());

    @Override
    public MetadataDef<BuiltInMetadata.OriginalRowType> getDef() {
        return BuiltInMetadata.OriginalRowType.DEF;
    }

    public RelDataType getOriginalRowType(SingleRel rel, RelMetadataQuery mq) {
        return mq.getOriginalRowType(rel.getInput());
    }

    public RelDataType getOriginalRowType(LogicalProject rel, RelMetadataQuery mq) {
        return rel.getOriginalRowType();
    }

    public RelDataType getOriginalRowType(Join rel, RelMetadataQuery mq) {
        return SqlValidatorUtil.deriveJoinOriginalRowType(mq.getOriginalRowType(rel.getLeft()),
            mq.getOriginalRowType(rel.getRight()), rel.getJoinType(), rel.getCluster().getTypeFactory(), null,
            rel.getSystemFieldList());
    }

    public RelDataType getOriginalRowType(SemiJoin rel, RelMetadataQuery mq) {
        if (rel.getJoinType() == JoinRelType.LEFT) {
            return SqlValidatorUtil.createLeftSemiJoinOriginalType(
                rel.getCluster().getTypeFactory(),
                rel.getLeft().getRowType(),
                rel.getRight().getRowType(),
                null,
                rel.getSystemFieldList(), 1);
        } else {
            return SqlValidatorUtil.deriveJoinOriginalRowType(
                rel.getLeft().getRowType(),
                null,
                JoinRelType.INNER,
                rel.getCluster().getTypeFactory(),
                null,
                ImmutableList.<RelDataTypeField>of());
        }
    }

    public RelDataType getOriginalRowType(SetOp rel, RelMetadataQuery mq) {
        final List<RelDataType> inputRowTypes = Lists.transform(rel.getInputs(),
            new Function<RelNode, RelDataType>() {
                public RelDataType apply(RelNode input) {
                    return mq.getOriginalRowType(input);
                }
            });
        final RelDataType rowType =
            rel.getCluster().getTypeFactory().leastRestrictive(inputRowTypes);
        if (rowType == null) {
            throw new IllegalArgumentException("Cannot compute compatible row type "
                + "for arguments to set op: "
                + Util.sepList(inputRowTypes, ", "));
        }
        return rowType;
    }

    public RelDataType getOriginalRowType(TableLookup rel, RelMetadataQuery mq) {
        return mq.getOriginalRowType(rel.getProject());
    }

    public RelDataType getOriginalRowType(Aggregate rel, RelMetadataQuery mq) {
        return SqlValidatorUtil.deriveAggOriginalRowType(rel.getCluster().getTypeFactory(), rel.getInput().getRowType(),
            rel.indicator, rel.getGroupSet(), rel.getAggCallList());
    }

    public RelDataType getOriginalRowType(LogicalView rel, RelMetadataQuery mq) {
        return mq.getOriginalRowType(rel.getPushedRelNode());
    }

    public RelDataType getOriginalRowType(MysqlTableScan rel, RelMetadataQuery mq) {
        return mq.getOriginalRowType(rel.getNodeForMetaQuery());
    }

    public RelDataType getOriginalRowType(BroadcastTableModify rel, RelMetadataQuery mq) {
        return mq.getOriginalRowType(rel.getDirectTableOperation());
    }

    /**
     * catch all
     **/
    public RelDataType getOriginalRowType(RelNode rel, RelMetadataQuery mq) {
        return rel.getRowType();
    }
}
