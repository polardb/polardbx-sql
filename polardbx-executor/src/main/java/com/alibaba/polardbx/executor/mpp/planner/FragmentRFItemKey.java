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

package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FragmentRFItemKey {
    private final String buildColumnName;
    private final String probeColumnName;
    private final int buildIndex;
    private final int probeIndex;

    public FragmentRFItemKey(String buildColumnName, String probeColumnName, int buildIndex, int probeIndex) {
        this.buildColumnName = buildColumnName;
        this.probeColumnName = probeColumnName;
        this.buildIndex = buildIndex;
        this.probeIndex = probeIndex;
    }

    /**
     * Get the list of the FragmentRFItemKey object sorted by buildIndex field.
     *
     * @param join the RelNode of join.
     */
    public static List<FragmentRFItemKey> buildItemKeys(Join join) {
        boolean isOuterBuild = (join instanceof HashJoin && ((HashJoin) join).isOuterBuild())
            || (join instanceof SemiHashJoin && ((SemiHashJoin) join).isOuterBuild());

        // Don't support runtime filter for anti join without reverse.
        if (join.getJoinType() == JoinRelType.ANTI && !isOuterBuild) {
            // empty set.
            return new ArrayList<>();
        }

        RelNode buildNode = join.getInner();
        RelNode probeNode = join.getOuter();

        RexNode equalCondition = null;
        if (join instanceof HashJoin) {
            equalCondition = ((HashJoin) join).getEqualCondition();
        } else if (join instanceof SemiHashJoin) {
            equalCondition = ((SemiHashJoin) join).getEqualCondition();
        }
        JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), equalCondition);

        ImmutableIntList buildKeys, probeKeys;
        if (buildNode == join.getLeft()) {
            buildKeys = joinInfo.leftKeys;
            probeKeys = joinInfo.rightKeys;
        } else {
            buildKeys = joinInfo.rightKeys;
            probeKeys = joinInfo.leftKeys;
        }

        Preconditions.checkArgument(buildKeys.size() == probeKeys.size());

        RelDataType buildType = buildNode.getRowType();
        RelDataType probeType = probeNode.getRowType();

        List<String> buildColumnNames =
            buildKeys.stream()
                .map(k -> buildType.getFieldNames().get(k))
                .collect(Collectors.toList());

        List<String> probeColumnNames =
            probeKeys.stream()
                .map(k -> probeType.getFieldNames().get(k))
                .collect(Collectors.toList());

        List<FragmentRFItemKey> itemKeys = new ArrayList<>();

        for (int i = 0; i < buildKeys.size(); i++) {
            // Check if the dataType of build and probe key are integer types and equal.
            RelDataType buildDataType = buildType.getFieldList().get(buildKeys.get(i)).getType();
            RelDataType probeDataType = probeType.getFieldList().get(probeKeys.get(i)).getType();

            if (SqlTypeUtil.isIntType(buildDataType) && SqlTypeUtil.isIntType(probeDataType)
                && buildDataType.getSqlTypeName() == probeDataType.getSqlTypeName()) {

                if (!isOuterBuild) {
                    // for not reversed join.
                    itemKeys.add(new FragmentRFItemKey(
                        buildColumnNames.get(i),
                        probeColumnNames.get(i),
                        buildKeys.get(i),
                        probeKeys.get(i)
                    ));

                } else {
                    // For reversed join, swap the build/probe sides.
                    itemKeys.add(new FragmentRFItemKey(
                        probeColumnNames.get(i),
                        buildColumnNames.get(i),
                        probeKeys.get(i),
                        buildKeys.get(i)
                    ));

                }

            }
        }

        Collections.sort(itemKeys, Comparator.comparing(FragmentRFItemKey::getBuildIndex));

        return itemKeys;
    }

    public String getBuildColumnName() {
        return buildColumnName;
    }

    public String getProbeColumnName() {
        return probeColumnName;
    }

    public int getBuildIndex() {
        return buildIndex;
    }

    public int getProbeIndex() {
        return probeIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FragmentRFItemKey itemKey = (FragmentRFItemKey) o;
        return buildIndex == itemKey.buildIndex && probeIndex == itemKey.probeIndex && Objects.equals(
            buildColumnName, itemKey.buildColumnName) && Objects.equals(probeColumnName, itemKey.probeColumnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildColumnName, probeColumnName, buildIndex, probeIndex);
    }

    @Override
    public String toString() {
        return "FragmentRFItemKey{" +
            "buildColumnName='" + buildColumnName + '\'' +
            ", probeColumnName='" + probeColumnName + '\'' +
            ", buildIndex=" + buildIndex +
            ", probeIndex=" + probeIndex +
            '}';
    }
}
