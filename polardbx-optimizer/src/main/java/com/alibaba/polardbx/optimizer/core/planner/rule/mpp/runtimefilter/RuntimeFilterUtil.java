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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.alibaba.polardbx.optimizer.core.datatype.CharType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.datatype.TimeType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlRuntimeFilterBuildFunction;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RuntimeFilterUtil {

    /**
     * CN与DN共同支持的RuntimeFilter过滤类型
     */
    private static final Set<DataType<?>> RUNTIME_FILTER_BOTH_SUPPORTED_TYPES = Sets.newHashSet(
        DataTypes.StringType,
        DataTypes.ShortType,
        DataTypes.DoubleType,
        DataTypes.FloatType,
        DataTypes.TinyIntType,
        DataTypes.UTinyIntType,
        DataTypes.SmallIntType,
        DataTypes.USmallIntType,
        DataTypes.MediumIntType,
        DataTypes.UMediumIntType,
        DataTypes.IntegerType,
        DataTypes.UIntegerType,
        DataTypes.LongType,
        DataTypes.ULongType,
        DataTypes.DateType,
        DataTypes.TimestampType,
        DataTypes.DatetimeType,
        DataTypes.TimeType);

    private static final Set<DataType<?>> OSS_MIN_MAX_FILTER_SUPPORTED_DATATYPES = Sets.newHashSet(
        DataTypes.DateType,
        DataTypes.DoubleType,
        DataTypes.FloatType,
        DataTypes.TinyIntType,
        DataTypes.UTinyIntType,
        DataTypes.SmallIntType,
        DataTypes.USmallIntType,
        DataTypes.MediumIntType,
        DataTypes.UMediumIntType,
        DataTypes.IntegerType,
        DataTypes.UIntegerType,
        DataTypes.LongType,
        DataTypes.ULongType);

    public static boolean canPushRuntimeFilterToOss(DataType<?> dataType) {
        if (dataType instanceof VarcharType
            || dataType instanceof CharType
            || dataType instanceof TimestampType
            || dataType instanceof DecimalType
            || dataType instanceof DateTimeType
            || dataType instanceof TimeType) {
            return true;
        }
        return OSS_MIN_MAX_FILTER_SUPPORTED_DATATYPES.contains(dataType);
    }

    /**
     * 仅CN支持的RuntimeFilter过滤类型
     */
    private static final Set<DataType<?>> RUNTIME_FILTER_SUPPORTED_TYPES = Sets.newHashSet(
        DataTypes.DecimalType
    );

    public static boolean canPushRuntimeFilterToMysql(DataType<?> dataType) {
        return RUNTIME_FILTER_BOTH_SUPPORTED_TYPES.contains(dataType);
    }

    public static boolean supportsRuntimeFilter(DataType<?> dataType) {
        // 如果一个类型支持生成的bloomfilter下推到mysql，必定支持类型在polarx这一层生成runtime filter
        if (dataType instanceof VarcharType
            || dataType instanceof CharType
            || dataType instanceof TimestampType
            || dataType instanceof DecimalType
            || dataType instanceof DateTimeType
            || dataType instanceof TimeType) {
            return true;
        }
        return RUNTIME_FILTER_SUPPORTED_TYPES.contains(dataType) ||
            RUNTIME_FILTER_BOTH_SUPPORTED_TYPES.contains(dataType) ||
            OSS_MIN_MAX_FILTER_SUPPORTED_DATATYPES.contains(dataType);
    }

    public static double findMinFpp(double buildNdv, double bloomFilterMaxSize) {
        if (buildNdv <= 0) {
            return BloomFilter.DEFAULT_FPP;
        }
        double minFpp = Math.exp(-3.843 * bloomFilterMaxSize / buildNdv);
        Preconditions.checkArgument(minFpp < 1);
        return Math.max(minFpp, BloomFilter.DEFAULT_FPP);
    }

    public static double calcMaxBloomFilterSize(
        ParamManager paramManager, double probeTotalSize) {
        long broadCastNum = paramManager.getLong(ConnectionParams.BLOOM_FILTER_BROADCAST_NUM);
        double filterRatio = paramManager.getFloat(ConnectionParams.BLOOM_FILTER_RATIO);
        long maxBloomFilterSize = paramManager.getLong(ConnectionParams.BLOOM_FILTER_MAX_SIZE);
        return Math.min(probeTotalSize * filterRatio / broadCastNum, maxBloomFilterSize);
    }

    public static boolean satisfyFilterRatio(
        ParamManager paramManager, double buildNdv, double probeCount, double fpp) {

        return (1 - buildNdv / probeCount) * (1 - fpp) > paramManager
            .getFloat(ConnectionParams.BLOOM_FILTER_RATIO);
    }

    public static void updateBuildFunctionNdv(RelNode input, Collection<RexNode> coditions) {
        RelMetadataQuery mq = input.getCluster().getMetadataQuery();
        for (RexNode rexNode : coditions) {
            SqlRuntimeFilterBuildFunction buildFunction =
                (SqlRuntimeFilterBuildFunction) ((RexCall) rexNode).getOperator();
            List<Integer> keys = new ArrayList<>();
            for (RexNode rexNode1 : ((RexCall) rexNode).getOperands()) {
                keys.add(((RexInputRef) rexNode1).getIndex());
            }
            double buildKeyNdv = convertDouble(mq.getDistinctRowCount(input, ImmutableBitSet.of(keys), null));
            buildFunction.updateNdv(buildKeyNdv);
        }
    }

    public static double convertDouble(Double count) {
        if (count == null) {
            return 0;
        } else {
            return count;
        }
    }
}
