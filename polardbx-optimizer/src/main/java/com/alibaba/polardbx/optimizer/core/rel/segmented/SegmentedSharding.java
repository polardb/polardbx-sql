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

package com.alibaba.polardbx.optimizer.core.rel.segmented;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取并行度信息，根据直方图信息计算当前应该使用的并行数量，并行数量可以高于并行度（多余的任务处于待调度状态）
 * 目前构造方法中没有加入并行度信息，需要再处理
 *
 * @author hongxi.chx
 */
public class SegmentedSharding {

    private int maxCountParallel;

    private final static Object SEGMENT_PARAM_SPLIT_FLAG = new Object();
    private int size;
    private List<Object> parameters;
    private boolean parallel;
    private String tableName;

    public SegmentedSharding(String schemaName, String localTableName, List<String> primaryKeys, int size) {
        maxCountParallel = size;
        //TODO replace getHistograms api with statistic api.
        final List<Histogram> histograms = OptimizerContext.getContext(schemaName)
            .getStatisticManager().getHistograms(localTableName, primaryKeys);
        if (histograms == null || histograms.size() == 0) {
            size = 0;
            return;
        }
        //联合主键只取第一个字段，因为单独第二个字段起不到很好的加速作用，故忽略
        for (Histogram histogram : histograms) {
            final List<Histogram.Bucket> buckets = histogram.getBuckets();
            int bucketSize = buckets.size();
            int actualCount = bucketSize + 2;
            if (actualCount > maxCountParallel) {
                actualCount = maxCountParallel;
            }

            if (actualCount <= 1) {
                return;
            }
            parameters = new ArrayList<>();
            parallel = true;
            int skip = 0;
            if (bucketSize > actualCount) {
                skip = bucketSize / actualCount;
            }
            for (int i = 0; i < actualCount - 1; i++) {
                final int index = i * skip + skip;
                final Histogram.Bucket bucketPre = buckets.get(index);
                Object start = bucketPre.getLower();
                if (i == 0) {
                    //构造小于start
                    this.parameters.add(start);
                    this.parameters.add(SEGMENT_PARAM_SPLIT_FLAG);
                }
                if (i < actualCount - 2) {
                    final Histogram.Bucket bucketPost = buckets.get(index + skip);
                    Object end = bucketPost.getLower();
                    this.parameters.add(start);
                    this.parameters.add(end);
                    this.parameters.add(SEGMENT_PARAM_SPLIT_FLAG);
                } else {
                    this.parameters.add(start);
                    //同时构建 > end 条件
                    this.parameters.add(SEGMENT_PARAM_SPLIT_FLAG);
                }

            }
            this.size = actualCount;

        }
        this.tableName = localTableName;

    }

    public static boolean isParamsSplitFlag(Object obj) {
        return SEGMENT_PARAM_SPLIT_FLAG == obj;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public List<Object> getParameters() {
        if (!parallel) {
            return null;
        }
        return parameters;

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getMaxCountParallel() {
        return maxCountParallel;
    }
}
