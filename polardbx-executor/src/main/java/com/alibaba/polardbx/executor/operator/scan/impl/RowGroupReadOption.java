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

package com.alibaba.polardbx.executor.operator.scan.impl;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RowGroupReadOption {
    private Path path;
    private long stripeId;
    private boolean[] rowGroupIncluded;
    private boolean[] columnsIncluded;

    public RowGroupReadOption(Path path, long stripeId, boolean[] rowGroupIncluded, boolean[] columnsIncluded) {
        this.path = path;
        this.stripeId = stripeId;
        this.rowGroupIncluded = rowGroupIncluded;
        this.columnsIncluded = columnsIncluded;
    }

    public void resetRowGroups(Random random, double ratio) {
        // fill the row group bitmap according to ratio
        for (int i = 0; i < rowGroupIncluded.length * ratio; i++) {
            rowGroupIncluded[i] = true;
        }
    }

    public Path getPath() {
        return path;
    }

    public long getStripeId() {
        return stripeId;
    }

    public boolean[] getRowGroupIncluded() {
        return rowGroupIncluded;
    }

    public boolean[] getColumnsIncluded() {
        return columnsIncluded;
    }

    @Override
    public String toString() {
        return "RowGroupReadOption{" +
            "path=" + path +
            ", stripeId=" + stripeId +
            ", rowGroupIncluded=" + toList(rowGroupIncluded) +
            ", columnsIncluded=" + toList(columnsIncluded) +
            '}';
    }

    private static List<Integer> toList(boolean[] array) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < array.length; i++) {
            if (array[i]) {
                result.add(i);
            }
        }
        return result;
    }
}
