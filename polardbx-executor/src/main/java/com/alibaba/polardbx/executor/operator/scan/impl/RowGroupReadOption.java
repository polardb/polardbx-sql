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
