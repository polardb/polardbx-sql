package org.apache.calcite.rel.logical;

public class SortOptimizationContext {
    private boolean isSortPushed = false;

    public SortOptimizationContext() {}

    void copyFrom(SortOptimizationContext aggOptimizationContext) {
        this.isSortPushed = aggOptimizationContext.isSortPushed;
    }

    public boolean isSortPushed() {
        return isSortPushed;
    }

    public void setSortPushed(boolean sortPushed) {
        isSortPushed = sortPushed;
    }

}
