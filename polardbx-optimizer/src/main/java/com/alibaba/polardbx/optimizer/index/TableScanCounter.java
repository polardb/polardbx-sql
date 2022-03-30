package com.alibaba.polardbx.optimizer.index;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

/**
 * @author shengyu
 */
public class TableScanCounter extends RelShuttleImpl {

    private int counter;

    public TableScanCounter() {
        counter = 0;
    }

    @Override
    public RelNode visit(TableScan scan) {
        counter++;
        return scan;
    }

    public int getCount() {
        return counter;
    }
}
