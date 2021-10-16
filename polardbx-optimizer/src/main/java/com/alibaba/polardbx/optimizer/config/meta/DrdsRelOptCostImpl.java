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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.runtime.Utilities;

/**
 * DrdsRelOptCostImpl provides a default implementation for the {@link RelOptCost}
 * interface. It it defined in terms of a single scalar quantity; somewhat
 * arbitrarily, it returns this scalar for rows processed and zero for both CPU
 * and I/O.
 */
public class DrdsRelOptCostImpl implements RelOptCost {

    public static final Factory FACTORY = new Factory();

    public static final DrdsRelOptCostImpl INFINITY = FACTORY.makeCost(
        Double.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY,
        Double.POSITIVE_INFINITY);

    public static final DrdsRelOptCostImpl HUGE = FACTORY.makeCost(
        Double.MAX_VALUE / 10000,
        Double.MAX_VALUE / 10000,
        Double.MAX_VALUE / 10000,
        Double.MAX_VALUE / 10000,
        Double.MAX_VALUE / 10000);

    public static final DrdsRelOptCostImpl ZERO =
        FACTORY.makeCost(0.0, 0.0, 0.0, 0.0, 0.0);

    public static final DrdsRelOptCostImpl TINY =
        FACTORY.makeCost(1.0, 1.0, 0.0, 0.0, 0.0);

    //~ Instance fields --------------------------------------------------------

    protected final double rows;
    protected final double cpu;
    protected final double memory;
    protected final double io;
    protected final double net;

    //~ Constructors -----------------------------------------------------------

    public DrdsRelOptCostImpl(double rows, double cpu, double memory, double io, double net) {
        this.rows = rows;
        this.cpu = cpu;
        this.memory = memory;
        this.io = io;
        this.net = net;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptCost
    public double getRows() {
        return rows;
    }

    // implement RelOptCost
    public double getMemory() {
        return memory;
    }

    // implement RelOptCost
    public double getIo() {
        return io;
    }

    // implement RelOptCost
    public double getCpu() {
        return cpu;
    }

    // implement RelOptCost
    public double getNet() {
        return net;
    }

    // implement RelOptCost
    public boolean isInfinite() {
        return Double.isInfinite(rows) || Double.isInfinite(cpu) || Double.isInfinite(memory) || Double.isInfinite(io)
            || Double.isInfinite(net);
    }

    public boolean isHuge() {
        return this == HUGE || this.getValue() >= Double.MAX_VALUE / 10000;
    }

    // implement RelOptCost
    public boolean isLe(RelOptCost other) {
        final DrdsRelOptCostImpl that = (DrdsRelOptCostImpl) other;
        return this.getValue() <= that.getValue();
    }

    // implement RelOptCost
    public boolean isLt(RelOptCost other) {
        final DrdsRelOptCostImpl that = (DrdsRelOptCostImpl) other;
        return this.getValue() < that.getValue();
    }

    public double getValue() {
        double costValue = this.getCpu()
            + CostModelWeight.INSTANCE.getIoWeight() * this.getIo()
            + CostModelWeight.INSTANCE.getNetWeight() * this.getNet()
            + CostModelWeight.INSTANCE.getMemoryWeight() * this.getMemory();
        return costValue;
    }

    @Override
    public int hashCode() {
        return Utilities.hashCode(getRows() + getCpu() + getMemory() + getIo() + getNet());
    }

    // implement RelOptCost
    public boolean equals(RelOptCost other) {
        return getRows() == other.getRows()
            && getCpu() == other.getCpu()
            && getMemory() == other.getMemory()
            && getRows() == other.getRows()
            && getNet() == other.getNet();
    }

    // implement RelOptCost
    public boolean isEqWithEpsilon(RelOptCost other) {
        return Math.abs(getRows() - other.getRows()) < RelOptUtil.EPSILON
            && Math.abs(getCpu() - other.getCpu()) < RelOptUtil.EPSILON
            && Math.abs(getMemory() - other.getMemory()) < RelOptUtil.EPSILON
            && Math.abs(getIo() - other.getIo()) < RelOptUtil.EPSILON
            && Math.abs(getNet() - other.getNet()) < RelOptUtil.EPSILON;
    }

    // implement RelOptCost
    public RelOptCost minus(RelOptCost other) {

        return new DrdsRelOptCostImpl(
            getRows() - other.getRows(),
            getCpu() - other.getCpu(),
            getMemory() - other.getMemory(),
            getIo() - other.getIo(),
            getNet() - other.getNet());
    }

    // implement RelOptCost
    public RelOptCost plus(RelOptCost other) {
        return new DrdsRelOptCostImpl(
            getRows() + other.getRows(),
            getCpu() + other.getCpu(),
            getMemory() + other.getMemory(),
            getIo() + other.getIo(),
            getNet() + other.getNet());
    }

    // implement RelOptCost
    public RelOptCost multiplyBy(double factor) {
        return new DrdsRelOptCostImpl(
            getRows() * factor,
            getCpu() * factor,
            getMemory() * factor,
            getIo() * factor,
            getNet() * factor);
    }

    public double divideBy(RelOptCost cost) {
        if (((DrdsRelOptCostImpl) cost).getValue() == 0) {
            return Double.MAX_VALUE;
        }
        return this.getValue() / ((DrdsRelOptCostImpl) cost).getValue();
    }

    // implement RelOptCost
    public String toString() {
        if (isHuge()) {
            return "huge";
        } else {
            double value = Math.ceil(this.getValue());
            return "value = " + value + ", cpu = " + Math.ceil(getCpu()) + ", memory = " + Math.ceil(getMemory()) +
                ", io = " + getIo()
                + ", net = " + getNet();
        }
    }

    /**
     * Implementation of {@link RelOptCostFactory} that creates
     * {@link DrdsRelOptCostImpl}s.
     */
    public static class Factory implements RelOptCostFactory {

        public Factory() {
        }

        // implement RelOptPlanner
        public DrdsRelOptCostImpl makeCost(double rows, double cpu, double mem, double io, double net) {
            return new DrdsRelOptCostImpl(rows, cpu, mem, io, net);
        }

        // implement RelOptPlanner
        public DrdsRelOptCostImpl makeHugeCost() {
            return DrdsRelOptCostImpl.HUGE;
        }

        // implement RelOptPlanner
        public DrdsRelOptCostImpl makeInfiniteCost() {
            return DrdsRelOptCostImpl.INFINITY;
        }

        // implement RelOptPlanner
        public DrdsRelOptCostImpl makeTinyCost() {
            return DrdsRelOptCostImpl.TINY;
        }

        // implement RelOptPlanner
        public DrdsRelOptCostImpl makeZeroCost() {
            return DrdsRelOptCostImpl.ZERO;
        }
    }

}
