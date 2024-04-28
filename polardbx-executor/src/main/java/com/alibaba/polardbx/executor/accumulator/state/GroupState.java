package com.alibaba.polardbx.executor.accumulator.state;

/**
 * State of groups
 *
 * @author Eric Fu
 */
public interface GroupState {

    long estimateSize();

}
