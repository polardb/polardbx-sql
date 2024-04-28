package com.alibaba.polardbx.executor.operator.scan;

/**
 * States of File Scan.
 * The transformation between states:
 * +----------+          +----------+      +---------+
 * |   READY  |<-------> | BLOCKED  |----->| FAILED  |
 * +----------+          +----------+      +---------+
 * |                                         |
 * |                                         |
 * |                                         |
 * |                                         |
 * |                +------------+           |--> +---------+
 * | -------------> | FINISHED   |--------------->| CLOSED  |
 * .                +------------+                +---------+
 */
public enum ScanState {
    /**
     * IO production is ready for fetching.
     */
    READY,
    /**
     * Consumption is blocked because IO progress is running.
     */
    BLOCKED,
    /**
     * IO progress is done, and waiting for resource closed.
     */
    FINISHED,
    /**
     * IO progress is broken because of any exceptions.
     */
    FAILED,
    /**
     * File resources are all closed.
     */
    CLOSED
}
