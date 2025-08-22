package com.alibaba.polardbx.executor.mpp.execution.scheduler;

public enum RandomNodeMode {

    RANDOM,     // default mode
    GROUP,
    NONE;

    public static RandomNodeMode getRandomNodeMode(String mode) {
        if (mode == null) {
            return RANDOM;
        }
        mode = mode.toUpperCase();
        switch (mode) {
        case "GROUP":
            return GROUP;
        case "NONE":
            return NONE;
        case "RANDOM":
        default:
            return RANDOM;
        }
    }

}
