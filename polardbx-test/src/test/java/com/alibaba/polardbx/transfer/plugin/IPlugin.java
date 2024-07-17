package com.alibaba.polardbx.transfer.plugin;

/**
 * @author wuzhe
 */
public interface IPlugin {
    long getOpAndClear();

    void run();

    boolean isEnabled();

    void interrupt();
}
