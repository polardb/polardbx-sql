package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;

import java.util.function.Consumer;

public interface IInnerConnection {
    ITransactionPolicy getTrxPolicy();

    void setTrxPolicy(ITransactionPolicy trxPolicy);

    void addExecutionContextInjectHook(Consumer<Object> hook);

    void clearExecutionContextInjectHooks();

    void setTimeZone(String timeZone);
}
