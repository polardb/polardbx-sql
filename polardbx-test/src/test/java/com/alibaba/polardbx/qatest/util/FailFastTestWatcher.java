package com.alibaba.polardbx.qatest.util;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class FailFastTestWatcher extends TestWatcher {

    private final Class<? extends Throwable> exceptionClass;

    public FailFastTestWatcher(Class<? extends Throwable> exceptionClass) {
        this.exceptionClass = exceptionClass;
    }

    @Override
    protected void failed(Throwable e, Description description) {
        if (exceptionClass.isInstance(e)) {
            System.out.println("Test failed: " + description.getDisplayName());
            System.out.println("Throwable: " + e.getMessage());
            System.out.println("StackTrace: ");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
