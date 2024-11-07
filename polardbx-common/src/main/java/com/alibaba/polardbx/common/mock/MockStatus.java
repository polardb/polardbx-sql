package com.alibaba.polardbx.common.mock;

public class MockStatus {
    private static volatile boolean mock = false;

    public static boolean isMock() {
        return mock;
    }

    public static void setMock(boolean mock) {
        MockStatus.mock = mock;
    }
}
