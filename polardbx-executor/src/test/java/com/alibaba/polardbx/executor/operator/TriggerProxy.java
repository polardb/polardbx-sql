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

package com.alibaba.polardbx.executor.operator;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.executor.operator.spill.MemoryRevoker;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.Callable;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.System.exit;

/**
 * use dynamic proxy binding callback ,when object calls some methods every x times trigger the callback
 *
 */
public class TriggerProxy {
    Object target;
    TriggerInvocationHandler invokeHandler;

    private TriggerProxy(Builder builder) {
        invokeHandler = new TriggerInvocationHandler(builder.target, builder.triggers.build());
        this.target = builder.target;
    }

    public Object getProxy() {
        return Proxy.newProxyInstance(
            target.getClass().getClassLoader(),
            target.getClass().getInterfaces(),
            invokeHandler);
    }

    public Object getProxyWithInterface(Class<?> proxyInterface) {
        assert allTriggersExistsInInterfaces(new Class<?>[] {proxyInterface});
        return Proxy.newProxyInstance(
            target.getClass().getClassLoader(),
            new Class[] {proxyInterface},
            invokeHandler);
    }

    public Object getProxyWithInterfaces(Class<?>[] proxyInterfaces) {
        assert allTriggersExistsInInterfaces(proxyInterfaces);
        return Proxy.newProxyInstance(
            target.getClass().getClassLoader(),
            proxyInterfaces,
            invokeHandler);
    }

    private boolean allTriggersExistsInInterfaces(Class<?>[] proxyInterfaces) {
        for (Trigger trigger : invokeHandler.triggers) {
            boolean found = false;
            for (Class<?> proxyInterface : proxyInterfaces) {
                if (TriggerInInterface(trigger, proxyInterface)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    static private boolean TriggerInInterface(Trigger trigger, Class<?> proxyInterface) {
        try {
            proxyInterface.getDeclaredMethod(trigger.method.getName(), trigger.method.getParameterTypes());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    static class Trigger {
        final Method method;
        final int methodCallNumEveryCallback;
        final Runnable callback;

        int methodCallCnt;

        public Trigger(Method bindMethod, Runnable callback, int methodCallNumEveryCallback) {
            assert callback != null;
            assert methodCallNumEveryCallback != 0;
            this.method = bindMethod;
            this.methodCallNumEveryCallback = methodCallNumEveryCallback;
            this.callback = callback;
            methodCallCnt = 0;
        }

        public void call() throws Throwable {
            if (methodCallCnt % methodCallNumEveryCallback == 0) {
                callback.run();
            }
            methodCallCnt++;
        }
    }

    private static class TriggerInvocationHandler implements InvocationHandler {
        Object target;
        List<Trigger> triggers;

        public TriggerInvocationHandler(Object target, List<Trigger> triggers) {
            this.target = target;
            this.triggers = triggers;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            Object result = null;
            try {
                result = method.invoke(target, args);
                for (Trigger trigger : triggers) {
                    if (trigger.method.equals(method)) {
                        trigger.call();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                exit(0);
            } finally {
                return result;
            }
        }
    }

    static class Builder {
        protected Object target;
        protected ImmutableList.Builder<Trigger> triggers = ImmutableList.builder();

        public Builder(Object target) {
            this.target = target;
        }

        public Builder addTrigger(Method method, Runnable callback, int methodCallNumEveryCallback) {
            triggers.add(new Trigger(method, callback, methodCallNumEveryCallback));
            return this;
        }

        public Builder addTrigger(Method method, Runnable callback) {
            return this.addTrigger(method, callback, 1);
        }

        public TriggerProxy build() {
            return new TriggerProxy(this);
        }
    }
}
