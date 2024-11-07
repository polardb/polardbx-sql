package com.alibaba.polardbx.optimizer.core.function.calc.scalar.encryption;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class AesTest {

    @Test
    public void testAesEncryptAndDecrypt() {
        AesEncrypt encrypt = new AesEncrypt();
        ExecutionContext context = new ExecutionContext();
        context.setExtraServerVariables(new HashMap<>());
        String text = "hello";
        String key = "goodbye";
        Object[] args1 = new Object[] {
            text, key
        };
        Object encResult = encrypt.compute(args1, context);

        AesDecrypt decrypt = new AesDecrypt();
        Object[] args2 = new Object[] {
            encResult, key
        };
        Object decResult = decrypt.compute(args2, context);
        Assert.assertEquals(text, decResult);
    }

    @Test
    public void testConcurrent() {
        final int threadCount = 5;
        final int testCount = 10;
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                AesEncrypt encrypt = new AesEncrypt();
                AesDecrypt decrypt = new AesDecrypt();

                ExecutionContext context = new ExecutionContext();
                context.setExtraServerVariables(new HashMap<>());
                String text = "hello";
                String key = "goodbye";
                for (int j = 0; j < testCount; j++) {
                    Object[] args1 = new Object[] {
                        text, key
                    };
                    Object encResult = encrypt.compute(args1, context);

                    Object[] args2 = new Object[] {
                        encResult, key
                    };
                    Object decResult = decrypt.compute(args2, context);
                    if (!Objects.equals(text, decResult)) {
                        exceptionRef.set(new RuntimeException("Decryption failed"));
                        break;
                    }
                }
            }).start();
        }
        Assert.assertNull(exceptionRef.get());
    }

    @Test
    public void testIncorrectParamCount() {
        AesEncrypt encrypt = new AesEncrypt();
        ExecutionContext context = new ExecutionContext();
        context.setExtraServerVariables(new HashMap<>());
        context.getExtraServerVariables().put(ConnectionProperties.BLOCK_ENCRYPTION_MODE, "aes-128-cfb8");
        String text = "hello";
        String key = "goodbye";
        Object[] args1 = new Object[] {
            text, key
        };
        try {
            Object encResult = encrypt.compute(args1, context);
            Assert.fail("Expect failed");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Incorrect parameter count in the call"));
        }
    }
}
