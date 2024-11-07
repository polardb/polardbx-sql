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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentInfo;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionConstructor;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VectorizedExpressionRegistryTest {
    @Test
    public void testGetBuilderConstructor() {
        ArgumentInfo arg1 = new ArgumentInfo(DataTypes.LongType, ArgumentKind.Const);
        ArgumentInfo arg2 = new ArgumentInfo(DataTypes.IntegerType, ArgumentKind.Variable);
        ExpressionSignature sig = new ExpressionSignature("expr_test", new ArgumentInfo[] {arg1, arg2});

        Optional<ExpressionConstructor<?>> constructor =
            VectorizedExpressionRegistry.builderConstructorOf(sig);
        assertTrue("Construct of " + sig + " should exist.", constructor.isPresent());
        assertEquals("Class of " + sig + " should be " + TestVectorizedExpression.class,
            TestVectorizedExpression.class, constructor.get().getDeclaringClass());
        assertTrue(constructor.get().build(0, new VectorizedExpression[0]) instanceof TestVectorizedExpression);
    }

    @Test
    public void testNotExistSignature() {
        ArgumentInfo arg1 = new ArgumentInfo(DataTypes.LongType, ArgumentKind.Const);
        ArgumentInfo arg2 = new ArgumentInfo(DataTypes.IntegerType, ArgumentKind.Variable);
        ExpressionSignature sig = new ExpressionSignature("expr_test_not_exist", new ArgumentInfo[] {arg1, arg2});

        Optional<ExpressionConstructor<?>> constructor =
            VectorizedExpressionRegistry.builderConstructorOf(sig);
        assertFalse("Construct of " + sig + " should not exist.", constructor.isPresent());
        try {
            constructor.get().build(0, new VectorizedExpression[0]);
            Assert.fail("Expect failed when signature does not exist");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No value"));
        }
    }

    @Test
    public void testNotExistClass() {
        try {
            ExpressionConstructor.of(FailTestVectorizedExpression.class);
            Assert.fail("Expect failed when class construct does not exist");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Failed to get builder constructor"));
            Assert.assertTrue(e.getMessage().contains(FailTestVectorizedExpression.class.getName()));
        }
    }

}

class FailTestVectorizedExpression extends AbstractVectorizedExpression {
    private FailTestVectorizedExpression() {
        super(DataTypes.BlobType, 0, null);
    }

    @Override
    public void eval(EvaluationContext ctx) {
    }
}