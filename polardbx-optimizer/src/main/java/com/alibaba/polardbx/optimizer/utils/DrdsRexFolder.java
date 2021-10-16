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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprVisitor;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/**
 * DrdsRexFolder folds constant in RexNode including RexDynamicParams
 *
 * @author Eric Fu
 */
public abstract class DrdsRexFolder {

    private static final Logger logger = LoggerFactory.getLogger(DrdsRexFolder.class);

    public static Object fold(RexNode rex, PlannerContext plannerContext) {
        // FIXME: too hacky!
        ExecutionContext context = new ExecutionContext();
        context.setParams(plannerContext.getParams());
        return fold(rex, context);
    }

    public static Object fold(RexNode rex, ExecutionContext context) {
        if (rex == null) {
            return null;
        }
        FoldingRex2ExprVisitor visitor = new FoldingRex2ExprVisitor(context);
        IExpression expression = rex.accept(visitor);
        if (visitor.isFoldable()) {
            try {
                return expression.eval(null);
            } catch (Exception ex) {
                // Just in case
                logger.warn("Failed to evaluate expression");
                return null;
            }
        }
        return null;
    }

    static class FoldingRex2ExprVisitor extends Rex2ExprVisitor {

        private boolean foldable = true;

        public FoldingRex2ExprVisitor(ExecutionContext executionContext) {
            super(executionContext);
        }

        @Override
        public IExpression visitInputRef(RexInputRef inputRef) {
            foldable = false;
            return super.visitInputRef(inputRef);
        }

        @Override
        public IExpression visitFieldAccess(RexFieldAccess fieldAccess) {
            foldable = false;
            // Note that this method may modify execution context
            return super.visitFieldAccess(fieldAccess);
        }

        public boolean isFoldable() {
            return foldable;
        }
    }
}
