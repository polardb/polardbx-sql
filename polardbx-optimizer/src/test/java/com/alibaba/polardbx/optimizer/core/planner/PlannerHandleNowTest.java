package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsParameterizeSqlVisitor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PlannerHandleNowTest {
    @Test
    public void test() {
        ExecutionContext context = new ExecutionContext();
        MysqlDateTime nowValue = DataTypeUtil.getNow(6, context);
        context.getConstantValue("now", () -> nowValue);

        DrdsParameterizeSqlVisitor.ConstantVariable constantVariable =
            Mockito.mock(DrdsParameterizeSqlVisitor.ConstantVariable.class);
        Mockito.when(constantVariable.getName()).thenReturn("now");
        Mockito.when(constantVariable.getArgs()).thenReturn(new Integer[] {5});

        MysqlDateTime mysqlDateTime = Planner.handleNow(context, constantVariable);

        Assert.assertFalse(nowValue == mysqlDateTime);

        MysqlDateTime truncated = MySQLTimeCalculator.timeTruncate(nowValue, 5);
        Assert.assertTrue(truncated.toPackedLong() == mysqlDateTime.toPackedLong());
    }
}