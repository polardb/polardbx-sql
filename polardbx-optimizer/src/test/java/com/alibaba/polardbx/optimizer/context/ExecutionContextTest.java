package com.alibaba.polardbx.optimizer.context;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author fangwu
 */
public class ExecutionContextTest {
    @Test
    public void subqueryRelatedIdTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.getScalarSubqueryCtxMap().put(23342227, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(0, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(-1, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(-23342227, new ScalarSubQueryExecContext().setSubQueryResult(1));
        Assert.assertTrue(ec.getScalarSubqueryVal(23342227) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(0) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(-1) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(-23342227) != null);
    }
}
