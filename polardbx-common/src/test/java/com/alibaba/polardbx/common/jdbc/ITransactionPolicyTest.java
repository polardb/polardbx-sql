package com.alibaba.polardbx.common.jdbc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ITransactionPolicyTest {
    @Test
    public void testXA() {
        ITransactionPolicy policy = ITransactionPolicy.of("XA");
        assertEquals(ITransactionPolicy.XA, policy);

        ITransactionPolicy.TransactionClass trxClass =
            policy.getTransactionType(false, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.XA, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, true);
        assertEquals(ITransactionPolicy.TransactionClass.XA, trxClass);
    }

    @Test
    public void testTSO() {
        ITransactionPolicy policy = ITransactionPolicy.of("TSO");
        assertEquals(ITransactionPolicy.TSO, policy);

        ITransactionPolicy.TransactionClass trxClass =
            policy.getTransactionType(false, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.TSO, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, true);
        assertEquals(ITransactionPolicy.TransactionClass.TSO, trxClass);

        trxClass =
            policy.getTransactionType(true, true, true, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT_SINGLE_SHARD, trxClass);

        trxClass =
            policy.getTransactionType(true, true, true, true);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT_SINGLE_SHARD, trxClass);
    }

    @Test
    public void testAllowRead() {
        ITransactionPolicy policy = ITransactionPolicy.of("ALLOW_READ");
        assertEquals(ITransactionPolicy.ALLOW_READ_CROSS_DB, policy);

        ITransactionPolicy.TransactionClass trxClass =
            policy.getTransactionType(false, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.ALLOW_READ_CROSS_DB, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, true);
        assertEquals(ITransactionPolicy.TransactionClass.ALLOW_READ_CROSS_DB, trxClass);
    }

    @Test
    public void testFree() {
        ITransactionPolicy policy = ITransactionPolicy.of("FREE");
        assertEquals(ITransactionPolicy.FREE, policy);

        ITransactionPolicy.TransactionClass trxClass =
            policy.getTransactionType(false, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.COBAR_STYLE, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, true);
        assertEquals(ITransactionPolicy.TransactionClass.COBAR_STYLE, trxClass);
    }

    @Test
    public void testNoTransaction() {
        ITransactionPolicy policy = ITransactionPolicy.of("NO_TRANSACTION");
        assertEquals(ITransactionPolicy.NO_TRANSACTION, policy);

        ITransactionPolicy.TransactionClass trxClass =
            policy.getTransactionType(false, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.AUTO_COMMIT, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, true);
        assertEquals(ITransactionPolicy.TransactionClass.TSO, trxClass);
    }

    @Test
    public void testArchive() {
        ITransactionPolicy policy = ITransactionPolicy.of("ARCHIVE");
        assertEquals(ITransactionPolicy.ARCHIVE, policy);
        ITransactionPolicy.TransactionClass trxClass =
            policy.getTransactionType(false, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.ARCHIVE, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, false);
        assertEquals(ITransactionPolicy.TransactionClass.ARCHIVE, trxClass);

        trxClass =
            policy.getTransactionType(true, false, false, true);
        assertEquals(ITransactionPolicy.TransactionClass.ARCHIVE, trxClass);
    }
}
