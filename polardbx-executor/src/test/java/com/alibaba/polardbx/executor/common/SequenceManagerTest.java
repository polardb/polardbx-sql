package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.impl.NewSequence;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SequenceManagerTest {
    public SequenceLoadFromDBManager subManager = Mockito.mock(SequenceLoadFromDBManager.class);

    @Test
    public void testGetSequence() {
        Sequence newSequence = Mockito.mock(NewSequence.class);
        SequenceManager sequenceManager = new SequenceManager(subManager);
        Mockito.when(subManager.isInited()).thenReturn(true);
        Mockito.when(subManager.getSequence("schemaName", "seqName")).thenReturn(newSequence);

        Assert.assertTrue(sequenceManager.getSequence("schemaName", "seqName") instanceof NewSequence);
    }

    @Test(expected = TddlRuntimeException.class)
    public void testGetSequenceException() {
        SequenceManager sequenceManager = new SequenceManager(subManager);
        Mockito.when(subManager.isInited()).thenReturn(true);
        Mockito.when(subManager.getSequence("schemaName", "seqName")).thenReturn(null);
        sequenceManager.getSequence("schemaName", "seqName");
    }

}
