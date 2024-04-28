package org.apache.orc.customized;

import java.nio.ByteBuffer;

/**
 * It's responsible for memory copy between invisible objects in polardbx-orc.
 */
public interface ORCDataOutput {
    /**
     * Read data from byte buffer and write into inner object.
     *
     * @param buffer source byte buffer.
     * @param bytesToRead how many bytes need to read.
     */
    void read(ByteBuffer buffer, int bytesToRead);
}
