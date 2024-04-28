package org.apache.orc.impl.reader;

import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.BufferChunk;

import java.nio.ByteBuffer;

public class StreamInformation {
    public final OrcProto.Stream.Kind kind;
    public final int column;
    public final long offset;
    public final long length;
    public BufferChunk firstChunk;

    public StreamInformation(OrcProto.Stream.Kind kind,
                             int column, long offset, long length) {
        this.kind = kind;
        this.column = column;
        this.offset = offset;
        this.length = length;
    }

    public void releaseBuffers(DataReader reader) {
        long end = offset + length;
        BufferChunk ptr = firstChunk;
        while (ptr != null && ptr.getOffset() < end) {
            ByteBuffer buffer = ptr.getData();
            if (buffer != null) {
                reader.releaseBuffer(buffer);
                ptr.setChunk(null);
            }
            ptr = (BufferChunk) ptr.next;
        }
        firstChunk = null;
    }

    public long releaseBuffers() {
        long releasedBytes = 0L;
        long end = offset + length;
        BufferChunk ptr = firstChunk;
        while (ptr != null && ptr.getOffset() < end) {
            ByteBuffer buffer = ptr.getData();
            if (buffer != null) {
                // record the capacity of buffer because the total array will be GC.
                releasedBytes += buffer.capacity();
                ptr.setChunk(null);
            }
            ptr = (BufferChunk) ptr.next;
        }
        firstChunk = null;
        return releasedBytes;
    }
}