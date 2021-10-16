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

package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.net.AbstractConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executor;

/**
 * write from netty sslHandler
 *
 * @author zhouxy
 */
public class SslHandler {

    private static final Logger logger = LoggerFactory.getLogger(SslHandler.class);
    private final SSLEngine engine;

    private static final int MAX_PLAINTEXT_LENGTH = 16 * 1024;                                // 2^14
    private static final int MAX_COMPRESSED_LENGTH = MAX_PLAINTEXT_LENGTH + 1024;
    private static final int MAX_CIPHERTEXT_LENGTH = MAX_COMPRESSED_LENGTH + 1024;

    static final int MAX_ENCRYPTED_PACKET_LENGTH = MAX_CIPHERTEXT_LENGTH + 5 + 20 + 256;
    private final int maxPacketBufferSize;
    private final boolean wantsDirectBuffer;
    private boolean flushedBeforeHandshakeDone;

    private Promise sslClose = new Promise();

    private Promise handshakePromise = new Promise();

    private AbstractConnection source;
    /**
     * {@code true} if and only if
     * {@link SSLEngine#wrap(ByteBuffer, ByteBuffer)} requires the output buffer
     * to be always as large as {@link #maxPacketBufferSize} even if the input
     * buffer contains small amount of data.
     * <p>
     * If this flag is {@code false}, we allocate a smaller output buffer.
     * </p>
     */
    private int packetLength;
    private boolean wantsInboundHeapBuffer;
    private final Deque<ByteBufferHolder> pendingUnencryptedWrites = new ArrayDeque<ByteBufferHolder>();
    private final Deque<ByteBufferHolder> results = new ArrayDeque<ByteBufferHolder>();

    public SslHandler(SSLEngine engine, AbstractConnection source) {
        if (engine == null) {
            throw new NullPointerException("engine");
        }
        this.engine = engine;
        maxPacketBufferSize = engine.getSession().getPacketBufferSize();

        wantsDirectBuffer = false;
        this.source = source;
    }

    /**
     * Return how much bytes can be read out of the encrypted data. Be aware
     * that this method will not increase the readerIndex of the given
     * {@link ByteBuf}.
     *
     * @param buffer The {@link ByteBuf} to read from. Be aware that it must
     * have at least 5 bytes to read, otherwise it will throw an
     * {@link IllegalArgumentException}.
     * @return length The length of the encrypted packet that is included in the
     * buffer. This will return {@code -1} if the given {@link ByteBuf} is not
     * encrypted at all.
     * @throws IllegalArgumentException Is thrown if the given {@link ByteBuf}
     * has not at least 5 bytes to read.
     */
    private static int getEncryptedPacketLength(ByteBufferHolder buffer, int offset) {
        int packetLength = 0;

        // SSLv3 or TLS - Check ContentType
        boolean tls;
        switch (buffer.getUnsignedByte(offset)) {
        case 20: // change_cipher_spec
        case 21: // alert
        case 22: // handshake
        case 23: // application_data
            tls = true;
            break;
        default:
            // SSLv2 or bad data
            tls = false;
        }

        if (tls) {
            // SSLv3 or TLS - Check ProtocolVersion
            int majorVersion = buffer.getUnsignedByte(offset + 1);
            if (majorVersion == 3) {
                // SSLv3 or TLS
                packetLength = buffer.getUnsignedShort(offset + 3) + 5;
                if (packetLength <= 5) {
                    // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                    tls = false;
                }
            } else {
                // Neither SSLv3 or TLSv1 (i.e. SSLv2 or bad data)
                tls = false;
            }
        }

        if (!tls) {
            // SSLv2 or bad data - Check the version
            boolean sslv2 = true;
            int headerLength = (buffer.getUnsignedByte(offset) & 0x80) != 0 ? 2 : 3;
            int majorVersion = buffer.getUnsignedByte(offset + headerLength + 1);
            if (majorVersion == 2 || majorVersion == 3) {
                // SSLv2
                if (headerLength == 2) {
                    packetLength = (buffer.getShort(offset) & 0x7FFF) + 2;
                } else {
                    packetLength = (buffer.getShort(offset) & 0x3FFF) + 3;
                }
                if (packetLength <= headerLength) {
                    sslv2 = false;
                }
            } else {
                sslv2 = false;
            }

            if (!sslv2) {
                return -1;
            }
        }
        return packetLength;
    }

    public void decode(ByteBufferHolder in) throws SSLException {
        for (; ; ) {
            final int startOffset = in.readerIndex();
            final int endOffset = in.writerIndex();
            int offset = startOffset;
            int totalLength = 0;

            // If we calculated the length of the current SSL record before, use
            // that information.
            if (packetLength > 0) {
                if (endOffset - startOffset < packetLength) {
                    return;
                } else {
                    offset += packetLength;
                    totalLength = packetLength;
                    packetLength = 0;
                }
            }

            final int left = endOffset - offset;
            if (left < 5 && totalLength == 0) {
                break;
            }
            boolean nonSslRecord = false;

            while (totalLength < MAX_ENCRYPTED_PACKET_LENGTH) {
                final int readableBytes = endOffset - offset;
                if (readableBytes < 5) {
                    break;
                }

                final int packetLength = getEncryptedPacketLength(in, offset);
                if (packetLength == -1) {
                    nonSslRecord = true;
                    break;
                }

                if (packetLength > readableBytes) {
                    // wait until the whole packet can be read
                    this.packetLength = packetLength;
                    break;
                }

                int newTotalLength = totalLength + packetLength;
                if (newTotalLength > MAX_ENCRYPTED_PACKET_LENGTH) {
                    // Don't read too much.
                    break;
                }

                // We have a whole packet.
                // Increment the offset to handle the next packet.
                offset += packetLength;
                totalLength = newTotalLength;
            }

            if (totalLength > 0) {
                // The buffer contains one or more full SSL records.
                // Slice out the whole packet so unwrap will only be called with
                // complete packets.
                // Also directly reset the packetLength. This is needed as
                // unwrap(..) may trigger
                // decode(...) again via:
                // 1) unwrap(..) is called
                // 2) wrap(...) is called from within unwrap(...)
                // 3) wrap(...) calls unwrapLater(...)
                // 4) unwrapLater(...) calls decode(...)
                //
                // See https://github.com/netty/netty/issues/1534

                final ByteBuffer inNetBuf = in.nioBuffer(startOffset, totalLength);
                in.skipBytes(totalLength);
                unwrap(inNetBuf, totalLength);

            }

            if (nonSslRecord) {
                // Not an SSL/TLS packet
                SSLException e = new SSLException("not an SSL/TLS record");
                throw e;
            }
        }
    }

    /**
     * Unwraps inbound SSL records.
     */
    private void unwrap(ByteBuffer packet, int initialOutAppBufCapacity) throws SSLException {

        // If SSLEngine expects a heap buffer for unwrapping, do the conversion.
        final ByteBuffer oldPacket;
        final ByteBufferHolder newPacket;
        final int oldPos = packet.position();
        if (wantsInboundHeapBuffer && packet.isDirect()) {
            newPacket = source.allocate();
            newPacket.writeBytes(packet);
            oldPacket = packet;
            packet = newPacket.getBuffer();
        } else {
            oldPacket = null;
            newPacket = null;
        }
        boolean wrapLater = false;
        ByteBufferHolder decodeOut = source.allocate();
        try {
            for (; ; ) {
                final SSLEngineResult result = unwrap(engine, packet, decodeOut);
                final Status status = result.getStatus();
                final HandshakeStatus handshakeStatus = result.getHandshakeStatus();
                final int produced = result.bytesProduced();
                final int consumed = result.bytesConsumed();

                if (status == Status.CLOSED) {
                    // notify about the CLOSED state of the SSLEngine. See #137
                    sslClose.tryFailure();
                    break;
                }

                switch (handshakeStatus) {
                case NEED_UNWRAP:
                    break;
                case NEED_WRAP:
                    wrapNonAppData(true);
                    break;
                case NEED_TASK:
                    runDelegatedTasks();
                    break;
                case FINISHED:
                    setHandshakeSuccess();
                    wrapLater = true;
                    continue;
                case NOT_HANDSHAKING:
                    if (setHandshakeSuccessIfStillHandshaking()) {
                        wrapLater = true;
                        continue;
                    }
                    if (flushedBeforeHandshakeDone) {
                        // We need to call wrap(...) in case there was a
                        // flush
                        // done before the handshake completed.
                        //
                        // See https://github.com/netty/netty/pull/2437
                        flushedBeforeHandshakeDone = false;
                        wrapLater = true;
                    }

                    break;
                default:
                    throw new IllegalStateException("Unknown handshake status: " + handshakeStatus);
                }

                if (status == Status.BUFFER_UNDERFLOW || consumed == 0 && produced == 0) {
                    break;
                }
            }

            if (wrapLater) {
                wrap(true);
            }
        } catch (SSLException e) {
            setHandshakeFailure(e);
            throw e;
        } finally {
            // If we converted packet into a heap buffer at the beginning of
            // this method,
            // we should synchronize the position of the original buffer.
            if (newPacket != null) {
                oldPacket.position(oldPos + packet.position());
                source.recycle(newPacket);
            }

            if (decodeOut.isReadable()) {
                results.add(decodeOut);
            } else {
                source.recycle(decodeOut);
            }
        }
    }

    private static SSLEngineResult unwrap(SSLEngine engine, ByteBuffer in, ByteBufferHolder out) throws SSLException {
        int overflows = 0;
        for (; ; ) {
            ByteBuffer out0 = out.getBuffer();
            SSLEngineResult result = engine.unwrap(in, out0);
            out.writerIndex(out.writerIndex() + result.bytesProduced());
            switch (result.getStatus()) {
            case BUFFER_OVERFLOW:
                int max = engine.getSession().getApplicationBufferSize();
                switch (overflows++) {
                case 0:
                    out.ensureWritable(Math.min(max, in.remaining()));
                    break;
                default:
                    out.ensureWritable(max);
                }
                break;
            default:
                return result;
            }
        }
    }

    private void wrap(boolean inUnwrap) throws SSLException {
        ByteBufferHolder out = null;
        try {
            for (; ; ) {
                ByteBufferHolder pending = pendingUnencryptedWrites.peek();
                if (pending == null) {
                    break;
                }

                ByteBufferHolder buf = pending;
                if (out == null) {
                    out = source.allocate();
                }

                SSLEngineResult result = wrap(engine, buf, out);

                if (!pending.isReadable()) {
                    source.recycle(pending);
                    pendingUnencryptedWrites.remove();
                }

                if (result.getStatus() == Status.CLOSED) {
                    // SSLEngine has been closed already.
                    // Any further write attempts should be denied.
                    for (; ; ) {
                        ByteBufferHolder w = pendingUnencryptedWrites.poll();
                        if (w == null) {
                            break;
                        }
                        source.recycle(w);
                    }
                    return;
                } else {
                    switch (result.getHandshakeStatus()) {
                    case NEED_TASK:
                        runDelegatedTasks();
                        break;
                    case FINISHED:
                        setHandshakeSuccess();
                        // deliberate fall-through
                    case NOT_HANDSHAKING:
                        setHandshakeSuccessIfStillHandshaking();
                        // deliberate fall-through
                    case NEED_WRAP:
                        finishWrap(out, inUnwrap);
                        out = null;
                        break;
                    case NEED_UNWRAP:
                        return;
                    default:
                        throw new IllegalStateException("Unknown handshake status: " + result.getHandshakeStatus());
                    }
                }
            }
        } catch (SSLException e) {
            setHandshakeFailure(e);
            throw e;
        } finally {
            finishWrap(out, inUnwrap);
        }
    }

    private void finishWrap(ByteBufferHolder out, boolean inUnwrap) {
        if (out == null) {
            out = ByteBufferHolder.EMPTY;
        } else if (!out.isReadable()) {
            source.recycle(out);
            out = ByteBufferHolder.EMPTY;
        }
        if (out == ByteBufferHolder.EMPTY) {
            return;
        }
        out.position(out.writerIndex());
        source.doWrite(out);
    }

    private SSLEngineResult wrap(SSLEngine engine, ByteBufferHolder in, ByteBufferHolder out) throws SSLException {
        ByteBuffer in0 = in.nioBuffer(in.readerIndex(), in.writerIndex());

        if (!in0.isDirect()) {
            ByteBuffer newIn0 = ByteBuffer.allocateDirect(in0.remaining());
            newIn0.put(in0).flip();
            in0 = newIn0;
        }
        for (; ; ) {
            ByteBuffer out0 = out.nioBuffer(out.writerIndex(), out.capacity());
            SSLEngineResult result = engine.wrap(in0, out0);
            in.skipBytes(result.bytesConsumed());
            out.writerIndex(out.writerIndex() + result.bytesProduced());
            switch (result.getStatus()) {
            case BUFFER_OVERFLOW:
                out.ensureWritable(maxPacketBufferSize);
                break;
            default:
                return result;
            }
        }
    }

    /**
     * Fetches all delegated tasks from the {@link SSLEngine} and runs them via
     * the {@link #delegatedTaskExecutor}. If the {@link #delegatedTaskExecutor}
     * is {@link ImmediateExecutor}, just call {@link Runnable#run()} directly
     * instead of using {@link Executor#execute(Runnable)}. Otherwise, run the
     * tasks via the {@link #delegatedTaskExecutor} and wait until the tasks are
     * finished.
     */
    private void runDelegatedTasks() {
        for (; ; ) {
            Runnable task = engine.getDelegatedTask();
            if (task == null) {
                break;
            }

            task.run();
        }
    }

    private void wrapNonAppData(boolean inUnwrap) throws SSLException {
        ByteBufferHolder out = null;
        try {
            for (; ; ) {
                if (out == null) {
                    out = source.allocate();
                }
                SSLEngineResult result = wrap(engine, ByteBufferHolder.EMPTY, out);

                if (result.bytesProduced() > 0) {
                    out.position(out.writerIndex());
                    source.doWrite(out);
                    out = null;
                }

                switch (result.getHandshakeStatus()) {
                case FINISHED:
                    setHandshakeSuccess();
                    break;
                case NEED_TASK:
                    runDelegatedTasks();
                    break;
                case NEED_UNWRAP:
                    if (!inUnwrap) {
                        unwrapNonAppData();
                    }
                    break;
                case NEED_WRAP:
                    break;
                case NOT_HANDSHAKING:
                    setHandshakeSuccessIfStillHandshaking();
                    // Workaround for TLS False Start problem reported at:
                    // https://github.com/netty/netty/issues/1108#issuecomment-14266970
                    if (!inUnwrap) {
                        unwrapNonAppData();
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown handshake status: " + result.getHandshakeStatus());
                }

                if (result.bytesProduced() == 0) {
                    break;
                }
            }
        } catch (SSLException e) {
            setHandshakeFailure(e);
            throw e;
        } catch (Exception e) {
            logger.error(e);
        } finally {
            if (out != null) {
                source.recycle(out);
            }
        }
    }

    /**
     * Calls {@link SSLEngine#unwrap(ByteBuffer, ByteBuffer)} with an empty
     * buffer to handle handshakes, etc.
     */
    private void unwrapNonAppData() throws SSLException {
        unwrap(ByteBufferHolder.EMPTY.getBuffer(), 0);
    }

    private boolean setHandshakeSuccessIfStillHandshaking() {
        if (!handshakePromise.isDone()) {
            setHandshakeSuccess();
            return true;
        }
        return false;
    }

    /**
     * Notify all the handshake futures about the successfully handshake
     */
    private void setHandshakeSuccess() {
        // Work around the JVM crash which occurs when a cipher suite with GCM
        // enabled.
        final String cipherSuite = String.valueOf(engine.getSession().getCipherSuite());
        if (!wantsDirectBuffer && (cipherSuite.contains("_GCM_") || cipherSuite.contains("-GCM-"))) {
            wantsInboundHeapBuffer = true;
        }

        handshakePromise.trySuccess();
    }

    /**
     * Notify all the handshake futures about the failure during the handshake.
     */
    private void setHandshakeFailure(Throwable cause) {
        // Release all resources such as internal buffers that SSLEngine
        // is managing.
        engine.closeOutbound();

        try {
            engine.closeInbound();
        } catch (SSLException e) {
            // only log in debug mode as it most likely harmless and latest
            // chrome still trigger
            // this all the time.
            //
            // See https://github.com/netty/netty/issues/1340
            String msg = e.getMessage();
            if (msg == null || !msg.contains("possible truncation attack")) {
                logger.debug("SSLEngine.closeInbound() raised an exception.", e);
            }
        }
        notifyHandshakeFailure(cause);
    }

    private void notifyHandshakeFailure(Throwable cause) {
        handshakePromise.tryFailure();

    }

    public static class Promise {

        private boolean succ;
        private boolean fail;

        boolean isDone() {
            if (succ || fail) {
                return true;
            }
            return false;
        }

        boolean isFail() {
            return fail;
        }

        void trySuccess() {
            succ = true;
        }

        void tryFailure() {
            fail = true;
        }
    }

    public void write(ByteBufferHolder holder) {
        pendingUnencryptedWrites.add(holder);
    }

    public void close() {
        for (; ; ) {
            ByteBufferHolder w = pendingUnencryptedWrites.poll();
            if (w == null) {
                break;
            }
            source.recycle(w);
        }
        engine.closeOutbound();
    }

    public void flush() throws Exception {

        if (pendingUnencryptedWrites.isEmpty()) {
            return;
        }
        if (!handshakePromise.isDone()) {
            flushedBeforeHandshakeDone = true;
        }
        wrap(false);
    }

    public Deque<ByteBufferHolder> getResults() {
        return results;
    }

}
