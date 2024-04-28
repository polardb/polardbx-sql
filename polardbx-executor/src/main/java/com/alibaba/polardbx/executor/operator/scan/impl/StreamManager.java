package com.alibaba.polardbx.executor.operator.scan.impl;

import org.apache.orc.impl.InStream;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.reader.StreamInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamManager {
    private StripeContext stripeContext;

    private final Map<StreamName, StreamInformation> streams;
    // the index streams sorted by offset
    private final List<StreamInformation> indexStreams;
    // the data streams sorted by offset
    private final List<StreamInformation> dataStreams;

    public StreamManager() {
        streams = new HashMap<>();
        indexStreams = new ArrayList<>();
        dataStreams = new ArrayList<>();
    }

    /**
     * Get the stream for the given name.
     * It is assumed that the name does <b>not</b> have the encryption set,
     * because the TreeReader's don't know if they are reading encrypted data.
     * Assumes that readData has already been called on this stripe.
     *
     * @param name the column/kind of the stream
     * @return a new stream with the options set correctly
     */
    public InStream getStream(StreamName name) throws IOException {
        StreamInformation stream = streams.get(name);

        InStream.StreamOptions streamOptions = StaticStripePlanner.getStreamOptions(
            stripeContext, stream.column, stream.kind
        );

        return stream == null ? null
            : InStream.create(
            name,
            stream.firstChunk,
            stream.offset,
            stream.length,
            streamOptions);
    }

    public StripeContext getStripeContext() {
        return stripeContext;
    }

    public StreamManager setStripeContext(StripeContext stripeContext) {
        this.stripeContext = stripeContext;
        return this;
    }

    public Map<StreamName, StreamInformation> getStreams() {
        return streams;
    }

    public List<StreamInformation> getIndexStreams() {
        return indexStreams;
    }

    public List<StreamInformation> getDataStreams() {
        return dataStreams;
    }

    @Override
    public String toString() {
        return "StreamManager{" +
            "streams=" + streams +
            ", indexStreams=" + indexStreams +
            ", dataStreams=" + dataStreams +
            '}';
    }
}
