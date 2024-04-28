package com.alibaba.polardbx.executor.operator.scan.impl;

import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.reader.ReaderEncryption;

public class StripeContext {
    private final StripeInformation stripeInformation;

    // global information
    private final TypeDescription schema;
    private final OrcFile.WriterVersion version;
    private final OrcProto.ColumnEncoding[] encodings;
    private final ReaderEncryption encryption;

    private final InStream.StreamOptions streamOptions;

    private final boolean ignoreNonUtf8BloomFilter;
    private final long maxBufferSize;
    private final OrcProto.Stream.Kind[] bloomFilterKinds;

    // does each column have a null stream?
    private final boolean[] hasNull;

    // specific to the current stripe
    private String writerTimezone;
    private long currentStripeId;
    private long originalStripeId;

    private boolean[] columnInclude;

    public StripeContext(StripeInformation stripeInformation,
                         TypeDescription schema,
                         ReaderEncryption encryption,
                         OrcFile.WriterVersion version,
                         InStream.StreamOptions streamOptions,
                         boolean ignoreNonUtf8BloomFilter,
                         long maxBufferSize) {
        this.stripeInformation = stripeInformation;
        this.schema = schema;
        this.version = version;
        this.encodings = new OrcProto.ColumnEncoding[schema.getMaximumId() + 1];
        this.encryption = encryption;
        this.streamOptions = streamOptions;
        this.ignoreNonUtf8BloomFilter = ignoreNonUtf8BloomFilter;
        this.bloomFilterKinds = new OrcProto.Stream.Kind[schema.getMaximumId() + 1];
        this.hasNull = new boolean[schema.getMaximumId() + 1];
        this.maxBufferSize = maxBufferSize;
    }

    public StripeContext setWriterTimezone(String writerTimezone) {
        this.writerTimezone = writerTimezone;
        return this;
    }

    public StripeContext setCurrentStripeId(long currentStripeId) {
        this.currentStripeId = currentStripeId;
        return this;
    }

    public StripeContext setOriginalStripeId(long originalStripeId) {
        this.originalStripeId = originalStripeId;
        return this;
    }

    public StripeContext setColumnInclude(boolean[] columnInclude) {
        this.columnInclude = columnInclude;
        return this;
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public OrcFile.WriterVersion getVersion() {
        return version;
    }

    public OrcProto.ColumnEncoding[] getEncodings() {
        return encodings;
    }

    public ReaderEncryption getEncryption() {
        return encryption;
    }

    public boolean isIgnoreNonUtf8BloomFilter() {
        return ignoreNonUtf8BloomFilter;
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public OrcProto.Stream.Kind[] getBloomFilterKinds() {
        return bloomFilterKinds;
    }

    public boolean[] getHasNull() {
        return hasNull;
    }

    public String getWriterTimezone() {
        return writerTimezone;
    }

    public long getCurrentStripeId() {
        return currentStripeId;
    }

    public long getOriginalStripeId() {
        return originalStripeId;
    }

    public boolean[] getColumnInclude() {
        return columnInclude;
    }

    public StripeInformation getStripeInformation() {
        return stripeInformation;
    }

    public InStream.StreamOptions getStreamOptions() {
        return streamOptions;
    }
}
