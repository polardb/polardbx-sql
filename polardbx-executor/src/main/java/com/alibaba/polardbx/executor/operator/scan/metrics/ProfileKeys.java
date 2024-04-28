package com.alibaba.polardbx.executor.operator.scan.metrics;

/**
 * Collections of pre-defined metrics keys.
 */
public class ProfileKeys {
    public static final ProfileKey PUSH_CHUNK_COUNTER = ProfileKey.builder()
        .setName("Operator.PullChunkCounter")
        .setDescription("The count of pulled chunks in this operator.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.NONE)
        .build();

    public static final ProfileKey PULL_CHUNK_ROWS_COUNTER = ProfileKey.builder()
        .setName("Operator.PullChunkRows")
        .setDescription("The count of rows in pulled chunks in this operator.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.NONE)
        .build();

    public static final ProfileKey ORC_STRIPE_LOADER_OPEN_TIMER = ProfileKey.builder()
        .setName("ORC.StripeLoaderOpenTimer")
        .setDescription("The time cost of stripe-loader opening.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey ORC_IN_STREAM_MEMORY_COUNTER = ProfileKey.builder()
        .setName("ORC.InStreamMemoryCounter")
        .setDescription("The count of memory allocation in bytes during processing of in-stream.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.BYTES)
        .build();

    public static final ProfileKey ORC_IN_STREAM_DECOMPRESS_TIMER = ProfileKey.builder()
        .setName("ORC.InStreamDecompressTimer")
        .setDescription("The time cost of decompression during processing of in-stream.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey ORC_IO_RAW_DATA_MEMORY_COUNTER = ProfileKey.builder()
        .setName("ORC.IORawDataMemoryCounter")
        .setDescription("The count of memory allocation in bytes during processing of data reading.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.BYTES)
        .build();

    public static final ProfileKey ORC_IO_RAW_DATA_TIMER = ProfileKey.builder()
        .setName("ORC.IORawDataTimer")
        .setDescription("The time cost of processing of data reading.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey ORC_LOGICAL_BYTES_RANGE = ProfileKey.builder()
        .setName("ORC.LogicalBytesRange")
        .setDescription("The count of bytes range of data processing plan.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.BYTES)
        .build();

    public static final ProfileKey ORC_STREAM_READER_MEMORY_COUNTER = ProfileKey.builder()
        .setName("ORC.StreamReaderMemoryCounter")
        .setDescription("The count of bytes allocated in stream reader.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.BYTES)
        .build();

    public static final ProfileKey ORC_COLUMN_IO_PREPARING_TIMER = ProfileKey.builder()
        .setName("ORC.ColumnIOPreparingTimer")
        .setDescription("The time cost of data IO data preparing for stripe-loader.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey ORC_COLUMN_SEEK_TIMER = ProfileKey.builder()
        .setName("ORC.ColumnSeekTimer")
        .setDescription("The time cost of position seeking in column reader.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey ORC_COLUMN_PARSE_TIMER = ProfileKey.builder()
        .setName("ORC.ColumnParseTimer")
        .setDescription("The time cost of column data parsing in column reader.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey SCAN_WORK_BLOCK_MEMORY_COUNTER = ProfileKey.builder()
        .setName("ScanWork.BlockMemoryCounter")
        .setDescription("The count of bytes allocated in block loader.")
        .setProfileType(ProfileType.COUNTER)
        .setProfileUnit(ProfileUnit.BYTES)
        .build();

    public static final ProfileKey SCAN_WORK_BLOCK_LOAD_TIMER = ProfileKey.builder()
        .setName("ScanWork.BlockLoadTimer")
        .setDescription("The time cost of loading in block loader.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();

    public static final ProfileKey SCAN_WORK_EVALUATION_TIMER = ProfileKey.builder()
        .setName("ScanWork.EvaluationTimer")
        .setDescription("The time cost of evaluation in scan-work.")
        .setProfileType(ProfileType.TIMER)
        .setProfileUnit(ProfileUnit.NANO_SECOND)
        .build();
}
