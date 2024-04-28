package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.impl.ORCMetaReaderImpl;
import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

/**
 * A stand-alone orc reading interface to apply the customized IO optimization.
 */
public interface ORCMetaReader extends Closeable {

    static ORCMetaReader create(Configuration configuration, FileSystem fileSystem) {
        return new ORCMetaReaderImpl(configuration, fileSystem);
    }

    /**
     * Execute the preheating and get preheating results.
     *
     * @param path file path to preheat.
     * @return preheating results including stripe-level and file-level meta.
     */
    PreheatFileMeta preheat(Path path) throws IOException;
}
