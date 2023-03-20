package com.alibaba.polardbx.common.oss.filesystem;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * This class is used by listStatus for nfs files.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NFSFileStatus extends FileStatus {
    public NFSFileStatus(long length, boolean isdir, int blockReplication,
                         long blocksize, long modTime, Path path, String user) {
        super(length, isdir, blockReplication, blocksize, modTime, path);
        setOwner(user);
        setGroup(user);
    }
}

