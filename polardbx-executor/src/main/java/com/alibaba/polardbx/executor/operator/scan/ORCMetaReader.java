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
