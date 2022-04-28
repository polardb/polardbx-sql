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

package com.alibaba.polardbx.gms.engine;


import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class FileSystemGroup {

    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private FileSystem master;
    private List<FileSystem> slaves;
    private ThreadPoolExecutor executor;
    private DeletePolicy deletePolicy;
    private boolean readOnly;

    public FileSystemGroup(FileSystem master, List<FileSystem> slaves, ThreadPoolExecutor executor, DeletePolicy deletePolicy, boolean readOnly) {
        this.master = master;
        this.slaves = slaves;
        this.executor = executor;
        this.deletePolicy = deletePolicy;
        this.readOnly = readOnly;

        assert deletePolicy != null;
    }

    public FileSystem getMaster() {
        return master;
    }

    public void setMaster(FileSystem master) {
        this.master = master;
    }

    public List<FileSystem> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<FileSystem> slaves) {
        this.slaves = slaves;
    }

    public void close() throws IOException {
        master.close();
        for (FileSystem slave : slaves) {
            slave.close();
        }
    }

    public void writeFile(File localFile, String ossKey) throws IOException {
        if (readOnly) {
            throw new TddlRuntimeException(ErrorCode.ERR_FILE_STORAGE_READ_ONLY);
        }

        List<FileSystem> allFileSystem = new ArrayList<>();
        allFileSystem.add(master);
        allFileSystem.addAll(slaves);

        List<Future> futures = new ArrayList<>();
        for (FileSystem fileSystem : allFileSystem) {
            Future future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    Path path = FileSystemUtils.buildPath(fileSystem, ossKey.toString());
                    try (OutputStream outputStream = fileSystem.create(path);
                         InputStream inputStream = new FileInputStream(localFile)) {
                        IOUtils.copy(inputStream, outputStream);
                    } catch (IOException e) {
                        throw GeneralUtil.nestedException(e);
                    }
                }
            });
            futures.add(future);
        }

        AsyncUtils.waitAll(futures);
    }

    public boolean exists(String ossKey) throws IOException {
        return master.exists(FileSystemUtils.buildPath(master, ossKey));
    }

    public boolean delete(String ossKey, boolean recursive) throws IOException {
        if (readOnly) {
            throw new TddlRuntimeException(ErrorCode.ERR_FILE_STORAGE_READ_ONLY);
        }

        Path path = FileSystemUtils.buildPath(master, ossKey);
        if (deletePolicy == DeletePolicy.NEVER) {
            LOGGER.info("success: delete " + path + " with policy " + deletePolicy);
            return true;
        }

        boolean ok = master.delete(FileSystemUtils.buildPath(master, ossKey), recursive);
        if (deletePolicy == DeletePolicy.MASTER_ONLY) {
            LOGGER.info((ok ? "success" : "fail") + ": delete " + path + " with policy " + deletePolicy);
            return ok;
        }

        if (!slaves.isEmpty() && deletePolicy == DeletePolicy.MASTER_SLAVE) {
            List<Future> futures = new ArrayList<>();
            for (FileSystem slave : slaves) {
                Future future = executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        boolean ok = false;
                        Path path = FileSystemUtils.buildPath(slave, ossKey);
                        try {
                            ok = slave.delete(path, recursive);
                        } catch (IOException e) {
                            // ignore
                            ok = false;
                            throw GeneralUtil.nestedException(e);
                        } finally {
                            LOGGER.info((ok ? "success" : "fail") + ": delete " + path + " with policy " + deletePolicy);
                        }
                    }
                });
                futures.add(future);
            }

            AsyncUtils.waitAll(futures);
        }

        LOGGER.info((ok ? "success" : "fail") + ": delete " + path + " with policy " + deletePolicy);
        return ok;
    }

}
