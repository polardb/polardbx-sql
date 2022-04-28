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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class FileStorageMetaStore {

    private static final Logger logger = LoggerFactory.getLogger("oss");

    public static String MATA_STORE_FILE_PATH = "meta/files_meta.txt";

    public static String MATA_STORE_FILE_LOCAL_PATH = "/tmp/files_meta.txt";

    private Engine engine;

    protected Connection connection;

    private FileStorageFilesMetaAccessor fileStorageFilesMetaAccessor;

    public FileStorageMetaStore(Engine engine) {
        this.engine = engine;
        this.fileStorageFilesMetaAccessor = new FileStorageFilesMetaAccessor();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
        this.fileStorageFilesMetaAccessor.setConnection(connection);
    }

    public List<OssFileMeta> queryFromFileStorage() {
        List<OssFileMeta> fileMetaList = new ArrayList<>();
        long stamp = FileSystemManager.readLockWithTimeOut(engine);
        try {
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine);
            Path path = new Path(MATA_STORE_FILE_PATH);
            FileSystem fileSystem = fileSystemGroup.getMaster();

            boolean fileExists = false;
            FileStatus fileStatus;

            if (fileSystem instanceof FileMergeCachingFileSystem) {
                FileMergeCachingFileSystem fileMergeCachingFileSystem = (FileMergeCachingFileSystem) fileSystem;
                if (fileMergeCachingFileSystem.getDataTier() instanceof OSSFileSystem) {
                    OSSFileSystem ossFileSystem = (OSSFileSystem) fileMergeCachingFileSystem.getDataTier();
                    // NOTE: invalidate meta cache
                    ossFileSystem.getMetaCache().invalidate(path);
                    // NOTE: replace filesystem, bypass data cache
                    fileSystem = ossFileSystem;
                }
            }

            try {
                fileStatus = fileSystem.getFileStatus(path);
                if (fileStatus != null) {
                    fileExists = true;
                }
            } catch (FileNotFoundException e) {
                fileExists = false;
                fileStatus = null;
            }

            if (fileExists) {
                int length = (int) fileStatus.getLen();
                try (FSDataInputStream fsDataInputStream = fileSystem.open(path)) {
                    byte[] buf = new byte[length];
                    fsDataInputStream.readFully(0, buf);
                    String json = new String(buf, StandardCharsets.UTF_8);
                    JSONObject jsonObject = JSON.parseObject(json);
                    JSONArray jsonArray = jsonObject.getJSONArray("files");
                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject fileJson = jsonArray.getJSONObject(i);
                        OssFileMeta ossFileMeta = new OssFileMeta(fileJson.getString("fileName"), fileJson.getLongValue("commitTs"), fileJson.getLong("removeTs"));
                        fileMetaList.add(ossFileMeta);
                    }
                }
            }

            return fileMetaList;
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        } finally {
            FileSystemManager.unlockRead(engine, stamp);
        }
    }

    public List<FileStorageFilesMetaRecord> queryFromMetaDb() {
        return fileStorageFilesMetaAccessor.query(engine);
    }

    public List<FileStorageFilesMetaRecord> queryTableNeedToPurge(long removeTs) {
        return fileStorageFilesMetaAccessor.queryTableNeedToPurge(engine, removeTs);
    }

    public void backup() {
        List<FileStorageFilesMetaRecord> recordList = fileStorageFilesMetaAccessor.query(engine);
        JSONObject jsonObject = new JSONObject();
        JSONArray filesJsonArray = new JSONArray();
        jsonObject.put("files", filesJsonArray);

        for (FileStorageFilesMetaRecord record : recordList) {
            JSONObject bucketJson = new JSONObject();
            bucketJson.put("fileName", record.fileName);
            bucketJson.put("commitTs", record.commitTs);
            bucketJson.put("removeTs", record.removeTs);
            filesJsonArray.add(bucketJson);
        }
        String str = jsonObject.toJSONString();

        File file = FileSystemUtils.createLocalFile(MATA_STORE_FILE_LOCAL_PATH);
        try {
            //  write local file
            try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
                fileOutputStream.write(str.getBytes(StandardCharsets.UTF_8));
                fileOutputStream.flush();
            } catch (IOException e) {
                throw new TddlNestableRuntimeException(e);
            }

            // upload to oss
            try {
                FileSystemUtils.writeFile(file, MATA_STORE_FILE_PATH, engine);
            } catch (IOException e) {
                throw new TddlNestableRuntimeException(e);
            }
        } finally {
            // delete tmp file
            file.delete();
        }
    }

    private void updateOssMetaFileTs(String fileName, Long ts, boolean commit) {
        List<FileStorageFilesMetaRecord> fileStorageFilesMetaRecords = fileStorageFilesMetaAccessor.query(fileName);
        if (fileStorageFilesMetaRecords.isEmpty()) {
            FileStorageFilesMetaRecord record = new FileStorageFilesMetaRecord();
            record.fileName = fileName;
            record.engine = engine.name();
            if (commit) {
                record.commitTs = ts;
            } else {
                record.removeTs = ts;
            }
            fileStorageFilesMetaAccessor.replace(ImmutableList.of(record));
        } else {
            assert fileStorageFilesMetaRecords.size() == 1;
            FileStorageFilesMetaRecord record = fileStorageFilesMetaRecords.get(0);
            if (commit) {
                fileStorageFilesMetaAccessor.updateCommitTs(record.id, ts);
            } else {
                fileStorageFilesMetaAccessor.updateRemoveTs(record.id, ts);
            }
        }
    }

    public void updateFileCommitTs(String fileName, long ts) {
        updateOssMetaFileTs(fileName, ts, true);
    }

    public void updateFileRemoveTs(String fileName, long ts) {
        updateOssMetaFileTs(fileName, ts, false);
    }

    public void deleteFileRemoveTs(String fileName) {
        updateOssMetaFileTs(fileName, null, false);
    }

    public void deleteAll() {
        fileStorageFilesMetaAccessor.delete(engine);
    }

    public void deleteFile(String fileName) {
        fileStorageFilesMetaAccessor.deleteByFileName(fileName);
    }

    public static class OssFileMeta {
        String dataPath;
        Long commitTs;
        Long removeTs;

        public OssFileMeta(String dataPath, Long commitTs, Long removeTs) {
            this.commitTs = commitTs;
            this.removeTs = removeTs;
            this.dataPath = dataPath;
        }

        public String getDataPath() {
            return dataPath;
        }

        public Long getCommitTs() {
            return commitTs;
        }

        public Long getRemoveTs() {
            return removeTs;
        }
    }
}
