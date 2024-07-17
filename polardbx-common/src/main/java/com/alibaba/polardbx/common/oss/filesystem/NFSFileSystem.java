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

package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.common.Engine;
import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFileOutputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class NFSFileSystem extends FileSystem implements RateLimitable {
    private static final Logger LOG =
        LoggerFactory.getLogger(NFSFileSystem.class);

    /**
     * nfs://server<:port>/path
     */
    private URI uri;

    private String username;
    private Path workingDir;

    private Cache<Path, FileStatus> metaCache;
    private boolean enableCache;

    private Nfs3 nfs3;

    /**
     * Limit the rate of file input-stream and output-stream
     */
    private FileSystemRateLimiter rateLimiter;

    private static final PathFilter DEFAULT_FILTER = new PathFilter() {
        @Override
        public boolean accept(Path file) {
            return true;
        }
    };

    public NFSFileSystem(boolean enableCache, FileSystemRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
        this.enableCache = enableCache;
        this.metaCache = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterAccess(300, SECONDS)
            .build();
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);

        if (!name.getScheme().equalsIgnoreCase(Engine.NFS.name())) {
            throw new UnsupportedOperationException(
                "Invalid schema: " + name.getScheme() + ", NFS schema is required.");
        }
        uri = name;
        username = UserGroupInformation.getCurrentUser().getShortUserName();
        workingDir = new Path("/user", username).makeQualified(uri, null);

        nfs3 = new Nfs3(name.getAuthority(), name.getPath(), new CredentialUnix(0, 0, null), 3);

        setConf(conf);
    }

    @Override
    public String getScheme() {
        return "nfs";
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        final FileStatus fileStatus = getFileStatus(path);
        if (fileStatus.isDirectory()) {
            throw new FileNotFoundException("Can't open " + path +
                " because it is a directory!");
        }

        Nfs3File nfs3File = new Nfs3File(nfs3, getNfsPath(path));
        return new FSDataInputStream(new NFSDataInputStream(nfs3File, bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean overwrite, int bufferSize, short i,
                                     long l,
                                     Progressable progressable) throws IOException {
        String nfsPath = getNfsPath(path);
        FileStatus status = null;

        try {
            // get the status or throw a FNFE
            status = getFileStatus(path);

            // if the thread reaches here, there is something at the path
            if (status.isDirectory()) {
                // path references a directory
                throw new FileAlreadyExistsException(path + " is a directory!");
            }

            if (!overwrite) {
                // path references a file and overwrite is disabled
                throw new FileAlreadyExistsException(path + " already exists");
            }
            LOG.debug("Overwriting file {}", path);
        } catch (FileNotFoundException e) {
            // this means the file is not found
        }

        if (mkdirs(path.getParent(), fsPermission)) {
            Nfs3File nfs3File = new Nfs3File(nfs3, nfsPath);
            if (!nfs3File.createNewFile()) {
                throw new FileAlreadyExistsException(path + " already exists");
            }
            return new FSDataOutputStream(new NfsFileOutputStream(nfs3File), statistics);
        } else {
            throw new IOException("Failed to create parent directory of file: " + path);
        }

    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new IOException("Append is not supported!");
    }

    @Override
    public boolean rename(Path srcPath, Path dstPath) throws IOException {
        if (srcPath.isRoot()) {
            // Cannot rename root of file system
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot rename the root of a filesystem");
            }
            return false;
        }
        Path parent = dstPath.getParent();
        while (parent != null && !srcPath.equals(parent)) {
            parent = parent.getParent();
        }
        if (parent != null) {
            return false;
        }
        FileStatus srcStatus = getFileStatus(srcPath);
        FileStatus dstStatus;
        try {
            dstStatus = getFileStatus(dstPath);
        } catch (FileNotFoundException fnde) {
            dstStatus = null;
        }
        if (dstStatus == null) {
            // If dst doesn't exist, check whether dst dir exists or not
            dstStatus = getFileStatus(dstPath.getParent());
            if (!dstStatus.isDirectory()) {
                throw new IOException(String.format(
                    "Failed to rename %s to %s, %s is a file", srcPath, dstPath,
                    dstPath.getParent()));
            }
        } else {
            if (srcStatus.getPath().equals(dstStatus.getPath())) {
                return !srcStatus.isDirectory();
            } else if (dstStatus.isDirectory()) {
                // If dst is a directory
                dstPath = new Path(dstPath, srcPath.getName());
                FileStatus[] statuses;
                try {
                    statuses = listStatus(dstPath);
                } catch (FileNotFoundException fnde) {
                    statuses = null;
                }
                if (statuses != null && statuses.length > 0) {
                    // If dst exists and not a directory / not empty
                    throw new FileAlreadyExistsException(String.format(
                        "Failed to rename %s to %s, file already exists or not empty!",
                        srcPath, dstPath));
                }
            } else {
                // If dst is not a directory
                throw new FileAlreadyExistsException(String.format(
                    "Failed to rename %s to %s, file already exists!", srcPath,
                    dstPath));
            }
        }

        boolean succeed;
        Nfs3File srcNfs3File = new Nfs3File(nfs3, getNfsPath(srcPath));
        Nfs3File dstNfs3File = new Nfs3File(nfs3, getNfsPath(dstPath));
        succeed = srcNfs3File.renameTo(dstNfs3File);

        metaCache.invalidate(srcPath);
        metaCache.invalidate(dstPath);

        return srcPath.equals(dstPath) || succeed;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        try {
            FileStatus status = getFileStatus(path);
            String nfsPath = getNfsPath(path);
            Nfs3File nfs3File = new Nfs3File(nfs3, nfsPath);
            nfs3File.delete();
        } catch (FileNotFoundException e) {
            LOG.debug("Couldn't delete {} - does not exist.", path);
            return false;
        } catch (IOException e) {
            LOG.debug("Failed to delete {}.", e.getMessage());
            return false;
        } finally {
            metaCache.invalidate(path);
        }
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        String nfsPath = getNfsPath(path);

        final List<FileStatus> result = new ArrayList<>();
        final FileStatus status = getFileStatus(path);

        if (status.isDirectory()) {

        } else {
            result.add(status);
        }

        return result.toArray(new FileStatus[result.size()]);
    }

    @Override
    public void setWorkingDirectory(Path path) {
        this.workingDir = path;
    }

    @Override
    public Path getWorkingDirectory() {
        return workingDir;
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        try {
            FileStatus status = getFileStatus(path);

            if (status.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + path);
            }
        } catch (FileNotFoundException e) {
            Path parentPath = path.getParent();
            while (true) {
                try {
                    FileStatus status = getFileStatus(parentPath);
                    if (status.isDirectory()) {
                        // If path exists, and it is a directory, the file could be created
                        break;
                    }
                } catch (FileNotFoundException fnfe) {
                    throw new FileAlreadyExistsException(String.format(
                        "Can't make directory for path '%s', it is a file.", parentPath));
                }
                parentPath = parentPath.getParent();
            }

            String nfsPath = getNfsPath(path);
            Nfs3File nfs3File = new Nfs3File(nfs3, nfsPath);
            try {
                nfs3File.mkdirs();
            } catch (Throwable t) {
                return false;
            }
            return true;
        }
    }

    /**
     * Turn a path (relative or otherwise) into an NFS path
     *
     * @param path the path of the file.
     * @return the path that represents the file, relative to nfs uri.
     */
    private String getNfsPath(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(workingDir, path);
        }

        if (uri.getPath().equals(path.toUri().getPath() + "/")) {
            /* is root */
            return "/";
        } else {
            return "/" + uri.relativize(path.toUri()).getPath();
        }
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        final Callable<FileStatus> valueLoader = () -> {
            return getFileStatusImpl(path);
        };
        FileStatus fileStatus;
        try {
            if (enableCache) {
                fileStatus = metaCache.get(path, valueLoader);
            } else {
                fileStatus = getFileStatusImpl(path);
            }
        } catch (FileNotFoundException fnfe) {
            throw fnfe;
        } catch (ExecutionException ex) {
            fileStatus = getFileStatusImpl(path);
        }
        return fileStatus;
    }

    public FileStatus getFileStatusImpl(Path path) throws IOException {
        Path qualifiedPath = path.makeQualified(uri, workingDir);
        String nfsPath = getNfsPath(qualifiedPath);

        // Root always exists
        if (nfsPath.length() == 1) {
            return new NFSFileStatus(0, true, 1, 0, 0, qualifiedPath, username);
        }

        Nfs3File nfs3File = new Nfs3File(nfs3, nfsPath);

        if (!nfs3File.exists()) {
            throw new FileNotFoundException(path + ": No such file or directory!");
        } else if (nfs3File.isDirectory()) {
            return new NFSFileStatus(0, true, 1, 0,
                nfs3File.getAttributes().getMtime().getTimeInMillis(),
                qualifiedPath, username);
        } else {
            return new NFSFileStatus(0, false, 1, getDefaultBlockSize(path),
                nfs3File.getAttributes().getMtime().getTimeInMillis(),
                qualifiedPath, username);
        }
    }

    @Override
    public FileSystemRateLimiter getRateLimiter() {
        return this.rateLimiter;
    }

    public Cache<Path, FileStatus> getMetaCache() {
        return metaCache;
    }
}
