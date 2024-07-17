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

package com.alibaba.polardbx.gms.engine.decorator;

import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.RateLimitable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.token.DelegationTokenIssuer;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class FileSystemDecorator extends FileSystem implements RateLimitable {

    FileSystem delegate;
    FileSystemStrategy strategy;

    public FileSystemDecorator(FileSystem delegate, FileSystemStrategy strategy) {
        if (!(delegate instanceof FileSystemWrapper)) {
            throw new IllegalArgumentException("file system delegate must be a FileSystemWrapper");
        }
        this.delegate = delegate;
        this.strategy = strategy;
    }

    /******************* Decorated Methods *******************/

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return strategy.open(delegate, f, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        return strategy.create(delegate, f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return strategy.append(delegate, f, bufferSize, progress);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return strategy.getFileStatus(delegate, f);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path p, FsPermission permission, EnumSet<CreateFlag> flags,
                                                 int bufferSize, short replication, long blockSize,
                                                 Progressable progress) throws IOException {
        return strategy.createNonRecursive(delegate,
            p, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FileSystemRateLimiter getRateLimiter() {
        return strategy.getRateLimiter();
    }

    /******************* Forwarding Methods *******************/

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        delegate.initialize(uri, conf);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return delegate.rename(src, dst);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return delegate.delete(f, recursive);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        return delegate.listStatus(f);
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
        delegate.setWorkingDirectory(newDir);
    }

    @Override
    public Path getWorkingDirectory() {
        return delegate.getWorkingDirectory();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return delegate.mkdirs(f, permission);
    }

    @Override
    public URI getUri() {
        return delegate.getUri();
    }

    @Override
    public String getScheme() {
        return delegate.getScheme();
    }

    @Override
    public Path makeQualified(Path path) {
        return delegate.makeQualified(path);
    }

    @Override
    public FSDataOutputStreamBuilder createFile(Path path) {
        return delegate.createFile(path);
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path p) throws FileNotFoundException, IOException {
        return delegate.listStatusIterator(p);
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        return delegate.getContentSummary(f);
    }

    @Override
    public void access(Path path, FsAction action) throws IOException {
        delegate.access(path, action);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
        delegate.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
        return delegate.deleteOnExit(f);
    }

    @Override
    public boolean cancelDeleteOnExit(Path f) {
        return delegate.cancelDeleteOnExit(f);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public String getCanonicalServiceName() {
        return delegate.getCanonicalServiceName();
    }

    @Override
    public DelegationTokenIssuer[] getAdditionalTokenIssuers() throws IOException {
        return delegate.getAdditionalTokenIssuers();
    }

    @Override
    public long getDefaultBlockSize() {
        return delegate.getDefaultBlockSize();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        return delegate.globStatus(pathPattern);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
        return delegate.globStatus(pathPattern, filter);
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return delegate.exists(f);
    }

    @Override
    public boolean isDirectory(Path f) throws IOException {
        return delegate.isDirectory(f);
    }

    @Override
    public boolean isFile(Path f) throws IOException {
        return delegate.isFile(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        return delegate.getFileChecksum(f, length);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        return delegate.getXAttrs(path);
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        return delegate.listXAttrs(path);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)
        throws FileNotFoundException, IOException {
        return delegate.listFiles(f, recursive);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
        throws FileNotFoundException, IOException {
        return delegate.listLocatedStatus(f);
    }

    @Override
    public boolean hasPathCapability(Path path, String capability) throws IOException {
        return delegate.hasPathCapability(path, capability);
    }

    @Override
    public Path getHomeDirectory() {
        return delegate.getHomeDirectory();
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        return delegate.getFileBlockLocations(file, start, len);
    }

    @Override
    public void setOwner(Path path, String owner, String group) throws IOException {
        delegate.setOwner(path, owner, group);
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
        delegate.setXAttr(path, name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        return delegate.getXAttr(path, name);
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        delegate.setPermission(path, permission);
    }

    @Override
    public Configuration getConf() {
        return delegate.getConf();
    }
}
