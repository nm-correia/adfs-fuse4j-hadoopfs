package fuse4j.hadoopfs;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import fuse.Errno;
import fuse.Filesystem3;
import fuse.FilesystemConstants;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfsSetter;
import fuse.LifecycleSupport;
import fuse.XattrLister;
import fuse.XattrSupport;
import fuse.util.FuseArgumentParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;


public class FuseHdfsClient implements Filesystem3, XattrSupport, LifecycleSupport {

    private static final String LOCALHOST_HDFS = "hdfs://localhost:9000";

    private static final Log log = LogFactory.getLog(FuseHdfsClient.class);

    private static final int BLOCK_SIZE = 512;
    private static final int NAME_LENGTH = 1024;

    private HdfsClient hdfs = null;

    public FuseHdfsClient() {
        this(LOCALHOST_HDFS);
    }

    public FuseHdfsClient(String [] args) {
        this(new FuseArgumentParser(args).getSource());
    }

    public FuseHdfsClient(String hdfsUrl) {
        hdfs = HdfsClientFactory.create(hdfsUrl);
        log.info("created");
    }

    public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        log.info("getattr(): " + path);
        HdfsFileAttr s = hdfs.getFileInfo(path);

        if(s == null) {
            return Errno.ENOENT;
        }

        long size;
        long blocks;
        if(s.directory) {
            size = 512;
            blocks = 2;
        } else {
            size = s.size;
            blocks = (s.size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        }

        getattrSetter.set(
            s.inode, s.getMode(), s.numberOfLinks,
            s.uid, s.gid, 0,
            size, blocks,
            s.accessTime, s.modifiedTime, s.createTime
        );

        return 0;
    }


    public int readlink(String path, CharBuffer link) throws FuseException {
        // Not Supported
        log.info("readlink(): " + path);
        return Errno.ENOENT;
    }

    public int getdir(String path, FuseDirFiller filler) throws FuseException {
        log.info("getdir(): " + path);

        HdfsDirEntry[] entries = hdfs.listPaths(path);

        if(entries == null) {
            return FuseException.ENOTDIR;
        }

        for(HdfsDirEntry entry : entries) {
            filler.add(entry.name, entry.hashCode(), entry.getMode());
        }

        return 0;
    }

    /**
     * mknod()
     */
    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod(): " + path + " mode: " + mode);

        boolean status = hdfs.mknod(path);
        
        if(!status)
        	return FuseException.EPERM;
        
        return 0;
    }

    public int mkdir(String path, int mode) throws FuseException {
        log.info("mkdir(): " + path + " mode: " + mode);

        boolean status = hdfs.mkdir(path);

        if(!status) {
            return FuseException.EACCES;
        }
        return 0;
    }

    public int unlink(String path) throws FuseException {
        log.info("unlink(): " + path);

        boolean status = hdfs.unlink(path);

        if(!status) {
            return FuseException.EACCES;
        }
        return 0;
    }

    public int rmdir(String path) throws FuseException {
        log.info("rmdir(): " + path);

        boolean status = hdfs.rmdir(path);

        if(!status) {
            return FuseException.EACCES;
        }

        return 0;
    }

    public int symlink(String from, String to) throws FuseException {
        //symlink not supported")
        return FuseException.ENOSYS;
    }

    public int rename(String from, String to) throws FuseException {
        log.info("rename(): " + from + " to: " + to);

        boolean status = hdfs.rename(from, to);

        if(!status) {
            return FuseException.EPERM;
        }
        return 0;
    }

    public int link(String from, String to) throws FuseException {
        log.info("link(): " + from);
        //link not supported")
        return FuseException.ENOSYS;
    }

    public int chmod(String path, int mode) throws FuseException {
        log.info("chmod(): " + path);
        // chmod not supported
        // but silently ignore the requests.
        return 0;
    }

    public int chown(String path, int uid, int gid) throws FuseException {
        log.info("chown(): " + path);
        throw new FuseException("chown not supported")
            .initErrno(FuseException.ENOSYS);
    }

    public int truncate(String path, long size) throws FuseException {
        log.info("truncate(): " + path + " size: " + size);
        // Not supported
        return FuseException.EPERM;
    }

    public int utime(String path, int atime, int mtime) throws FuseException {
        // not supported right now (write-once files...)
        log.info("utime(): " + path);
        return FuseException.ENOSYS;
    }


    public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
        statfsSetter.set(
                BLOCK_SIZE,
                100000000,
                90000000,
                90000000,
                1000000, // TODO get actual file count.
                0,
                NAME_LENGTH
        );
        return 0;
        
        /*if(hdfs.statfs(statfsSetter, BLOCK_SIZE, NAME_LENGTH))
            return 0;
        else
            return FuseException.EPERM;*/
    }


    /**
     * open()
     */
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        log.info("open(): " + path + " flags " + flags);

        HdfsClientFH fh = new HdfsClientFH();
        
        if(isReadOnlyAccess(flags))
        	fh.setIn(hdfs.openForRead(path));
        else if(isWriteOnlyAccess(flags))
        	fh.setOut(hdfs.openForWrite(path));
        else {
        	fh.setIn(hdfs.openForRead(path));
        	fh.setOut(hdfs.openForWrite(path));
        }
        
        if(fh.getIn() == null && fh.getOut() == null)
        	return FuseException.EACCES;
        
        openSetter.setFh(fh);
        return 0;
    }

    // fh is filehandle passed from open
    /**
     * read()
     */
    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
        log.info("read(): " + path + " offset: " + offset + " len: "
            + buf.capacity());
        
        HdfsClientFH fhhdfs = (HdfsClientFH)fh;

        boolean status = hdfs.read(fhhdfs.getIn(), buf, offset);

        if(!status) {
            // read failed
            return FuseException.EACCES;
        }

        return 0;
    }

    // fh is filehandle passed from open,
    // isWritepage indicates that write was caused by a writepage
    /**
     * write()
     */
    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        log.info("write(): " + path + " offset: " + offset + " len: "
            + buf.capacity());

        HdfsClientFH fhhdfs = (HdfsClientFH)fh;

        boolean status = hdfs.write(fhhdfs.getOut(), buf, offset);

        if(!status) {
            // read failed
            return FuseException.EACCES;
        }

        return 0;
    }

    // (called when last filehandle is closed), fh is filehandle passed from open
    public int release(String path, Object fh, int flags) throws FuseException {
        log.info("release(): " + path + " flags: " + flags);
        
        HdfsClientFH fhhdfs = (HdfsClientFH)fh;
        Object in = fhhdfs.getIn();
        Object out = fhhdfs.getOut();
        boolean status = true;
        
        if(in != null)
        	status = status && hdfs.close(in);
        
        if(out != null)
        	status = status &&  hdfs.close(out);

        if(!status) {
            // read failed
            return FuseException.EACCES;
        }
        
        return 0;
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public int flush(String path, Object fh) throws FuseException {
        return 0;

    }

    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        return 0;
    }


    public int getxattr(String path, String name, ByteBuffer dst, int position) throws FuseException, BufferOverflowException {
        return 0;
    }

    public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException {
        return Errno.ENOATTR;
    }

    public int listxattr(String path, XattrLister lister) throws FuseException {
        return 0;
    }

    public int removexattr(String path, String name) throws FuseException {
        return 0;
    }

    public int setxattr(String path, String name, ByteBuffer value, int flags, int position) throws FuseException {
        return 0;
    }

    // LifeCycleSupport
    public int init() {
        log.info("Initializing Filesystem");
        return 0;
    }

    public int destroy() {
        log.info("Destroying Filesystem");

        try {
            System.exit(0);
        } catch (Exception e) {
        }

        return 0;
    }

    //
    // Java entry point
    public static void main(String[] args) {
        log.info("entering");

        if(args.length == 0) {
        	System.exit(1);
        	System.out.println("Usage: <hdfs_url> <fuse_args>");
        }

        String[] fuse_args = new String[args.length-1];
        System.arraycopy(args, 1, fuse_args, 0, fuse_args.length);

        try {
            FuseMount.mount(fuse_args, new FuseHdfsClient(args[0]), log);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            log.info("exiting");
        }
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //
    // Private Methods
    //

    /**
     * isWriteOnlyAccess()
     */
    private boolean isWriteOnlyAccess(int flags) {
        return ((flags & 0x0FF) == FilesystemConstants.O_WRONLY);
    }

    /**
     * isReadOnlyAccess()
     */
    private boolean isReadOnlyAccess(int flags) {
        return ((flags & 0x0FF) == FilesystemConstants.O_RDONLY);
    }

}

class HdfsClientFH {
	private Object inputStream;
	private Object outputStream;
	
	public Object getIn()          { return inputStream; }
	public void setIn(Object in)   { inputStream = in; }
	
	public Object getOut()         { return outputStream; }
	public void setOut(Object out) { outputStream = out; }
}
