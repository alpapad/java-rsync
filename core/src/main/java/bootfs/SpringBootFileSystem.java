/*
 * Copyright (c) 2009, 2017, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package bootfs;

import static bootfs.ZipConstants.CENCOM;
import static bootfs.ZipConstants.CENCRC;
import static bootfs.ZipConstants.CENEXT;
import static bootfs.ZipConstants.CENFLG;
import static bootfs.ZipConstants.CENHDR;
import static bootfs.ZipConstants.CENHOW;
import static bootfs.ZipConstants.CENLEN;
import static bootfs.ZipConstants.CENNAM;
import static bootfs.ZipConstants.CENOFF;
import static bootfs.ZipConstants.CENSIG;
import static bootfs.ZipConstants.CENSIZ;
import static bootfs.ZipConstants.CENTIM;
import static bootfs.ZipConstants.CENVER;
import static bootfs.ZipConstants.CH;
import static bootfs.ZipConstants.ENDCOM;
import static bootfs.ZipConstants.ENDHDR;
import static bootfs.ZipConstants.ENDOFF;
import static bootfs.ZipConstants.ENDSIG;
import static bootfs.ZipConstants.ENDSIZ;
import static bootfs.ZipConstants.ENDSUB;
import static bootfs.ZipConstants.ENDTOT;
import static bootfs.ZipConstants.END_MAXLEN;
import static bootfs.ZipConstants.EXTID_EXTT;
import static bootfs.ZipConstants.EXTID_NTFS;
import static bootfs.ZipConstants.EXTID_ZIP64;
import static bootfs.ZipConstants.EXTSIG;
import static bootfs.ZipConstants.FLAG_DATADESCR;
import static bootfs.ZipConstants.FLAG_EFS;
import static bootfs.ZipConstants.LG;
import static bootfs.ZipConstants.LL;
import static bootfs.ZipConstants.LOCEXT;
import static bootfs.ZipConstants.LOCHDR;
import static bootfs.ZipConstants.LOCNAM;
import static bootfs.ZipConstants.LOCSIG;
import static bootfs.ZipConstants.METHOD_DEFLATED;
import static bootfs.ZipConstants.METHOD_STORED;
import static bootfs.ZipConstants.READBLOCKSZ;
import static bootfs.ZipConstants.SH;
import static bootfs.ZipConstants.ZIP64_ENDHDR;
import static bootfs.ZipConstants.ZIP64_ENDOFF;
import static bootfs.ZipConstants.ZIP64_ENDSIG;
import static bootfs.ZipConstants.ZIP64_ENDSIZ;
import static bootfs.ZipConstants.ZIP64_ENDTOT;
import static bootfs.ZipConstants.ZIP64_LOCHDR;
import static bootfs.ZipConstants.ZIP64_LOCOFF;
import static bootfs.ZipConstants.ZIP64_LOCSIG;
import static bootfs.ZipConstants.ZIP64_MINVAL;
import static bootfs.ZipConstants.ZIP64_MINVAL32;
import static bootfs.ZipConstants.cenSigAt;
import static bootfs.ZipConstants.getSig;
import static bootfs.ZipConstants.locSigAt;
import static bootfs.ZipUtils.dosToJavaTime;
import static bootfs.ZipUtils.javaToDosTime;
import static bootfs.ZipUtils.javaToUnixTime;
import static bootfs.ZipUtils.javaToWinTime;
import static bootfs.ZipUtils.toDirectoryPath;
import static bootfs.ZipUtils.toRegexPattern;
import static bootfs.ZipUtils.unixToJavaTime;
import static bootfs.ZipUtils.winToJavaTime;
import static bootfs.ZipUtils.writeBytes;
import static bootfs.ZipUtils.writeInt;
import static bootfs.ZipUtils.writeLong;
import static bootfs.ZipUtils.writeShort;
import static java.lang.Boolean.TRUE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.ReadOnlyFileSystemException;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.ZipException;

/**
 * A FileSystem built on a zip file
 *
 * @author Xueming Shen
 */

public class SpringBootFileSystem extends FileSystem {
    
    // End of central directory record
    static class END {
        long cenlen; // 4 bytes
        long cenoff; // 4 bytes
        int centot; // 4 bytes
        int comlen; // comment length
        byte[] comment;
        /* members of Zip64 end of central directory locator */
        // int diskNum;
        long endpos;
        // int disktot;
        
        // these 2 fields are not used by anyone and write() uses "0"
        // int disknum;
        // int sdisknum;
        int endsub; // endsub
        
        void write(OutputStream os, long offset) throws IOException {
            boolean hasZip64 = false;
            long xlen = cenlen;
            long xoff = cenoff;
            if (xlen >= ZIP64_MINVAL) {
                xlen = ZIP64_MINVAL;
                hasZip64 = true;
            }
            if (xoff >= ZIP64_MINVAL) {
                xoff = ZIP64_MINVAL;
                hasZip64 = true;
            }
            int count = centot;
            if (count >= ZIP64_MINVAL32) {
                count = ZIP64_MINVAL32;
                hasZip64 = true;
            }
            if (hasZip64) {
                long off64 = offset;
                // zip64 end of central directory record
                writeInt(os, ZIP64_ENDSIG); // zip64 END record signature
                writeLong(os, ZIP64_ENDHDR - 12); // size of zip64 end
                writeShort(os, 45); // version made by
                writeShort(os, 45); // version needed to extract
                writeInt(os, 0); // number of this disk
                writeInt(os, 0); // central directory start disk
                writeLong(os, centot); // number of directory entries on disk
                writeLong(os, centot); // number of directory entries
                writeLong(os, cenlen); // length of central directory
                writeLong(os, cenoff); // offset of central directory
                
                // zip64 end of central directory locator
                writeInt(os, ZIP64_LOCSIG); // zip64 END locator signature
                writeInt(os, 0); // zip64 END start disk
                writeLong(os, off64); // offset of zip64 END
                writeInt(os, 1); // total number of disks (?)
            }
            writeInt(os, ENDSIG); // END record signature
            writeShort(os, 0); // number of this disk
            writeShort(os, 0); // central directory start disk
            writeShort(os, count); // number of directory entries on disk
            writeShort(os, count); // total number of directory entries
            writeInt(os, xlen); // length of central directory
            writeInt(os, xoff); // offset of central directory
            if (comment != null) { // zip file comment
                writeShort(os, comment.length);
                writeBytes(os, comment);
            } else {
                writeShort(os, 0);
            }
        }
    }
    
    static class Entry extends IndexNode implements ZipFileAttributes {
        
        static final int CEN = 1; // entry read from cen
        static final int COPY = 4; // copy of a CEN entry
        static final int FILECH = 3; // fch update in "file"
        static final int NEW = 2; // updated contents in bytes or file
        
        ///////////////////// CEN //////////////////////
        static Entry readCEN(SpringBootFileSystem zipfs, IndexNode inode) throws IOException {
            return new Entry().cen(zipfs, inode);
        }
        
        long atime = -1; // last access time
        byte[] bytes; // updated content bytes
        
        byte[] comment;
        long crc = -1; // crc-32 of entry data
        long csize = -1; // compressed size of entry data
        long ctime = -1; // create time
        byte[] extra;
        Path file; // use tmp file to store bytes;
        int flag;
        // these fields are not used by anyone and writeCEN uses "0"
        // int versionMade;
        // int disk;
        // int attrs;
        // long attrsEx;
        long locoff;
        int method = -1; // compression method
        long mtime = -1; // last modification time (in DOS time)
        
        // cen
        
        long size = -1; // uncompressed size of entry data
        int type = CEN; // default is the entry read from cen
        
        // entry attributes
        int version;
        
        Entry() {
        }
        
        Entry(byte[] name, boolean isdir) {
            name(name);
            this.isdir = isdir;
            mtime = ctime = atime = System.currentTimeMillis();
            crc = 0;
            size = 0;
            csize = 0;
            method = METHOD_STORED;
        }
        
        Entry(byte[] name, int type, boolean isdir) {
            this(name, isdir);
            this.type = type;
        }
        
        Entry(byte[] name, Path file, int type) {
            this(name, type, false);
            this.file = file;
            method = METHOD_STORED;
        }
        
        Entry(Entry e, int type) {
            name(e.name);
            isdir = e.isdir;
            version = e.version;
            ctime = e.ctime;
            atime = e.atime;
            mtime = e.mtime;
            crc = e.crc;
            size = e.size;
            csize = e.csize;
            method = e.method;
            extra = e.extra;
            /*
             * this.versionMade = e.versionMade; this.disk = e.disk; this.attrs = e.attrs;
             * this.attrsEx = e.attrsEx;
             */
            locoff = e.locoff;
            comment = e.comment;
            this.type = type;
        }
        
        private Entry cen(SpringBootFileSystem zipfs, IndexNode inode) throws IOException {
            byte[] cen = zipfs.cen;
            int pos = inode.pos;
            if (!cenSigAt(cen, pos)) {
                zerror("invalid CEN header (bad signature)");
            }
            version = CENVER(cen, pos);
            flag = CENFLG(cen, pos);
            method = CENHOW(cen, pos);
            mtime = dosToJavaTime(CENTIM(cen, pos));
            crc = CENCRC(cen, pos);
            csize = CENSIZ(cen, pos);
            size = CENLEN(cen, pos);
            int nlen = CENNAM(cen, pos);
            int elen = CENEXT(cen, pos);
            int clen = CENCOM(cen, pos);
            /*
             * versionMade = CENVEM(cen, pos); disk = CENDSK(cen, pos); attrs = CENATT(cen,
             * pos); attrsEx = CENATX(cen, pos);
             */
            locoff = CENOFF(cen, pos);
            pos += CENHDR;
            name = inode.name;
            isdir = inode.isdir;
            hashcode = inode.hashcode;
            
            pos += nlen;
            if (elen > 0) {
                extra = Arrays.copyOfRange(cen, pos, pos + elen);
                pos += elen;
                readExtra(zipfs);
            }
            if (clen > 0) {
                comment = Arrays.copyOfRange(cen, pos, pos + clen);
            }
            return this;
        }
        
        @Override
        public byte[] comment() {
            if (comment != null) {
                return Arrays.copyOf(comment, comment.length);
            }
            return null;
        }
        
        ///////// zip entry attributes ///////////
        @Override
        public long compressedSize() {
            return csize;
        }
        
        ///////////////////// LOC //////////////////////
        
        @Override
        public long crc() {
            return crc;
        }
        
        ///////// basic file attributes ///////////
        @Override
        public FileTime creationTime() {
            return FileTime.fromMillis(ctime == -1 ? mtime : ctime);
        }
        
        @Override
        public byte[] extra() {
            if (extra != null) {
                return Arrays.copyOf(extra, extra.length);
            }
            return null;
        }
        
        @Override
        public Object fileKey() {
            return null;
        }
        
        @Override
        public boolean isDirectory() {
            return isDir();
        }
        
        @Override
        public boolean isOther() {
            return false;
        }
        
        @Override
        public boolean isRegularFile() {
            return !isDir();
        }
        
        @Override
        public boolean isSymbolicLink() {
            return false;
        }
        
        @Override
        public FileTime lastAccessTime() {
            return FileTime.fromMillis(atime == -1 ? mtime : atime);
        }
        
        @Override
        public FileTime lastModifiedTime() {
            return FileTime.fromMillis(mtime);
        }
        
        @Override
        public int method() {
            return method;
        }
        
        // read NTFS, UNIX and ZIP64 data from cen.extra
        void readExtra(SpringBootFileSystem zipfs) throws IOException {
            if (extra == null) {
                return;
            }
            int elen = extra.length;
            int off = 0;
            int newOff = 0;
            while (off + 4 < elen) {
                // extra spec: HeaderID+DataSize+Data
                int pos = off;
                int tag = SH(extra, pos);
                int sz = SH(extra, pos + 2);
                pos += 4;
                if (pos + sz > elen) {
                    break;
                }
                switch (tag) {
                    case EXTID_ZIP64:
                        if (size == ZIP64_MINVAL) {
                            if (pos + 8 > elen) {
                                break; // fields, just skip
                            }
                            size = LL(extra, pos);
                            pos += 8;
                        }
                        if (csize == ZIP64_MINVAL) {
                            if (pos + 8 > elen) {
                                break;
                            }
                            csize = LL(extra, pos);
                            pos += 8;
                        }
                        if (locoff == ZIP64_MINVAL) {
                            if (pos + 8 > elen) {
                                break;
                            }
                            locoff = LL(extra, pos);
                            pos += 8;
                        }
                        break;
                    case EXTID_NTFS:
                        if (sz < 32) {
                            break;
                        }
                        pos += 4; // reserved 4 bytes
                        if (SH(extra, pos) != 0x0001) {
                            break;
                        }
                        if (SH(extra, pos + 2) != 24) {
                            break;
                        }
                        // override the loc field, datatime here is
                        // more "accurate"
                        mtime = winToJavaTime(LL(extra, pos + 4));
                        atime = winToJavaTime(LL(extra, pos + 12));
                        ctime = winToJavaTime(LL(extra, pos + 20));
                        break;
                    case EXTID_EXTT:
                        // spec says the Extened timestamp in cen only has mtime
                        // need to read the loc to get the extra a/ctime, if flag
                        // "zipinfo-time" is not specified to false;
                        // there is performance cost (move up to loc and read) to
                        // access the loc table foreach entry;
                        if (zipfs.noExtt) {
                            if (sz == 5) {
                                mtime = unixToJavaTime(LG(extra, pos + 1));
                            }
                            break;
                        }
                        byte[] buf = new byte[LOCHDR];
                        if (zipfs.readFullyAt(buf, 0, buf.length, locoff) != buf.length) {
                            throw new ZipException("loc: reading failed");
                        }
                        if (!locSigAt(buf, 0)) {
                            throw new ZipException("loc: wrong sig ->" + Long.toString(getSig(buf, 0), 16));
                        }
                        int locElen = LOCEXT(buf);
                        if (locElen < 9) {
                            break;
                        }
                        int locNlen = LOCNAM(buf);
                        buf = new byte[locElen];
                        if (zipfs.readFullyAt(buf, 0, buf.length, locoff + LOCHDR + locNlen) != buf.length) {
                            throw new ZipException("loc extra: reading failed");
                        }
                        int locPos = 0;
                        while (locPos + 4 < buf.length) {
                            int locTag = SH(buf, locPos);
                            int locSZ = SH(buf, locPos + 2);
                            locPos += 4;
                            if (locTag != EXTID_EXTT) {
                                locPos += locSZ;
                                continue;
                            }
                            int end = locPos + locSZ - 4;
                            int flag = CH(buf, locPos++);
                            if ((flag & 0x1) != 0 && locPos <= end) {
                                mtime = unixToJavaTime(LG(buf, locPos));
                                locPos += 4;
                            }
                            if ((flag & 0x2) != 0 && locPos <= end) {
                                atime = unixToJavaTime(LG(buf, locPos));
                                locPos += 4;
                            }
                            if ((flag & 0x4) != 0 && locPos <= end) {
                                ctime = unixToJavaTime(LG(buf, locPos));
                                locPos += 4;
                            }
                            break;
                        }
                        break;
                    default: // unknown tag
                        System.arraycopy(extra, off, extra, newOff, sz + 4);
                        newOff += sz + 4;
                }
                off += sz + 4;
            }
            if (newOff != 0 && newOff != extra.length) {
                extra = Arrays.copyOf(extra, newOff);
            } else {
                extra = null;
            }
        }
        
        @Override
        public long size() {
            return size;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(1024);
            Formatter fm = new Formatter(sb);
            fm.format("    name            : %s%n", new String(name));
            fm.format("    creationTime    : %tc%n", creationTime().toMillis());
            fm.format("    lastAccessTime  : %tc%n", lastAccessTime().toMillis());
            fm.format("    lastModifiedTime: %tc%n", lastModifiedTime().toMillis());
            fm.format("    isRegularFile   : %b%n", isRegularFile());
            fm.format("    isDirectory     : %b%n", isDirectory());
            fm.format("    isSymbolicLink  : %b%n", isSymbolicLink());
            fm.format("    isOther         : %b%n", isOther());
            fm.format("    fileKey         : %s%n", fileKey());
            fm.format("    size            : %d%n", size());
            fm.format("    compressedSize  : %d%n", compressedSize());
            fm.format("    crc             : %x%n", crc());
            fm.format("    method          : %d%n", method());
            fm.close();
            return sb.toString();
        }
        
        int version() throws ZipException {
            if (method == METHOD_DEFLATED) {
                return 20;
            } else if (method == METHOD_STORED) {
                return 10;
            }
            throw new ZipException("unsupported compression method");
        }
        
        int writeCEN(OutputStream os) throws IOException {
            int version0 = version();
            long csize0 = csize;
            long size0 = size;
            long locoff0 = locoff;
            int elen64 = 0; // extra for ZIP64
            int elenNTFS = 0; // extra for NTFS (a/c/mtime)
            int elenEXTT = 0; // extra for Extended Timestamp
            boolean foundExtraTime = false; // if time stamp NTFS, EXTT present
            
            byte[] zname = isdir ? toDirectoryPath(name) : name;
            
            // confirm size/length
            int nlen = zname != null ? zname.length - 1 : 0; // name has [0] as "slash"
            int elen = extra != null ? extra.length : 0;
            int eoff = 0;
            int clen = comment != null ? comment.length : 0;
            if (csize >= ZIP64_MINVAL) {
                csize0 = ZIP64_MINVAL;
                elen64 += 8; // csize(8)
            }
            if (size >= ZIP64_MINVAL) {
                size0 = ZIP64_MINVAL; // size(8)
                elen64 += 8;
            }
            if (locoff >= ZIP64_MINVAL) {
                locoff0 = ZIP64_MINVAL;
                elen64 += 8; // offset(8)
            }
            if (elen64 != 0) {
                elen64 += 4; // header and data sz 4 bytes
            }
            while (eoff + 4 < elen) {
                int tag = SH(extra, eoff);
                int sz = SH(extra, eoff + 2);
                if (tag == EXTID_EXTT || tag == EXTID_NTFS) {
                    foundExtraTime = true;
                }
                eoff += 4 + sz;
            }
            if (!foundExtraTime) {
                if (isWindows) { // use NTFS
                    elenNTFS = 36; // total 36 bytes
                } else { // Extended Timestamp otherwise
                    elenEXTT = 9; // only mtime in cen
                }
            }
            writeInt(os, CENSIG); // CEN header signature
            if (elen64 != 0) {
                writeShort(os, 45); // ver 4.5 for zip64
                writeShort(os, 45);
            } else {
                writeShort(os, version0); // version made by
                writeShort(os, version0); // version needed to extract
            }
            writeShort(os, flag); // general purpose bit flag
            writeShort(os, method); // compression method
                                    // last modification time
            writeInt(os, (int) javaToDosTime(mtime));
            writeInt(os, crc); // crc-32
            writeInt(os, csize0); // compressed size
            writeInt(os, size0); // uncompressed size
            writeShort(os, nlen);
            writeShort(os, elen + elen64 + elenNTFS + elenEXTT);
            
            if (comment != null) {
                writeShort(os, Math.min(clen, 0xffff));
            } else {
                writeShort(os, 0);
            }
            writeShort(os, 0); // starting disk number
            writeShort(os, 0); // internal file attributes (unused)
            writeInt(os, 0); // external file attributes (unused)
            writeInt(os, locoff0); // relative offset of local header
            writeBytes(os, zname, 1, nlen);
            if (elen64 != 0) {
                writeShort(os, EXTID_ZIP64);// Zip64 extra
                writeShort(os, elen64 - 4); // size of "this" extra block
                if (size0 == ZIP64_MINVAL) {
                    writeLong(os, size);
                }
                if (csize0 == ZIP64_MINVAL) {
                    writeLong(os, csize);
                }
                if (locoff0 == ZIP64_MINVAL) {
                    writeLong(os, locoff);
                }
            }
            if (elenNTFS != 0) {
                writeShort(os, EXTID_NTFS);
                writeShort(os, elenNTFS - 4);
                writeInt(os, 0); // reserved
                writeShort(os, 0x0001); // NTFS attr tag
                writeShort(os, 24);
                writeLong(os, javaToWinTime(mtime));
                writeLong(os, javaToWinTime(atime));
                writeLong(os, javaToWinTime(ctime));
            }
            if (elenEXTT != 0) {
                writeShort(os, EXTID_EXTT);
                writeShort(os, elenEXTT - 4);
                if (ctime == -1) {
                    os.write(0x3); // mtime and atime
                } else {
                    os.write(0x7); // mtime, atime and ctime
                }
                writeInt(os, javaToUnixTime(mtime));
            }
            if (extra != null) {
                writeBytes(os, extra);
            }
            if (comment != null) {
                writeBytes(os, comment);
            }
            return CENHDR + nlen + elen + clen + elen64 + elenNTFS + elenEXTT;
        }
        
        // Data Descriptior
        int writeEXT(OutputStream os) throws IOException {
            writeInt(os, EXTSIG); // EXT header signature
            writeInt(os, crc); // crc-32
            if (csize >= ZIP64_MINVAL || size >= ZIP64_MINVAL) {
                writeLong(os, csize);
                writeLong(os, size);
                return 24;
            } else {
                writeInt(os, csize); // compressed size
                writeInt(os, size); // uncompressed size
                return 16;
            }
        }
        
        int writeLOC(OutputStream os) throws IOException {
            writeInt(os, LOCSIG); // LOC header signature
            version();
            
            byte[] zname = isdir ? toDirectoryPath(name) : name;
            int nlen = zname != null ? zname.length - 1 : 0; // [0] is slash
            int elen = extra != null ? extra.length : 0;
            boolean foundExtraTime = false; // if extra timestamp present
            int eoff = 0;
            int elen64 = 0;
            int elenEXTT = 0;
            int elenNTFS = 0;
            if ((flag & FLAG_DATADESCR) != 0) {
                writeShort(os, version()); // version needed to extract
                writeShort(os, flag); // general purpose bit flag
                writeShort(os, method); // compression method
                // last modification time
                writeInt(os, (int) javaToDosTime(mtime));
                // store size, uncompressed size, and crc-32 in data descriptor
                // immediately following compressed entry data
                writeInt(os, 0);
                writeInt(os, 0);
                writeInt(os, 0);
            } else {
                if (csize >= ZIP64_MINVAL || size >= ZIP64_MINVAL) {
                    elen64 = 20; // headid(2) + size(2) + size(8) + csize(8)
                    writeShort(os, 45); // ver 4.5 for zip64
                } else {
                    writeShort(os, version()); // version needed to extract
                }
                writeShort(os, flag); // general purpose bit flag
                writeShort(os, method); // compression method
                                        // last modification time
                writeInt(os, (int) javaToDosTime(mtime));
                writeInt(os, crc); // crc-32
                if (elen64 != 0) {
                    writeInt(os, ZIP64_MINVAL);
                    writeInt(os, ZIP64_MINVAL);
                } else {
                    writeInt(os, csize); // compressed size
                    writeInt(os, size); // uncompressed size
                }
            }
            while (eoff + 4 < elen) {
                int tag = SH(extra, eoff);
                int sz = SH(extra, eoff + 2);
                if (tag == EXTID_EXTT || tag == EXTID_NTFS) {
                    foundExtraTime = true;
                }
                eoff += 4 + sz;
            }
            if (!foundExtraTime) {
                if (isWindows) {
                    elenNTFS = 36; // NTFS, total 36 bytes
                } else { // on unix use "ext time"
                    elenEXTT = 9;
                    if (atime != -1) {
                        elenEXTT += 4;
                    }
                    if (ctime != -1) {
                        elenEXTT += 4;
                    }
                }
            }
            writeShort(os, nlen);
            writeShort(os, elen + elen64 + elenNTFS + elenEXTT);
            writeBytes(os, zname, 1, nlen);
            if (elen64 != 0) {
                writeShort(os, EXTID_ZIP64);
                writeShort(os, 16);
                writeLong(os, size);
                writeLong(os, csize);
            }
            if (elenNTFS != 0) {
                writeShort(os, EXTID_NTFS);
                writeShort(os, elenNTFS - 4);
                writeInt(os, 0); // reserved
                writeShort(os, 0x0001); // NTFS attr tag
                writeShort(os, 24);
                writeLong(os, javaToWinTime(mtime));
                writeLong(os, javaToWinTime(atime));
                writeLong(os, javaToWinTime(ctime));
            }
            if (elenEXTT != 0) {
                writeShort(os, EXTID_EXTT);
                writeShort(os, elenEXTT - 4);// size for the folowing data block
                int fbyte = 0x1;
                if (atime != -1) {
                    fbyte |= 0x2;
                }
                if (ctime != -1) {
                    fbyte |= 0x4;
                }
                os.write(fbyte); // flags byte
                writeInt(os, javaToUnixTime(mtime));
                if (atime != -1) {
                    writeInt(os, javaToUnixTime(atime));
                }
                if (ctime != -1) {
                    writeInt(os, javaToUnixTime(ctime));
                }
            }
            if (extra != null) {
                writeBytes(os, extra);
            }
            return LOCHDR + nlen + elen + elen64 + elenNTFS + elenEXTT;
        }
    }
    
    // Inner class implementing the input stream used to read
    // a (possibly compressed) zip file entry.
    private class EntryInputStream extends InputStream {
        // point to a new channel after sync()
        private long pos; // current position within entry data
        protected long rem; // number of remaining bytes within entry
        private final SeekableByteChannel zfch; // local ref to zipfs's "ch". zipfs.ch might
        
        EntryInputStream(Entry e, SeekableByteChannel zfch) throws IOException {
            this.zfch = zfch;
            rem = e.csize;
            pos = e.locoff;
            if (pos == -1) {
                Entry e2 = getEntry(e.name);
                if (e2 == null) {
                    throw new ZipException("invalid loc for entry <" + e.name + ">");
                }
                pos = e2.locoff;
            }
            pos = -pos; // lazy initialize the real data offset
        }
        
        @Override
        public int available() {
            return rem > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) rem;
        }
        
        @Override
        public void close() {
            rem = 0;
            streams.remove(this);
        }
        
        private void initDataPos() throws IOException {
            if (pos <= 0) {
                pos = -pos + locpos;
                byte[] buf = new byte[LOCHDR];
                if (readFullyAt(buf, 0, buf.length, pos) != LOCHDR) {
                    throw new ZipException("invalid loc " + pos + " for entry reading");
                }
                pos += LOCHDR + LOCNAM(buf) + LOCEXT(buf);
            }
        }
        
        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            if (read(b, 0, 1) == 1) {
                return b[0] & 0xff;
            } else {
                return -1;
            }
        }
        
        @Override
        public int read(byte b[], int off, int len) throws IOException {
            ensureOpen();
            initDataPos();
            if (rem == 0) {
                return -1;
            }
            if (len <= 0) {
                return 0;
            }
            if (len > rem) {
                len = (int) rem;
            }
            // readFullyAt()
            long n = 0;
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.position(off);
            bb.limit(off + len);
            synchronized (zfch) {
                n = zfch.position(pos).read(bb);
            }
            if (n > 0) {
                pos += n;
                rem -= n;
            }
            if (rem == 0) {
                close();
            }
            return (int) n;
        }
        
        @Override
        public long skip(long n) throws IOException {
            ensureOpen();
            if (n > rem) {
                n = rem;
            }
            pos += n;
            rem -= n;
            if (rem == 0) {
                close();
            }
            return n;
        }
    }
    // Returns an output SeekableByteChannel for either
    // (1) writing the contents of a new entry, if the entry doesn't exit, or
    // (2) updating/replacing the contents of an existing entry.
    // Note: The content is not compressed.
    private class EntryOutputStream extends DeflaterOutputStream {
        private CRC32 crc;
        private Entry e;
        private boolean isClosed = false;
        private long written;
        
        EntryOutputStream(Entry e, OutputStream os) throws IOException {
            super(os, getDeflater());
            if (e == null) {
                throw new NullPointerException("Zip entry is null");
            }
            this.e = e;
            crc = new CRC32();
        }
        
        @Override
        public synchronized void close() throws IOException {
            if (isClosed) {
                return;
            }
            isClosed = true;
            // TBD ensureOpen();
            switch (e.method) {
                case METHOD_DEFLATED:
                    finish();
                    e.size = def.getBytesRead();
                    e.csize = def.getBytesWritten();
                    e.crc = crc.getValue();
                    break;
                case METHOD_STORED:
                    // we already know that both e.size and e.csize are the same
                    e.size = e.csize = written;
                    e.crc = crc.getValue();
                    break;
                default:
                    throw new ZipException("invalid compression method");
            }
            // crc.reset();
            if (out instanceof ByteArrayOutputStream) {
                e.bytes = ((ByteArrayOutputStream) out).toByteArray();
            }
            
            if (e.type == Entry.FILECH) {
                releaseDeflater(def);
                return;
            }
            super.close();
            releaseDeflater(def);
            update(e);
        }
        
        @Override
        public synchronized void write(byte b[], int off, int len) throws IOException {
            if (e.type != Entry.FILECH) {
                ensureOpen();
            }
            if (isClosed) {
                throw new IOException("Stream closed");
            }
            if (off < 0 || len < 0 || off > b.length - len) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }
            switch (e.method) {
                case METHOD_DEFLATED:
                    super.write(b, off, len);
                    break;
                case METHOD_STORED:
                    written += len;
                    out.write(b, off, len);
                    break;
                default:
                    throw new ZipException("invalid compression method");
            }
            crc.update(b, off, len);
        }
    }
    
    private static class ExChannelCloser {
        SeekableByteChannel ch;
        Path path;
        Set<InputStream> streams;
        
        ExChannelCloser(Path path, SeekableByteChannel ch, Set<InputStream> streams) {
            this.path = path;
            this.ch = ch;
            this.streams = streams;
        }
    }
    
    // Internal node that links a "name" to its pos in cen table.
    // The node itself can be used as a "key" to lookup itself in
    // the HashMap inodes.
    static class IndexNode {
        private static final ThreadLocal<IndexNode> cachedKey = new ThreadLocal<>();
        
        final static IndexNode keyOf(byte[] name) { // get a lookup key;
            IndexNode key = cachedKey.get();
            if (key == null) {
                key = new IndexNode(name, -1);
                cachedKey.set(key);
            }
            return key.as(name);
        }
        
        IndexNode child; // 1st child
        int hashcode; // node is hashable/hashed by its name
        
        // entry does not exists in zip file
        boolean isdir;
        
        byte[] name;
        
        int pos = -1; // position in cen table, -1 menas the
        
        IndexNode sibling;
        
        IndexNode() {
        }
        
        IndexNode(byte[] name, boolean isdir) {
            name(name);
            this.isdir = isdir;
            pos = -1;
        }
        
        IndexNode(byte[] name, int pos) {
            name(name);
            this.pos = pos;
        }
        
        // constructor for cenInit()
        IndexNode(byte[] cen, int noff, int nlen, int pos) {
            if (cen[noff + nlen - 1] == '/') {
                isdir = true;
                nlen--;
            }
            name = new byte[nlen + 1];
            System.arraycopy(cen, pos + CENHDR, name, 1, nlen);
            name[0] = '/';
            name(name);
            this.pos = pos;
        }
        
        final IndexNode as(byte[] name) { // reuse the node, mostly
            name(name); // as a lookup "key"
            return this;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof IndexNode)) {
                return false;
            }
            if (other instanceof ParentLookup) {
                return ((ParentLookup) other).equals(this);
            }
            return Arrays.equals(name, ((IndexNode) other).name);
        }
        
        @Override
        public int hashCode() {
            return hashcode;
        }
        
        boolean isDir() {
            return isdir;
        }
        
        final void name(byte[] name) {
            this.name = name;
            hashcode = Arrays.hashCode(name);
        }
    }
    
    // purely for parent lookup, so we don't have to copy the parent
    // name every time
    static class ParentLookup extends IndexNode {
        ParentLookup() {
        }
        
        final ParentLookup as(byte[] name, int len) { // as a lookup "key"
            name(name, len);
            return this;
        }
        
        @Override
        public boolean equals(Object other) {
            if (!(other instanceof IndexNode)) {
                return false;
            }
            byte[] oname = ((IndexNode) other).name;
            return Arrays.equals(name, oname);
        }
        
        void name(byte[] name, int len) {
            this.name = Arrays.copyOfRange(name, 0, len);
            hashcode = Arrays.hashCode(this.name);
        }
        
    }
    
    private static final String GLOB_SYNTAX = "glob";
    
    private static final boolean isWindows = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> System.getProperty("os.name").startsWith("Windows"));
    
    private static final String REGEX_SYNTAX = "regex";
    
    private static byte[] ROOTPATH = new byte[] { '/' };
    
    private static final Set<String> supportedFileAttributeViews = new HashSet<>(Arrays.asList("basic", "zip"));
    
    private static void copyStream(InputStream is, OutputStream os) throws IOException {
        byte[] copyBuf = new byte[8192];
        int n;
        while ((n = is.read(copyBuf)) != -1) {
            os.write(copyBuf, 0, n);
        }
    }
    
    private static byte[] getParent(byte[] path) {
        int off = getParentOff(path);
        if (off <= 1) {
            return ROOTPATH;
        }
        return Arrays.copyOf(path, off);
    }
    
    private static int getParentOff(byte[] path) {
        int off = path.length - 1;
        if (off > 0 && path[off] == '/') {
            off--;
        }
        while (off > 0 && path[off] != '/') {
            off--;
        }
        return off;
    }
    
    static void zerror(String msg) throws ZipException {
        throw new ZipException(msg);
    }
    
    final byte[] cen; // CEN & ENDHDR
    
    private final SeekableByteChannel ch; // channel to the zipfile
    
    // List of available Deflater objects for compression
    private final List<Deflater> deflaters = new ArrayList<>();
    
    private END end;
    
    // the ex-channel and ex-path that need to close when their outstanding
    // input streams are all closed by the obtainers.
    private Set<ExChannelCloser> exChClosers = new HashSet<>();
    
    private boolean hasUpdate = false;
    
    // List of available Inflater objects for decompression
    private final List<Inflater> inflaters = new ArrayList<>();
    
    // name -> pos (in cen), IndexNode itself can be used as a "key"
    private LinkedHashMap<IndexNode, IndexNode> inodes;
    
    private volatile boolean isOpen = true;
    
    private long locpos; // position of first LOC header (usually 0)
    
    // shared key. consumer guarantees the "writeLock" before use it.
    private final IndexNode LOOKUPKEY = new IndexNode(null, -1);
    // Maxmum number of de/inflater we cache
    private final int MAX_FLATER = 20;
    
    private final boolean noExtt; // see readExtra()
    
    private final SpringBootFileSystemProvider provider;
    
    // is to use BAOS for better performance
    private boolean readOnly = false; // readonly file system
    
    private final ZipPath rootdir;
    
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    
    // the outstanding input streams that need to be closed
    private Set<InputStream> streams = Collections.synchronizedSet(new HashSet<InputStream>());
    
    private Set<Path> tmppaths = Collections.synchronizedSet(new HashSet<Path>());
    
    // configurable by env map
    private final boolean useTempFile; // use a temp file for newOS, default
    
    final ZipCoder zc;
    
    private final Path zfpath;
    
    // default time stamp for pseudo entries
    private long zfsDefaultTimeStamp = System.currentTimeMillis();

    
    SpringBootFileSystem(SpringBootFileSystemProvider provider, Path zfpath, Map<String, ?> env) throws IOException {
        // create a new zip if not exists
        boolean createNew = env.get("create") == null || "true".equals(env.get("create"));
        // default encoding for name/comment
        String nameEncoding = env.containsKey("encoding") ? (String) env.get("encoding") : "UTF-8";
        noExtt = "false".equals(env.get("zipinfo-time"));
        useTempFile = TRUE.equals(env.get("useTempFile"));
        this.provider = provider;
        this.zfpath = zfpath;
        if (Files.notExists(zfpath)) {
            Files.createDirectories(zfpath.getParent());
            if (createNew) {
                try (OutputStream os = Files.newOutputStream(zfpath, CREATE_NEW, WRITE)) {
                    new END().write(os, 0);
                }
            } else {
                throw new FileSystemNotFoundException(zfpath.toString());
            }
        }
        // sm and existence check
        zfpath.getFileSystem().provider().checkAccess(zfpath, AccessMode.READ);
        boolean writeable = AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> Files.isWritable(zfpath));
        readOnly = !writeable;
        zc = ZipCoder.get(nameEncoding);
        rootdir = new ZipPath(this, new byte[] { '/' });
        ch = new BootSeekableByteChannel(Files.newByteChannel(zfpath, READ));
        try {
            cen = initCEN();
        } catch (IOException x) {
            try {
                ch.close();
            } catch (IOException xx) {
                x.addSuppressed(xx);
            }
            throw x;
        }
    }
    
    private final void beginRead() {
        rwlock.readLock().lock();
    }
    
    private final void beginWrite() {
        rwlock.writeLock().lock();
    }
    
    private void buildNodeTree() throws IOException {
        beginWrite();
        try {
            IndexNode root = new IndexNode(ROOTPATH, true);
            IndexNode[] nodes = inodes.keySet().toArray(new IndexNode[0]);
            inodes.put(root, root);
            ParentLookup lookup = new ParentLookup();
            for (IndexNode node : nodes) {
                IndexNode parent;
                while (true) {
                    int off = getParentOff(node.name);
                    if (off <= 1) { // parent is root
                        node.sibling = root.child;
                        root.child = node;
                        break;
                    }
                    lookup = lookup.as(node.name, off);
                    if (inodes.containsKey(lookup)) {
                        parent = inodes.get(lookup);
                        node.sibling = parent.child;
                        parent.child = node;
                        break;
                    }
                    // add new pseudo directory entry
                    parent = new IndexNode(Arrays.copyOf(node.name, off), true);
                    inodes.put(parent, parent);
                    node.sibling = parent.child;
                    parent.child = node;
                    node = parent;
                }
            }
        } finally {
            endWrite();
        }
    }
    
    void checkAccess(byte[] path) throws IOException {
        beginRead();
        try {
            ensureOpen();
            // is it necessary to readCEN as a sanity check?
            if (getInode(path) == null) {
                throw new NoSuchFileException(toString());
            }
            
        } finally {
            endRead();
        }
    }
    
    private void checkOptions(Set<? extends OpenOption> options) {
        // check for options of null type and option is an intance of StandardOpenOption
        for (OpenOption option : options) {
            if (option == null) {
                throw new NullPointerException();
            }
            if (!(option instanceof StandardOpenOption)) {
                throw new IllegalArgumentException();
            }
        }
        if (options.contains(APPEND) && options.contains(TRUNCATE_EXISTING)) {
            throw new IllegalArgumentException("APPEND + TRUNCATE_EXISTING not allowed");
        }
    }
    
    // check if all parents really exit. ZIP spec does not require
    // the existence of any "parent directory".
    private void checkParents(byte[] path) throws IOException {
        beginRead();
        try {
            while ((path = getParent(path)) != null && path != ROOTPATH) {
                if (!inodes.containsKey(IndexNode.keyOf(path))) {
                    throw new NoSuchFileException(getString(path));
                }
            }
        } finally {
            endRead();
        }
    }
    
    private void checkWritable() throws IOException {
        if (readOnly) {
            throw new ReadOnlyFileSystemException();
        }
    }
    
    @Override
    public void close() throws IOException {
        beginWrite();
        try {
            if (!isOpen) {
                return;
            }
            isOpen = false; // set closed
        } finally {
            endWrite();
        }
        if (!streams.isEmpty()) { // unlock and close all remaining streams
            Set<InputStream> copy = new HashSet<>(streams);
            for (InputStream is : copy) {
                is.close();
            }
        }
        beginWrite(); // lock and sync
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                sync();
                return null;
            });
            ch.close(); // close the ch just in case no update
        } catch (PrivilegedActionException e) { // and sync dose not close the ch
            throw (IOException) e.getException();
        } finally {
            endWrite();
        }
        
        synchronized (inflaters) {
            for (Inflater inf : inflaters) {
                inf.end();
            }
        }
        synchronized (deflaters) {
            for (Deflater def : deflaters) {
                def.end();
            }
        }
        
        IOException ioe = null;
        synchronized (tmppaths) {
            for (Path p : tmppaths) {
                try {
                    AccessController.doPrivileged((PrivilegedExceptionAction<Boolean>) () -> Files.deleteIfExists(p));
                } catch (PrivilegedActionException e) {
                    IOException x = (IOException) e.getException();
                    if (ioe == null) {
                        ioe = x;
                    } else {
                        ioe.addSuppressed(x);
                    }
                }
            }
        }
        provider.removeFileSystem(zfpath, this);
        if (ioe != null) {
            throw ioe;
        }
    }
    
    void copyFile(boolean deletesrc, byte[] src, byte[] dst, CopyOption... options) throws IOException {
        checkWritable();
        if (Arrays.equals(src, dst)) {
            return; // do nothing, src and dst are the same
        }
        
        beginWrite();
        try {
            ensureOpen();
            Entry eSrc = getEntry(src); // ensureOpen checked
            
            if (eSrc == null) {
                throw new NoSuchFileException(getString(src));
            }
            if (eSrc.isDir()) { // spec says to create dst dir
                createDirectory(dst);
                return;
            }
            boolean hasReplace = false;
            boolean hasCopyAttrs = false;
            for (CopyOption opt : options) {
                if (opt == REPLACE_EXISTING) {
                    hasReplace = true;
                } else if (opt == COPY_ATTRIBUTES) {
                    hasCopyAttrs = true;
                }
            }
            Entry eDst = getEntry(dst);
            if (eDst != null) {
                if (!hasReplace) {
                    throw new FileAlreadyExistsException(getString(dst));
                }
            } else {
                checkParents(dst);
            }
            Entry u = new Entry(eSrc, Entry.COPY); // copy eSrc entry
            u.name(dst); // change name
            if (eSrc.type == Entry.NEW || eSrc.type == Entry.FILECH) {
                u.type = eSrc.type; // make it the same type
                if (deletesrc) { // if it's a "rename", take the data
                    u.bytes = eSrc.bytes;
                    u.file = eSrc.file;
                } else { // if it's not "rename", copy the data
                    if (eSrc.bytes != null) {
                        u.bytes = Arrays.copyOf(eSrc.bytes, eSrc.bytes.length);
                    } else if (eSrc.file != null) {
                        u.file = getTempPathForEntry(null);
                        Files.copy(eSrc.file, u.file, REPLACE_EXISTING);
                    }
                }
            }
            if (!hasCopyAttrs) {
                u.mtime = u.atime = u.ctime = System.currentTimeMillis();
            }
            update(u);
            if (deletesrc) {
                updateDelete(eSrc);
            }
        } finally {
            endWrite();
        }
    }
    
    // copy over the whole LOC entry (header if necessary, data and ext) from
    // old zip to the new one.
    private long copyLOCEntry(Entry e, boolean updateHeader, OutputStream os, long written, byte[] buf) throws IOException {
        long locoff = e.locoff; // where to read
        e.locoff = written; // update the e.locoff with new value
        
        // calculate the size need to write out
        long size = 0;
        // if there is A ext
        if ((e.flag & FLAG_DATADESCR) != 0) {
            if (e.size >= ZIP64_MINVAL || e.csize >= ZIP64_MINVAL) {
                size = 24;
            } else {
                size = 16;
            }
        }
        // read loc, use the original loc.elen/nlen
        if (readFullyAt(buf, 0, LOCHDR, locoff) != LOCHDR) {
            throw new ZipException("loc: reading failed");
        }
        if (updateHeader) {
            locoff += LOCHDR + LOCNAM(buf) + LOCEXT(buf); // skip header
            size += e.csize;
            written = e.writeLOC(os) + size;
        } else {
            os.write(buf, 0, LOCHDR); // write out the loc header
            locoff += LOCHDR;
            // use e.csize, LOCSIZ(buf) is zero if FLAG_DATADESCR is on
            // size += LOCNAM(buf) + LOCEXT(buf) + LOCSIZ(buf);
            size += LOCNAM(buf) + LOCEXT(buf) + e.csize;
            written = LOCHDR + size;
        }
        int n;
        while (size > 0 && (n = (int) readFullyAt(buf, 0, buf.length, locoff)) != -1) {
            if (size < n) {
                n = (int) size;
            }
            os.write(buf, 0, n);
            size -= n;
            locoff += n;
        }
        return written;
    }
    
    void createDirectory(byte[] dir, FileAttribute<?>... attrs) throws IOException {
        checkWritable();
        // dir = toDirectoryPath(dir);
        beginWrite();
        try {
            ensureOpen();
            if (dir.length == 0 || exists(dir)) {
                throw new FileAlreadyExistsException(getString(dir));
            }
            checkParents(dir);
            Entry e = new Entry(dir, Entry.NEW, true);
            e.method = METHOD_STORED; // STORED for dir
            update(e);
        } finally {
            endWrite();
        }
    }
    
    // Creates a new empty temporary file in the same directory as the
    // specified file. A variant of Files.createTempFile.
    private Path createTempFileInSameDirectoryAs(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        Path dir = parent == null ? path.getFileSystem().getPath(".") : parent;
        Path tmpPath = Files.createTempFile(dir, "zipfstmp", null);
        tmppaths.add(tmpPath);
        return tmpPath;
    }
    
    public void deleteFile(byte[] path, boolean failIfNotExists) throws IOException {
        checkWritable();
        
        IndexNode inode = getInode(path);
        if (inode == null) {
            if (path != null && path.length == 0) {
                throw new ZipException("root directory </> can't not be delete");
            }
            if (failIfNotExists) {
                throw new NoSuchFileException(getString(path));
            }
        } else {
            if (inode.isDir() && inode.child != null) {
                throw new DirectoryNotEmptyException(getString(path));
            }
            updateDelete(inode);
        }
    }
    
    private final void endRead() {
        rwlock.readLock().unlock();
    }
    
    private final void endWrite() {
        rwlock.writeLock().unlock();
    }
    
    private void ensureOpen() throws IOException {
        if (!isOpen) {
            throw new ClosedFileSystemException();
        }
    }
    
    ///////////////////////////////////////////////////////////////////
    
    boolean exists(byte[] path) throws IOException {
        beginRead();
        try {
            ensureOpen();
            return getInode(path) != null;
        } finally {
            endRead();
        }
    }
    
    @Override
    protected void finalize() throws IOException {
        close();
    }
    
    // Searches for end of central directory (END) header. The contents of
    // the END header will be read and placed in endbuf. Returns the file
    // position of the END header, otherwise returns -1 if the END header
    // was not found or an error occurred.
    private END findEND() throws IOException {
        byte[] buf = new byte[READBLOCKSZ];
        long ziplen = ch.size();
        long minHDR = ziplen - END_MAXLEN > 0 ? ziplen - END_MAXLEN : 0;
        long minPos = minHDR - (buf.length - ENDHDR);
        
        for (long pos = ziplen - buf.length; pos >= minPos; pos -= buf.length - ENDHDR) {
            int off = 0;
            if (pos < 0) {
                // Pretend there are some NUL bytes before start of file
                off = (int) -pos;
                Arrays.fill(buf, 0, off, (byte) 0);
            }
            int len = buf.length - off;
            if (readFullyAt(buf, off, len, pos + off) != len) {
                zerror("zip END header not found");
            }
            
            // Now scan the block backwards for END header signature
            for (int i = buf.length - ENDHDR; i >= 0; i--) {
                if (buf[i + 0] == (byte) 'P' && buf[i + 1] == (byte) 'K' && buf[i + 2] == (byte) '\005' && buf[i + 3] == (byte) '\006' && pos + i + ENDHDR + ENDCOM(buf, i) == ziplen) {
                    // Found END header
                    buf = Arrays.copyOfRange(buf, i, i + ENDHDR);
                    END end = new END();
                    end.endsub = ENDSUB(buf);
                    end.centot = ENDTOT(buf);
                    end.cenlen = ENDSIZ(buf);
                    end.cenoff = ENDOFF(buf);
                    end.comlen = ENDCOM(buf);
                    end.endpos = pos + i;
                    if (end.cenlen == ZIP64_MINVAL || end.cenoff == ZIP64_MINVAL || end.centot == ZIP64_MINVAL32) {
                        // need to find the zip64 end;
                        byte[] loc64 = new byte[ZIP64_LOCHDR];
                        if (readFullyAt(loc64, 0, loc64.length, end.endpos - ZIP64_LOCHDR) != loc64.length) {
                            return end;
                        }
                        long end64pos = ZIP64_LOCOFF(loc64);
                        byte[] end64buf = new byte[ZIP64_ENDHDR];
                        if (readFullyAt(end64buf, 0, end64buf.length, end64pos) != end64buf.length) {
                            return end;
                        }
                        // end64 found, re-calcualte everything.
                        end.cenlen = ZIP64_ENDSIZ(end64buf);
                        end.cenoff = ZIP64_ENDOFF(end64buf);
                        end.centot = (int) ZIP64_ENDTOT(end64buf); // assume total < 2g
                        end.endpos = end64pos;
                    }
                    return end;
                }
            }
        }
        zerror("zip END header not found");
        return null; // make compiler happy
    }
    
    final byte[] getBytes(String name) {
        return zc.getBytes(name);
    }
    
    // Gets an deflater from the list of available deflaters or allocates
    // a new one.
    private Deflater getDeflater() {
        synchronized (deflaters) {
            int size = deflaters.size();
            if (size > 0) {
                Deflater def = deflaters.remove(size - 1);
                return def;
            } else {
                return new Deflater(Deflater.NO_COMPRESSION, true);
            }
        }
    }
    
    Entry getEntry(byte[] path) throws IOException {
        IndexNode inode = getInode(path);
        if (inode instanceof Entry) {
            return (Entry) inode;
        }
        if (inode == null || inode.pos == -1) {
            return null;
        }
        return Entry.readCEN(this, inode);
    }
    
    ZipFileAttributes getFileAttributes(byte[] path) throws IOException {
        Entry e;
        beginRead();
        try {
            ensureOpen();
            e = getEntry(path);
            if (e == null) {
                IndexNode inode = getInode(path);
                if (inode == null) {
                    return null;
                }
                e = new Entry(inode.name, inode.isdir); // pseudo directory
                e.method = METHOD_STORED; // STORED for dir
                e.mtime = e.atime = e.ctime = zfsDefaultTimeStamp;
            }
        } finally {
            endRead();
        }
        return e;
    }
    
    FileStore getFileStore(ZipPath path) {
        return new ZipFileStore(path);
    }
    
    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.singletonList(new ZipFileStore(rootdir));
    }
    
    // Gets an inflater from the list of available inflaters or allocates
    // a new one.
    private Inflater getInflater() {
        synchronized (inflaters) {
            int size = inflaters.size();
            if (size > 0) {
                Inflater inf = inflaters.remove(size - 1);
                return inf;
            } else {
                return new Inflater(true);
            }
        }
    }
    
    IndexNode getInode(byte[] path) {
        if (path == null) {
            throw new NullPointerException("path");
        }
        return inodes.get(IndexNode.keyOf(path));
    }
    
    private InputStream getInputStream(Entry e) throws IOException {
        InputStream eis = null;
        
        if (e.type == Entry.NEW) {
            if (e.bytes != null) {
                eis = new ByteArrayInputStream(e.bytes);
            } else if (e.file != null) {
                eis = Files.newInputStream(e.file);
            } else {
                throw new ZipException("update entry data is missing");
            }
        } else if (e.type == Entry.FILECH) {
            // FILECH result is un-compressed.
            eis = Files.newInputStream(e.file);
            // TBD: wrap to hook close()
            // streams.add(eis);
            return eis;
        } else { // untouced CEN or COPY
            eis = new EntryInputStream(e, ch);
        }
        if (e.method == METHOD_DEFLATED) {
            // MORE: Compute good size for inflater stream:
            long bufSize = e.size + 2; // Inflater likes a bit of slack
            if (bufSize > 65536) {
                bufSize = 8192;
            }
            final long size = e.size;
            eis = new InflaterInputStream(eis, getInflater(), (int) bufSize) {
                private boolean eof;
                private boolean isClosed = false;
                
                @Override
                public int available() throws IOException {
                    if (isClosed) {
                        return 0;
                    }
                    long avail = size - inf.getBytesWritten();
                    return avail > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) avail;
                }
                
                @Override
                public void close() throws IOException {
                    if (!isClosed) {
                        releaseInflater(inf);
                        in.close();
                        isClosed = true;
                        streams.remove(this);
                    }
                }
                
                // Override fill() method to provide an extra "dummy" byte
                // at the end of the input stream. This is required when
                // using the "nowrap" Inflater option. (it appears the new
                // zlib in 7 does not need it, but keep it for now)
                @Override
                protected void fill() throws IOException {
                    if (eof) {
                        throw new EOFException("Unexpected end of ZLIB input stream");
                    }
                    len = in.read(buf, 0, buf.length);
                    if (len == -1) {
                        buf[0] = 0;
                        len = 1;
                        eof = true;
                    }
                    inf.setInput(buf, 0, len);
                }
            };
        } else if (e.method == METHOD_STORED) {
            // TBD: wrap/ it does not seem necessary
        } else {
            throw new ZipException("invalid compression method");
        }
        streams.add(eis);
        return eis;
    }
    
    // Returns an out stream for either
    // (1) writing the contents of a new entry, if the entry exits, or
    // (2) updating/replacing the contents of the specified existing entry.
    private OutputStream getOutputStream(Entry e) throws IOException {
        
        if (e.mtime == -1) {
            e.mtime = System.currentTimeMillis();
        }
        if (e.method == -1) {
            e.method = METHOD_STORED; // TBD: use default method
        }
        // store size, compressed size, and crc-32 in LOC header
        e.flag = 0;
        if (zc.isUTF8()) {
            e.flag |= FLAG_EFS;
        }
        OutputStream os;
        if (useTempFile) {
            e.file = getTempPathForEntry(null);
            os = Files.newOutputStream(e.file, WRITE);
        } else {
            os = new ByteArrayOutputStream(e.size > 0 ? (int) e.size : 8192);
        }
        return new EntryOutputStream(e, os);
    }
    
    @Override
    public ZipPath getPath(String first, String... more) {
        if (more.length == 0) {
            return new ZipPath(this, first);
        }
        StringBuilder sb = new StringBuilder();
        sb.append(first);
        for (String path : more) {
            if (path.length() > 0) {
                if (sb.length() > 0) {
                    sb.append('/');
                }
                sb.append(path);
            }
        }
        return new ZipPath(this, sb.toString());
    }
    
    @Override
    public PathMatcher getPathMatcher(String syntaxAndInput) {
        int pos = syntaxAndInput.indexOf(':');
        if (pos <= 0 || pos == syntaxAndInput.length()) {
            throw new IllegalArgumentException();
        }
        String syntax = syntaxAndInput.substring(0, pos);
        String input = syntaxAndInput.substring(pos + 1);
        String expr;
        if (syntax.equalsIgnoreCase(GLOB_SYNTAX)) {
            expr = toRegexPattern(input);
        } else {
            if (syntax.equalsIgnoreCase(REGEX_SYNTAX)) {
                expr = input;
            } else {
                throw new UnsupportedOperationException("Syntax '" + syntax + "' not recognized");
            }
        }
        // return matcher
        final Pattern pattern = Pattern.compile(expr);
        return new PathMatcher() {
            @Override
            public boolean matches(Path path) {
                return pattern.matcher(path.toString()).matches();
            }
        };
    }
    
    ZipPath getRootDir() {
        return rootdir;
    }
    
    //////////////////// update & sync //////////////////////////////////////
    
    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList(rootdir);
    }
    
    @Override
    public String getSeparator() {
        return "/";
    }
    
    final String getString(byte[] name) {
        return zc.toString(name);
    }
    
    private Path getTempPathForEntry(byte[] path) throws IOException {
        Path tmpPath = createTempFileInSameDirectoryAs(zfpath);
        if (path != null) {
            Entry e = getEntry(path);
            if (e != null) {
                try (InputStream is = newInputStream(path)) {
                    Files.copy(is, tmpPath, REPLACE_EXISTING);
                }
            }
        }
        return tmpPath;
    }
    
    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }
    
    Path getZipFile() {
        return zfpath;
    }
    
    // Reads zip file central directory. Returns the file position of first
    // CEN header, otherwise returns -1 if an error occurred. If zip->msg != NULL
    // then the error was a zip format error and zip->msg has the error text.
    // Always pass in -1 for knownTotal; it's used for a recursive call.
    private byte[] initCEN() throws IOException {
        end = findEND();
        if (end.endpos == 0) {
            inodes = new LinkedHashMap<>(10);
            locpos = 0;
            buildNodeTree();
            return null; // only END header present
        }
        if (end.cenlen > end.endpos) {
            zerror("invalid END header (bad central directory size)");
        }
        long cenpos = end.endpos - end.cenlen; // position of CEN table
        
        // Get position of first local file (LOC) header, taking into
        // account that there may be a stub prefixed to the zip file.
        locpos = cenpos - end.cenoff;
        if (locpos < 0) {
            zerror("invalid END header (bad central directory offset)");
        }
        
        // read in the CEN and END
        byte[] cen = new byte[(int) (end.cenlen + ENDHDR)];
        if (readFullyAt(cen, 0, cen.length, cenpos) != end.cenlen + ENDHDR) {
            zerror("read CEN tables failed");
        }
        // Iterate through the entries in the central directory
        inodes = new LinkedHashMap<>(end.centot + 1);
        int pos = 0;
        int limit = cen.length - ENDHDR;
        while (pos < limit) {
            if (!cenSigAt(cen, pos)) {
                zerror("invalid CEN header (bad signature)");
            }
            int method = CENHOW(cen, pos);
            int nlen = CENNAM(cen, pos);
            int elen = CENEXT(cen, pos);
            int clen = CENCOM(cen, pos);
            if ((CENFLG(cen, pos) & 1) != 0) {
                zerror("invalid CEN header (encrypted entry)");
            }
            if (method != METHOD_STORED && method != METHOD_DEFLATED) {
                zerror("invalid CEN header (unsupported compression method: " + method + ")");
            }
            if (pos + CENHDR + nlen > limit) {
                zerror("invalid CEN header (bad header size)");
            }
            IndexNode inode = new IndexNode(cen, pos + CENHDR, nlen, pos);
            inodes.put(inode, inode);
            
            // skip ext and comment
            pos += CENHDR + nlen + elen + clen;
        }
        if (pos + ENDHDR != cen.length) {
            zerror("invalid CEN header (bad header size)");
        }
        buildNodeTree();
        return cen;
    }
    
    boolean isDirectory(byte[] path) throws IOException {
        beginRead();
        try {
            IndexNode n = getInode(path);
            return n != null && n.isDir();
        } finally {
            endRead();
        }
    }
    
    @Override
    public boolean isOpen() {
        return isOpen;
    }
    
    @Override
    public boolean isReadOnly() {
        return readOnly;
    }
    
    // returns the list of child paths of "path"
    Iterator<Path> iteratorOf(byte[] path, DirectoryStream.Filter<? super Path> filter) throws IOException {
        beginWrite(); // iteration of inodes needs exclusive lock
        try {
            ensureOpen();
            IndexNode inode = getInode(path);
            if (inode == null) {
                throw new NotDirectoryException(getString(path));
            }
            List<Path> list = new ArrayList<>();
            IndexNode child = inode.child;
            while (child != null) {
                // assume all path from zip file itself is "normalized"
                ZipPath zp = new ZipPath(this, child.name, true);
                if (filter == null || filter.accept(zp)) {
                    list.add(zp);
                }
                child = child.sibling;
            }
            return list.iterator();
        } finally {
            endWrite();
        }
    }
    
    // Returns a Writable/ReadByteChannel for now. Might consdier to use
    // newFileChannel() instead, which dump the entry data into a regular
    // file on the default file system and create a FileChannel on top of
    // it.
    SeekableByteChannel newByteChannel(byte[] path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        checkOptions(options);
        if (options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.APPEND)) {
            checkWritable();
            beginRead();
            try {
                final WritableByteChannel wbc = Channels.newChannel(newOutputStream(path, options.toArray(new OpenOption[0])));
                long leftover = 0;
                if (options.contains(StandardOpenOption.APPEND)) {
                    Entry e = getEntry(path);
                    if (e != null && e.size >= 0) {
                        leftover = e.size;
                    }
                }
                final long offset = leftover;
                return new SeekableByteChannel() {
                    long written = offset;
                    
                    @Override
                    public void close() throws IOException {
                        wbc.close();
                    }
                    
                    @Override
                    public boolean isOpen() {
                        return wbc.isOpen();
                    }
                    
                    @Override
                    public long position() throws IOException {
                        return written;
                    }
                    
                    @Override
                    public SeekableByteChannel position(long pos) throws IOException {
                        throw new UnsupportedOperationException();
                    }
                    
                    @Override
                    public int read(ByteBuffer dst) throws IOException {
                        throw new UnsupportedOperationException();
                    }
                    
                    @Override
                    public long size() throws IOException {
                        return written;
                    }
                    
                    @Override
                    public SeekableByteChannel truncate(long size) throws IOException {
                        throw new UnsupportedOperationException();
                    }
                    
                    @Override
                    public int write(ByteBuffer src) throws IOException {
                        int n = wbc.write(src);
                        written += n;
                        return n;
                    }
                };
            } finally {
                endRead();
            }
        } else {
            beginRead();
            try {
                ensureOpen();
                Entry e = getEntry(path);
                if (e == null || e.isDir()) {
                    throw new NoSuchFileException(getString(path));
                }
                final ReadableByteChannel rbc = Channels.newChannel(getInputStream(e));
                final long size = e.size;
                return new SeekableByteChannel() {
                    long read = 0;
                    
                    @Override
                    public void close() throws IOException {
                        rbc.close();
                    }
                    
                    @Override
                    public boolean isOpen() {
                        return rbc.isOpen();
                    }
                    
                    @Override
                    public long position() throws IOException {
                        return read;
                    }
                    
                    @Override
                    public SeekableByteChannel position(long pos) throws IOException {
                        throw new UnsupportedOperationException();
                    }
                    
                    @Override
                    public int read(ByteBuffer dst) throws IOException {
                        int n = rbc.read(dst);
                        if (n > 0) {
                            read += n;
                        }
                        return n;
                    }
                    
                    @Override
                    public long size() throws IOException {
                        return size;
                    }
                    
                    @Override
                    public SeekableByteChannel truncate(long size) throws IOException {
                        throw new NonWritableChannelException();
                    }
                    
                    @Override
                    public int write(ByteBuffer src) throws IOException {
                        throw new NonWritableChannelException();
                    }
                };
            } finally {
                endRead();
            }
        }
    }
    
    // Returns a FileChannel of the specified entry.
    //
    // This implementation creates a temporary file on the default file system,
    // copy the entry data into it if the entry exists, and then create a
    // FileChannel on top of it.
    FileChannel newFileChannel(byte[] path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        checkOptions(options);
        final boolean forWrite = options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.APPEND);
        beginRead();
        try {
            ensureOpen();
            Entry e = getEntry(path);
            if (forWrite) {
                checkWritable();
                if (e == null) {
                    if (!options.contains(StandardOpenOption.CREATE) && !options.contains(StandardOpenOption.CREATE_NEW)) {
                        throw new NoSuchFileException(getString(path));
                    }
                } else {
                    if (options.contains(StandardOpenOption.CREATE_NEW)) {
                        throw new FileAlreadyExistsException(getString(path));
                    }
                    if (e.isDir()) {
                        throw new FileAlreadyExistsException("directory <" + getString(path) + "> exists");
                    }
                }
                options = new HashSet<>(options);
                options.remove(StandardOpenOption.CREATE_NEW); // for tmpfile
            } else if (e == null || e.isDir()) {
                throw new NoSuchFileException(getString(path));
            }
            
            final boolean isFCH = e != null && e.type == Entry.FILECH;
            final Path tmpfile = isFCH ? e.file : getTempPathForEntry(path);
            final FileChannel fch = tmpfile.getFileSystem().provider().newFileChannel(tmpfile, options, attrs);
            final Entry u = isFCH ? e : new Entry(path, tmpfile, Entry.FILECH);
            if (forWrite) {
                u.flag = FLAG_DATADESCR;
                u.method = METHOD_STORED;
            }
            // is there a better way to hook into the FileChannel's close method?
            return new FileChannel() {
                @Override
                public void force(boolean metaData) throws IOException {
                    fch.force(metaData);
                }
                
                @Override
                protected void implCloseChannel() throws IOException {
                    fch.close();
                    if (forWrite) {
                        u.mtime = System.currentTimeMillis();
                        u.size = Files.size(u.file);
                        
                        update(u);
                    } else {
                        if (!isFCH) {
                            removeTempPathForEntry(tmpfile);
                        }
                    }
                }
                
                @Override
                public FileLock lock(long position, long size, boolean shared) throws IOException {
                    return fch.lock(position, size, shared);
                }
                
                @Override
                public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
                    throw new UnsupportedOperationException();
                }
                
                @Override
                public long position() throws IOException {
                    return fch.position();
                }
                
                @Override
                public FileChannel position(long newPosition) throws IOException {
                    fch.position(newPosition);
                    return this;
                }
                
                @Override
                public int read(ByteBuffer dst) throws IOException {
                    return fch.read(dst);
                }
                
                @Override
                public int read(ByteBuffer dst, long position) throws IOException {
                    return fch.read(dst, position);
                }
                
                @Override
                public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                    return fch.read(dsts, offset, length);
                }
                
                @Override
                public long size() throws IOException {
                    return fch.size();
                }
                
                @Override
                public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
                    return fch.transferFrom(src, position, count);
                }
                
                @Override
                public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
                    return fch.transferTo(position, count, target);
                }
                
                @Override
                public FileChannel truncate(long size) throws IOException {
                    fch.truncate(size);
                    return this;
                }
                
                @Override
                public FileLock tryLock(long position, long size, boolean shared) throws IOException {
                    return fch.tryLock(position, size, shared);
                }
                
                @Override
                public int write(ByteBuffer src) throws IOException {
                    return fch.write(src);
                }
                
                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    return fch.write(src, position);
                }
                
                @Override
                public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                    return fch.write(srcs, offset, length);
                }
            };
        } finally {
            endRead();
        }
    }
    
    // Returns an input stream for reading the contents of the specified
    // file entry.
    InputStream newInputStream(byte[] path) throws IOException {
        beginRead();
        try {
            ensureOpen();
            Entry e = getEntry(path);
            if (e == null) {
                throw new NoSuchFileException(getString(path));
            }
            if (e.isDir()) {
                throw new FileSystemException(getString(path), "is a directory", null);
            }
            return getInputStream(e);
        } finally {
            endRead();
        }
    }
    
    // Returns an output stream for writing the contents into the specified
    // entry.
    OutputStream newOutputStream(byte[] path, OpenOption... options) throws IOException {
        checkWritable();
        boolean hasCreateNew = false;
        boolean hasCreate = false;
        boolean hasAppend = false;
        boolean hasTruncate = false;
        for (OpenOption opt : options) {
            if (opt == READ) {
                throw new IllegalArgumentException("READ not allowed");
            }
            if (opt == CREATE_NEW) {
                hasCreateNew = true;
            }
            if (opt == CREATE) {
                hasCreate = true;
            }
            if (opt == APPEND) {
                hasAppend = true;
            }
            if (opt == TRUNCATE_EXISTING) {
                hasTruncate = true;
            }
        }
        if (hasAppend && hasTruncate) {
            throw new IllegalArgumentException("APPEND + TRUNCATE_EXISTING not allowed");
        }
        beginRead(); // only need a readlock, the "update()" will
        try { // try to obtain a writelock when the os is
            ensureOpen(); // being closed.
            Entry e = getEntry(path);
            if (e != null) {
                if (e.isDir() || hasCreateNew) {
                    throw new FileAlreadyExistsException(getString(path));
                }
                if (hasAppend) {
                    InputStream is = getInputStream(e);
                    OutputStream os = getOutputStream(new Entry(e, Entry.NEW));
                    copyStream(is, os);
                    is.close();
                    return os;
                }
                return getOutputStream(new Entry(e, Entry.NEW));
            } else {
                if (!hasCreate && !hasCreateNew) {
                    throw new NoSuchFileException(getString(path));
                }
                checkParents(path);
                return getOutputStream(new Entry(path, Entry.NEW, false));
            }
        } finally {
            endRead();
        }
    }
    
    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public FileSystemProvider provider() {
        return provider;
    }
    
    // Reads len bytes of data from the specified offset into buf.
    // Returns the total number of bytes read.
    // Each/every byte read from here (except the cen, which is mapped).
    final long readFullyAt(byte[] buf, int off, long len, long pos) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(buf);
        bb.position(off);
        bb.limit((int) (off + len));
        return readFullyAt(bb, pos);
    }
    
    private final long readFullyAt(ByteBuffer bb, long pos) throws IOException {
        synchronized (ch) {
            return ch.position(pos).read(bb);
        }
    }
    
    // Releases the specified inflater to the list of available inflaters.
    private void releaseDeflater(Deflater def) {
        synchronized (deflaters) {
            if (inflaters.size() < MAX_FLATER) {
                def.reset();
                deflaters.add(def);
            } else {
                def.end();
            }
        }
    }
    
    // Releases the specified inflater to the list of available inflaters.
    private void releaseInflater(Inflater inf) {
        synchronized (inflaters) {
            if (inflaters.size() < MAX_FLATER) {
                inf.reset();
                inflaters.add(inf);
            } else {
                inf.end();
            }
        }
    }
    
    private void removeFromTree(IndexNode inode) {
        IndexNode parent = inodes.get(LOOKUPKEY.as(getParent(inode.name)));
        IndexNode child = parent.child;
        if (child.equals(inode)) {
            parent.child = child.sibling;
        } else {
            IndexNode last = child;
            while ((child = child.sibling) != null) {
                if (child.equals(inode)) {
                    last.sibling = child.sibling;
                    break;
                } else {
                    last = child;
                }
            }
        }
    }
    
    private void removeTempPathForEntry(Path path) throws IOException {
        Files.delete(path);
        tmppaths.remove(path);
    }
    
    // XXX: Edit to allow read only ZipFileSystems
    public void setReadOnly() {
        readOnly = true;
    }
    
    void setTimes(byte[] path, FileTime mtime, FileTime atime, FileTime ctime) throws IOException {
        checkWritable();
        beginWrite();
        try {
            ensureOpen();
            Entry e = getEntry(path); // ensureOpen checked
            if (e == null) {
                throw new NoSuchFileException(getString(path));
            }
            if (e.type == Entry.CEN) {
                e.type = Entry.COPY; // copy e
            }
            if (mtime != null) {
                e.mtime = mtime.toMillis();
            }
            if (atime != null) {
                e.atime = atime.toMillis();
            }
            if (ctime != null) {
                e.ctime = ctime.toMillis();
            }
            update(e);
        } finally {
            endWrite();
        }
    }
    
    @Override
    public Set<String> supportedFileAttributeViews() {
        return supportedFileAttributeViews;
    }
    
    // sync the zip file system, if there is any udpate
    private void sync() throws IOException {
        // System.out.printf("->sync(%s) starting....!%n", toString());
        // check ex-closer
        if (!exChClosers.isEmpty()) {
            for (ExChannelCloser ecc : exChClosers) {
                if (ecc.streams.isEmpty()) {
                    ecc.ch.close();
                    Files.delete(ecc.path);
                    exChClosers.remove(ecc);
                }
            }
        }
        if (!hasUpdate) {
            return;
        }
        Path tmpFile = createTempFileInSameDirectoryAs(zfpath);
        try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(tmpFile, WRITE))) {
            ArrayList<Entry> elist = new ArrayList<>(inodes.size());
            long written = 0;
            byte[] buf = new byte[8192];
            Entry e = null;
            
            // write loc
            for (IndexNode inode : inodes.values()) {
                if (inode instanceof Entry) { // an updated inode
                    e = (Entry) inode;
                    try {
                        if (e.type == Entry.COPY) {
                            // entry copy: the only thing changed is the "name"
                            // and "nlen" in LOC header, so we udpate/rewrite the
                            // LOC in new file and simply copy the rest (data and
                            // ext) without enflating/deflating from the old zip
                            // file LOC entry.
                            written += copyLOCEntry(e, true, os, written, buf);
                        } else { // NEW, FILECH or CEN
                            e.locoff = written;
                            written += e.writeLOC(os); // write loc header
                            if (e.bytes != null) { // in-memory, deflated
                                os.write(e.bytes); // already
                                written += e.bytes.length;
                            } else if (e.file != null) { // tmp file
                                try (InputStream is = Files.newInputStream(e.file)) {
                                    int n;
                                    if (e.type == Entry.NEW) { // deflated already
                                        while ((n = is.read(buf)) != -1) {
                                            os.write(buf, 0, n);
                                            written += n;
                                        }
                                    } else if (e.type == Entry.FILECH) {
                                        // the data are not deflated, use ZEOS
                                        try (OutputStream os2 = new EntryOutputStream(e, os)) {
                                            while ((n = is.read(buf)) != -1) {
                                                os2.write(buf, 0, n);
                                            }
                                        }
                                        written += e.csize;
                                        if ((e.flag & FLAG_DATADESCR) != 0) {
                                            written += e.writeEXT(os);
                                        }
                                    }
                                }
                                Files.delete(e.file);
                                tmppaths.remove(e.file);
                            } else {
                                // dir, 0-length data
                            }
                        }
                        elist.add(e);
                    } catch (IOException x) {
                        x.printStackTrace(); // skip any in-accurate entry
                    }
                } else { // unchanged inode
                    if (inode.pos == -1) {
                        continue; // pseudo directory node
                    }
                    e = Entry.readCEN(this, inode);
                    try {
                        written += copyLOCEntry(e, false, os, written, buf);
                        elist.add(e);
                    } catch (IOException x) {
                        x.printStackTrace(); // skip any wrong entry
                    }
                }
            }
            
            // now write back the cen and end table
            end.cenoff = written;
            for (Entry entry : elist) {
                written += entry.writeCEN(os);
            }
            end.centot = elist.size();
            end.cenlen = written - end.cenoff;
            end.write(os, written);
        }
        if (!streams.isEmpty()) {
            //
            // TBD: ExChannelCloser should not be necessary if we only
            // sync when being closed, all streams should have been
            // closed already. Keep the logic here for now.
            //
            // There are outstanding input streams open on existing "ch",
            // so, don't close the "cha" and delete the "file for now, let
            // the "ex-channel-closer" to handle them
            ExChannelCloser ecc = new ExChannelCloser(createTempFileInSameDirectoryAs(zfpath), ch, streams);
            Files.move(zfpath, ecc.path, REPLACE_EXISTING);
            exChClosers.add(ecc);
            streams = Collections.synchronizedSet(new HashSet<InputStream>());
        } else {
            ch.close();
            Files.delete(zfpath);
        }
        
        Files.move(tmpFile, zfpath, REPLACE_EXISTING);
        hasUpdate = false; // clear
    }
    
    @Override
    public String toString() {
        return zfpath.toString();
    }
    
    private void update(Entry e) {
        beginWrite();
        try {
            IndexNode old = inodes.put(e, e);
            if (old != null) {
                removeFromTree(old);
            }
            if (e.type == Entry.NEW || e.type == Entry.FILECH || e.type == Entry.COPY) {
                IndexNode parent = inodes.get(LOOKUPKEY.as(getParent(e.name)));
                e.sibling = parent.child;
                parent.child = e;
            }
            hasUpdate = true;
        } finally {
            endWrite();
        }
    }
    
    private void updateDelete(IndexNode inode) {
        beginWrite();
        try {
            removeFromTree(inode);
            inodes.remove(inode);
            hasUpdate = true;
        } finally {
            endWrite();
        }
    }
}
