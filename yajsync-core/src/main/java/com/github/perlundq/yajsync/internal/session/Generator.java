/*
 * Receiver -> Sender communication, generation of file meta data +
 * checksum info to peer Sender
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013-2016 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.perlundq.yajsync.internal.session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.FileSelection;
import com.github.perlundq.yajsync.RsyncException;
import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.attr.LocatableDeviceInfo;
import com.github.perlundq.yajsync.attr.LocatableFileInfo;
import com.github.perlundq.yajsync.attr.LocatableSymlinkInfo;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.channels.Message;
import com.github.perlundq.yajsync.internal.channels.MessageCode;
import com.github.perlundq.yajsync.internal.channels.RsyncOutChannel;
import com.github.perlundq.yajsync.internal.io.FileView;
import com.github.perlundq.yajsync.internal.io.FileViewException;
import com.github.perlundq.yajsync.internal.io.FileViewOpenFailed;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.text.TextEncoder;
import com.github.perlundq.yajsync.internal.util.FileOps;
import com.github.perlundq.yajsync.internal.util.MD5;
import com.github.perlundq.yajsync.internal.util.Pair;
import com.github.perlundq.yajsync.internal.util.Rolling;
import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.internal.util.Util;

public class Generator implements RsyncTask {
    public static class Builder {
        private Charset _charset;
        private final byte[] _checksumSeed;
        private FileSelection _fileSelection = FileSelection.EXACT;
        private boolean _isAlwaysItemize;
        private boolean _isDelete;
        private boolean _isIgnoreTimes;
        private boolean _isInterruptible = true;
        private boolean _isNumericIds;
        private boolean _isPreserveDevices;
        private boolean _isPreserveGroup;
        private boolean _isPreserveLinks;
        private boolean _isPreservePermissions;
        private boolean _isPreserveSpecials;
        private boolean _isPreserveTimes;
        private boolean _isPreserveUser;
        private final WritableByteChannel _out;
        
        public Builder(WritableByteChannel out, byte[] checksumSeed) {
            assert out != null;
            assert checksumSeed != null;
            this._out = out;
            this._checksumSeed = checksumSeed;
        }
        
        public Generator build() {
            assert !this._isDelete || this._fileSelection != FileSelection.EXACT;
            return new Generator(this);
        }
        
        public Builder charset(Charset charset) {
            assert charset != null;
            this._charset = charset;
            return this;
        }
        
        public Builder fileSelection(FileSelection fileSelection) {
            assert fileSelection != null;
            this._fileSelection = fileSelection;
            return this;
        }
        
        public Builder isAlwaysItemize(boolean isAlwaysItemize) {
            this._isAlwaysItemize = isAlwaysItemize;
            return this;
        }
        
        public Builder isDelete(boolean isDelete) {
            this._isDelete = isDelete;
            return this;
        }
        
        public Builder isIgnoreTimes(boolean isIgnoreTimes) {
            this._isIgnoreTimes = isIgnoreTimes;
            return this;
        }
        
        public Builder isInterruptible(boolean isInterruptible) {
            this._isInterruptible = isInterruptible;
            return this;
        }
        
        public Builder isNumericIds(boolean isNumericIds) {
            this._isNumericIds = isNumericIds;
            return this;
        }
        
        public Builder isPreserveDevices(boolean isPreserveDevices) {
            this._isPreserveDevices = isPreserveDevices;
            return this;
        }
        
        public Builder isPreserveGroup(boolean isPreserveGroup) {
            this._isPreserveGroup = isPreserveGroup;
            return this;
        }
        
        public Builder isPreserveLinks(boolean isPreserveLinks) {
            this._isPreserveLinks = isPreserveLinks;
            return this;
        }
        
        public Builder isPreservePermissions(boolean isPreservePermissions) {
            this._isPreservePermissions = isPreservePermissions;
            return this;
        }
        
        public Builder isPreserveSpecials(boolean isPreserveSpecials) {
            this._isPreserveSpecials = isPreserveSpecials;
            return this;
        }
        
        public Builder isPreserveTimes(boolean isPreserveTimes) {
            this._isPreserveTimes = isPreserveTimes;
            return this;
        }
        
        public Builder isPreserveUser(boolean isPreserveUser) {
            this._isPreserveUser = isPreserveUser;
            return this;
        }
    }
    
    private interface Job {
        void process() throws RsyncException;
    }
    
    private static final Logger _log = Logger.getLogger(Generator.class.getName());
    private static final int MIN_BLOCK_SIZE = 512;
    private static final int OUTPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final Checksum.Header ZERO_SUM;
    
    static {
        try {
            ZERO_SUM = new Checksum.Header(0, 0, 0);
        } catch (Checksum.ChunkOverflow e) {
            throw new RuntimeException(e);
        }
    }
    
    private static int getBlockLengthFor(long fileSize) {
        assert fileSize >= 0;
        if (fileSize == 0) {
            return 0;
        }
        int blockLength = pow2SquareRoot(fileSize);
        assert fileSize / blockLength <= Integer.MAX_VALUE;
        return Math.max(MIN_BLOCK_SIZE, blockLength);
    }
    
    private static int getDigestLength(long fileSize, int block_length) {
        int result = ((int) (10 + 2 * (long) Util.log2(fileSize) - (long) Util.log2(block_length)) - 24) / 8;
        result = Math.min(result, Checksum.MAX_DIGEST_LENGTH);
        return Math.max(result, Checksum.MIN_DIGEST_LENGTH);
    }
    
    // return the square root of num as the nearest lower number in base 2
    /**
     * @throws IllegalArgumentException if num is negative or result would overflow
     *                                  an integer
     */
    private static int pow2SquareRoot(long num) {
        if (num < 0) {
            throw new IllegalArgumentException(String.format("cannot compute square root of %d", num));
        }
        
        if (num == 0) {
            return 0;
        }
        // sqrt(2**n) == 2**(n/2)
        long nearestLowerBase2 = Long.highestOneBit(num);
        int exponent = Long.numberOfTrailingZeros(nearestLowerBase2);
        int sqrtExponent = exponent / 2;
        long result = 1 << sqrtExponent;
        if (result < 0 || result > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("square root of %d (%d) is either negative or larger than max" + " int value (%d)", num, result, Integer.MAX_VALUE));
        }
        return (int) result;
    }
    
    private final TextEncoder _characterEncoder;
    private final byte[] _checksumSeed;
    private final SimpleDateFormat _dateFormat = new SimpleDateFormat();
    private final Deque<Job> _deferredJobs = new ArrayDeque<>();
    private FileAttributeManager _fileAttributeManager;
    private final Filelist _fileList;
    private final FileSelection _fileSelection;
    private final List<Filelist.Segment> _generated = new LinkedList<>();
    private final boolean _isAlwaysItemize;
    private final boolean _isDelete;
    private volatile boolean _isDeletionsEnabled;
    private final boolean _isIgnoreTimes;
    private final boolean _isInterruptible;
    private final boolean _isNumericIds;
    private final boolean _isPreserveDevices;
    private final boolean _isPreserveGroup;
    private final boolean _isPreserveLinks;
    private final boolean _isPreservePermissions;
    private final boolean _isPreserveSpecials;
    
    private final boolean _isPreserveTimes;
    private final boolean _isPreserveUser;
    private boolean _isRunning = true;
    private final LinkedBlockingQueue<Job> _jobs = new LinkedBlockingQueue<>();
    
    private final BlockingQueue<Pair<Boolean, FileInfo>> _listing = new LinkedBlockingQueue<>();
    
    private final RsyncOutChannel _out;
    
    private final BitSet _pruned = new BitSet();
    
    private int _returnStatus;
    
    private Generator(Builder builder) {
        this._checksumSeed = builder._checksumSeed;
        this._fileSelection = builder._fileSelection;
        this._fileList = new ConcurrentFilelist(this._fileSelection == FileSelection.RECURSE, true);
        this._out = new RsyncOutChannel(builder._out, OUTPUT_CHANNEL_BUF_SIZE);
        this._characterEncoder = TextEncoder.newStrict(builder._charset);
        this._isAlwaysItemize = builder._isAlwaysItemize;
        this._isDelete = builder._isDelete;
        this._isDeletionsEnabled = this._fileSelection != FileSelection.EXACT;
        this._isIgnoreTimes = builder._isIgnoreTimes;
        this._isInterruptible = builder._isInterruptible;
        this._isPreserveDevices = builder._isPreserveDevices;
        this._isPreserveLinks = builder._isPreserveLinks;
        this._isPreservePermissions = builder._isPreservePermissions;
        this._isPreserveSpecials = builder._isPreserveSpecials;
        this._isPreserveTimes = builder._isPreserveTimes;
        this._isPreserveUser = builder._isPreserveUser;
        this._isPreserveGroup = builder._isPreserveGroup;
        this._isNumericIds = builder._isNumericIds;
    }
    
    private void appendJob(Job job) throws InterruptedException {
        assert job != null;
        this._jobs.put(job);
    }
    
    @Override
    public Boolean call() throws InterruptedException, RsyncException {
        try {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(this.toString());
            }
            this.processJobQueueBatched();
            return this._returnStatus == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            Pair<Boolean, FileInfo> poisonPill = new Pair<>(false, null);
            this._listing.add(poisonPill);
        }
    }
    
    Charset charset() {
        return this._characterEncoder.charset();
    }
    
    @Override
    public void closeChannel() throws ChannelException {
        this._out.close();
    }
    
    private void deferUpdateAttrsIfDiffer(final Path path, final RsyncFileAttributes curAttrsOrNull, final RsyncFileAttributes newAttrs) {
        assert path != null;
        assert newAttrs != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                try {
                    Generator.this.updateAttrsIfDiffer(path, curAttrsOrNull, newAttrs);
                } catch (IOException e) {
                    String msg = String.format("received I/O error while applying " + "attributes on %s: %s", path, e.getMessage());
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    Generator.this._out.putMessage(Generator.this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    Generator.this._returnStatus++;
                }
            }
        };
        this._deferredJobs.addFirst(j);
    }
    
    RsyncFileAttributes deleteIfDifferentType(LocatableFileInfo fileInfo) throws IOException {
        // null if file does not exist; throws IOException on any other error
        RsyncFileAttributes curAttrsOrNull = this._fileAttributeManager.statIfExists(fileInfo.path());
        if (curAttrsOrNull != null && curAttrsOrNull.fileType() != fileInfo.attrs().fileType()) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("deleting %s %s (expecting a %s)", FileOps.fileType(curAttrsOrNull.mode()), fileInfo.path(), FileOps.fileType(fileInfo.attrs().mode())));
            }
            FileOps.unlink(fileInfo.path());
            return null;
        } else {
            return curAttrsOrNull;
        }
    }
    
    void disableDelete() {
        if (this._isDelete && this._isDeletionsEnabled) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning("--delete disabled due to receiving error " + "notification from peer sender");
            }
            this._isDeletionsEnabled = false;
        }
    }
    
    Filelist fileList() {
        return this._fileList;
    }
    
    public BlockingQueue<Pair<Boolean, FileInfo>> files() {
        return this._listing;
    }
    
    FileSelection fileSelection() {
        return this._fileSelection;
    }
    
    void generateFile(final Filelist.Segment segment, final int fileIndex, final LocatableFileInfo fileInfo) throws InterruptedException {
        assert segment != null;
        assert fileInfo != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                try {
                    boolean isTransfer = Generator.this.itemizeFile(fileIndex, fileInfo, Checksum.MAX_DIGEST_LENGTH);
                    if (!isTransfer) {
                        segment.remove(fileIndex);
                        Generator.this.removeAllFinishedSegmentsAndNotifySender();
                    }
                } catch (IOException e) {
                    String msg = String.format("failed to generate file meta " + "data for %s (index %d): %s", fileInfo.path(), fileIndex, e.getMessage());
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    Generator.this._out.putMessage(Generator.this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    Generator.this._returnStatus++;
                }
            }
            
            @Override
            public String toString() {
                return String.format("generateFile (%s, %d, %s)", segment, fileIndex, fileInfo.path());
            }
        };
        this.appendJob(j);
    }
    
    void generateSegment(final Filelist.Segment segment) throws InterruptedException {
        assert segment != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this.sendChecksumForSegment(segment);
                Generator.this._generated.add(segment);
                Generator.this.removeAllFinishedSegmentsAndNotifySender();
            }
            
            @Override
            public String toString() {
                return String.format("generateSegment(%s)", segment);
            }
        };
        this.appendJob(j);
    }
    
    @Override
    public boolean isInterruptible() {
        return this._isInterruptible;
    }
    
    boolean isNumericIds() {
        return this._isNumericIds;
    }
    
    boolean isPreserveDevices() {
        return this._isPreserveDevices;
    }
    
    boolean isPreserveGroup() {
        return this._isPreserveGroup;
    }
    
    boolean isPreserveLinks() {
        return this._isPreserveLinks;
    }
    
    boolean isPreservePermissions() {
        return this._isPreservePermissions;
    }
    
    boolean isPreserveSpecials() {
        return this._isPreserveSpecials;
    }
    
    boolean isPreserveTimes() {
        return this._isPreserveTimes;
    }
    
    boolean isPreserveUser() {
        return this._isPreserveUser;
    }
    
    boolean isPruned(int index) {
        return this._pruned.get(index);
    }
    
    /**
     * @param index - currently unused
     * @param dev   - currently unused
     */
    private void itemizeDevice(int index, LocatableDeviceInfo dev) throws IOException {
        throw new IOException("unable to generate device file - operation " + "not supported");
    }
    
    private void itemizeDirectory(int index, LocatableFileInfo fileInfo) throws ChannelException, IOException {
        assert fileInfo != null;
        
        RsyncFileAttributes curAttrsOrNull = this.deleteIfDifferentType(fileInfo);
        if (curAttrsOrNull == null) {
            this.sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.attrs(), Item.LOCAL_CHANGE);
            this.mkdir(fileInfo); // throws IOException
        } else {
            if (this._isAlwaysItemize) {
                this.sendItemizeInfo(index, curAttrsOrNull, fileInfo.attrs(), Item.NO_CHANGE);
            }
            if (!curAttrsOrNull.equals(fileInfo.attrs())) {
                this.deferUpdateAttrsIfDiffer(fileInfo.path(), curAttrsOrNull, fileInfo.attrs());
            }
        }
    }
    
    private boolean itemizeFile(int index, LocatableFileInfo fileInfo, int digestLength) throws ChannelException, IOException {
        assert fileInfo != null;
        
        RsyncFileAttributes curAttrsOrNull = this.deleteIfDifferentType(fileInfo);
        
        // NOTE: native opens the file first though even if its file size is
        // zero
        if (FileOps.isDataModified(curAttrsOrNull, fileInfo.attrs()) || this._isIgnoreTimes) {
            if (curAttrsOrNull == null) {
                this.sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.attrs(), Item.TRANSFER);
                this.sendChecksumHeader(ZERO_SUM);
            } else {
                this.sendItemizeAndChecksums(index, fileInfo, curAttrsOrNull, digestLength);
            }
            return true;
        }
        
        if (this._isAlwaysItemize) {
            this.sendItemizeInfo(index, curAttrsOrNull, fileInfo.attrs(), Item.NO_CHANGE);
        }
        
        try {
            this.updateAttrsIfDiffer(fileInfo.path(), curAttrsOrNull, fileInfo.attrs());
        } catch (IOException e) {
            String msg = String.format("received an I/O error while applying attributes on %s: %s", fileInfo.path(), e.getMessage());
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(msg);
            }
            this._out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            this._returnStatus++;
        }
        return false;
    }
    
    private char itemizeFlags(RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs) {
        assert newAttrs != null;
        
        if (curAttrsOrNull == null) {
            return Item.IS_NEW;
        }
        char iFlags = Item.NO_CHANGE;
        if (this._isPreservePermissions && curAttrsOrNull.mode() != newAttrs.mode()) {
            iFlags |= Item.REPORT_PERMS;
        }
        if (this._isPreserveTimes && curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime()) {
            iFlags |= Item.REPORT_TIME;
        }
        if (this._isPreserveUser && !curAttrsOrNull.user().equals(newAttrs.user())) {
            iFlags |= Item.REPORT_OWNER;
        }
        if (this._isPreserveGroup && !curAttrsOrNull.group().equals(newAttrs.group())) {
            iFlags |= Item.REPORT_GROUP;
        }
        if (curAttrsOrNull.isRegularFile() && curAttrsOrNull.size() != newAttrs.size()) {
            iFlags |= Item.REPORT_SIZE;
        }
        return iFlags;
    }
    
    private int itemizeSegment(Filelist.Segment segment) throws ChannelException {
        int numErrors = 0;
        List<Integer> toRemove = new LinkedList<>();
        
        for (Map.Entry<Integer, FileInfo> entry : segment.entrySet()) {
            final int index = entry.getKey();
            final FileInfo f = entry.getValue();
            boolean isTransfer = false;
            
            if (f instanceof LocatableFileInfo) {
                LocatableFileInfo lf = (LocatableFileInfo) f;
                try {
                    if (_log.isLoggable(Level.FINE)) {
                        _log.fine(String.format("(Generator) generating %s, index %d", lf.path(), index));
                    }
                    if (f.attrs().isRegularFile()) {
                        isTransfer = this.itemizeFile(index, lf, Checksum.MIN_DIGEST_LENGTH);
                    } else if (f.attrs().isDirectory()) {
                        if (this._fileSelection != FileSelection.RECURSE) {
                            this.itemizeDirectory(index, lf);
                        }
                    } else if (f instanceof LocatableDeviceInfo && this._isPreserveDevices && (f.attrs().isBlockDevice() || f.attrs().isCharacterDevice())) {
                        this.itemizeDevice(index, (LocatableDeviceInfo) f);
                    } else if (f instanceof LocatableDeviceInfo && this._isPreserveSpecials && (f.attrs().isFifo() || f.attrs().isSocket())) {
                        this.itemizeDevice(index, (LocatableDeviceInfo) f);
                    } else if (this._isPreserveLinks && f instanceof LocatableSymlinkInfo) {
                        this.itemizeSymlink(index, (LocatableSymlinkInfo) f);
                    } else {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("(Generator) Skipping " + lf.path());
                        }
                    }
                } catch (IOException e) {
                    if (lf.attrs().isDirectory()) {
                        // we cannot remove the corresponding segment since we
                        // may not have received it yet
                        this.prune(index);
                    }
                    String msg = String.format("failed to generate %s %s (index %d): %s", FileOps.fileTypeToString(lf.attrs().mode()), lf.path(), index, e.getMessage());
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    this._out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    numErrors++;
                }
            } else {
                if (_log.isLoggable(Level.WARNING)) {
                    _log.warning("skipping untransferrable " + f);
                }
            }
            if (!isTransfer) {
                toRemove.add(index);
            }
        }
        segment.removeAll(toRemove);
        return numErrors;
    }
    
    private void itemizeSymlink(int index, LocatableSymlinkInfo linkInfo) throws IOException, ChannelException {
        try {
            RsyncFileAttributes curAttrsOrNull = this.deleteIfDifferentType(linkInfo);
            if (curAttrsOrNull != null) {
                Path curTarget = Files.readSymbolicLink(linkInfo.path());
                if (curTarget.toString().equals(linkInfo.targetPathName())) {
                    if (this._isAlwaysItemize) {
                        this.sendItemizeInfo(index, curAttrsOrNull, linkInfo.attrs(), Item.NO_CHANGE);
                    }
                    return;
                }
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("deleting symlink %s -> %s", linkInfo.path(), curTarget));
                }
                FileOps.unlink(linkInfo.path());
            }
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("creating symlink %s -> %s", linkInfo.path(), linkInfo.targetPathName()));
            }
            Path targetPath = linkInfo.path().getFileSystem().getPath(linkInfo.targetPathName());
            Files.createSymbolicLink(linkInfo.path(), targetPath);
            
            this.sendItemizeInfo(index, null /* curAttrsOrNull */, linkInfo.attrs(), (char) (Item.LOCAL_CHANGE | Item.REPORT_CHANGE));
        } catch (UnsupportedOperationException e) {
            throw new IOException(e);
        }
    }
    
    void listSegment(final Filelist.Segment segment) throws InterruptedException {
        assert segment != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Collection<FileInfo> c;
                if (Generator.this._fileSelection != FileSelection.RECURSE) {
                    c = segment.files();
                } else if (segment.directory() == null) {
                    c = Generator.this.toInitialListing(segment);
                } else {
                    c = Generator.this.toListing(segment);
                }
                Generator.this._listing.addAll(Generator.this.toListingPair(c));
                segment.removeAll();
                Filelist.Segment deleted = Generator.this._fileList.deleteFirstSegment();
                if (deleted != segment) {
                    throw new IllegalStateException(String.format("%s != %s", deleted, segment));
                }
                Generator.this._out.encodeIndex(Filelist.DONE);
                
            }
            
            @Override
            public String toString() {
                return String.format("listSegment(%s)", segment);
            }
        };
        this.appendJob(j);
    }
    
    // NOTE: no error if dir already exists
    private void mkdir(LocatableFileInfo dir) throws IOException {
        assert dir != null;
        
        RsyncFileAttributes attrs = this._fileAttributeManager.statOrNull(dir.path());
        if (attrs == null) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) creating directory " + dir.path());
            }
            Files.createDirectories(dir.path());
        }
        this.deferUpdateAttrsIfDiffer(dir.path(), attrs, dir.attrs());
    }
    
    synchronized long numBytesWritten() {
        return this._out.numBytesWritten();
    }
    
    void processDeferredJobs() throws InterruptedException {
        Job job = new Job() {
            @Override
            public void process() throws RsyncException {
                for (Job j : Generator.this._deferredJobs) {
                    j.process();
                }
            }
            
            @Override
            public String toString() {
                return "processDeferredJobs()";
            }
        };
        this.appendJob(job);
    }
    
    private void processJobQueueBatched() throws InterruptedException, RsyncException {
        List<Job> jobList = new LinkedList<>();
        while (this._isRunning) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("(Generator) awaiting next jobs...");
            }
            
            jobList.add(this._jobs.take());
            this._jobs.drainTo(jobList);
            
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) got %d job(s)", jobList.size()));
            }
            
            for (Job job : jobList) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("(Generator) processing " + job);
                }
                job.process();
            }
            jobList.clear();
            if (this._jobs.isEmpty()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("(Generator) flushing %d bytes", this._out.numBytesBuffered()));
                }
                this._out.flush();
            }
        }
    }
    
    void prune(int index) {
        this._pruned.set(index);
    }
    
    void purgeFile(final Filelist.Segment segment, final int index) throws InterruptedException {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException, RsyncProtocolException {
                if (segment != null) {
                    segment.remove(index);
                } else {
                    Filelist.Segment tmpSegment = Generator.this._fileList.getSegmentWith(index);
                    if (tmpSegment == null) {
                        throw new RsyncProtocolException(String.format("invalid file index %d from peer", index));
                    }
                    tmpSegment.remove(index);
                }
                Generator.this.removeAllFinishedSegmentsAndNotifySender();
            }
            
            @Override
            public String toString() {
                return String.format("purgeFile(%s, %d)", segment, index);
            }
        };
        this.appendJob(j);
    }
    
    private void removeAllFinishedSegmentsAndNotifySender() throws ChannelException {
        for (Iterator<Filelist.Segment> it = this._generated.iterator(); it.hasNext();) {
            Filelist.Segment segment = it.next();
            if (!segment.isFinished()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("(Generator) %s not finished yet", segment));
                }
                break;
            }
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) removing finished " + "segment %s and sending index %d", segment, Filelist.DONE));
            }
            Filelist.Segment deleted = this._fileList.deleteFirstSegment();
            // identity comparison
            if (deleted != segment) {
                throw new IllegalStateException(String.format("%s != %s", deleted, segment));
            }
            // NOTE: remove before notifying peer
            it.remove();
            this._out.encodeIndex(Filelist.DONE);
        }
    }
    
    // used for sending empty filter rules only
    void sendBytes(final ByteBuffer buf) throws InterruptedException {
        assert buf != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this._out.put(buf);
            }
            
            @Override
            public String toString() {
                return String.format("sendBytes(%s)", buf.duplicate());
            }
        };
        this.appendJob(j);
    }
    
    private void sendChecksumForSegment(Filelist.Segment segment) throws ChannelException {
        assert segment != null;
        
        final int dirIndex = segment.directoryIndex();
        if (segment.directory() != null && !(segment.directory() instanceof LocatableFileInfo)) {
            segment.removeAll();
            return;
        }
        LocatableFileInfo dir = (LocatableFileInfo) segment.directory();
        if (dir != null && (this.isPruned(dirIndex) || dir.path() == null)) {
            segment.removeAll();
            return;
        }
        
        boolean isInitialFileList = dir == null;
        if (isInitialFileList) {
            FileInfo tmp = segment.getFileWithIndexOrNull(dirIndex + 1);
            if (!(tmp instanceof LocatableFileInfo)) {
                segment.removeAll();
                return;
            }
            dir = (LocatableFileInfo) tmp;
        }
        if (this._isDelete && this._isDeletionsEnabled) {
            try {
                this.unlinkFilesInDirNotAtSender(dir.path(), segment.files());
            } catch (IOException e) {
                if (Files.exists(dir.path(), LinkOption.NOFOLLOW_LINKS)) {
                    String msg = String.format("failed to delete %s and all " + "its files: %s", dir, e);
                    if (_log.isLoggable(Level.WARNING)) {
                        _log.warning(msg);
                    }
                    this._out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    this._returnStatus++;
                }
            }
        }
        try {
            if (dir.attrs().isDirectory()) {
                this.mkdir(dir);
            }
            if (!isInitialFileList) {
                this.itemizeDirectory(dirIndex, dir);
            }
            this._returnStatus += this.itemizeSegment(segment);
        } catch (IOException e) {
            String msg = String.format("failed to generate files below dir %s (index %d): %s", dir.path(), dirIndex, e.getMessage());
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(msg);
            }
            this._out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            segment.removeAll();
            this._returnStatus++;
        }
    }
    
    private void sendChecksumHeader(Checksum.Header header) throws ChannelException {
        Connection.sendChecksumHeader(this._out, header);
    }
    
    private void sendItemizeAndChecksums(int index, LocatableFileInfo fileInfo, RsyncFileAttributes curAttrs, int minDigestLength) throws ChannelException {
        assert fileInfo != null;
        assert curAttrs != null;
        
        long currentSize = curAttrs.size();
        int blockLength = getBlockLengthFor(currentSize);
        int windowLength = blockLength;
        int digestLength = currentSize > 0 ? Math.max(minDigestLength, getDigestLength(currentSize, blockLength)) : 0;
        // new FileView() throws FileViewOpenFailed
        try (FileView fv = new FileView(fileInfo.path(), currentSize, blockLength, windowLength)) {
            
            // throws ChunkCountOverflow
            Checksum.Header header = new Checksum.Header(blockLength, digestLength, currentSize);
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) generating file %s, " + "index %d, checksum %s", fileInfo, index, header));
            }
            
            this.sendItemizeInfo(index, curAttrs, fileInfo.attrs(), Item.TRANSFER);
            this.sendChecksumHeader(header);
            
            MessageDigest md = MD5.newInstance();
            
            while (fv.windowLength() > 0) {
                int rolling = Rolling.compute(fv.array(), fv.startOffset(), fv.windowLength());
                this._out.putInt(rolling);
                md.update(fv.array(), fv.startOffset(), fv.windowLength());
                md.update(this._checksumSeed);
                this._out.put(md.digest(), 0, digestLength);
                fv.slide(fv.windowLength());
            }
        } catch (FileViewOpenFailed | Checksum.ChunkOverflow e) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(String.format("(Generator) received I/O error during checksum " + "generation (%s)", e.getMessage()));
            }
            this.sendItemizeInfo(index, null, fileInfo.attrs(), Item.TRANSFER);
            this.sendChecksumHeader(ZERO_SUM);
        } catch (FileViewException e) {
            // occurs at FileView.close() - if there were any I/O errors during
            // file read
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning("(Generator) Warning got I/O errors during " + "checksum generation. Errors ignored and data " + "filled with zeroes): " + e.getMessage());
            }
        }
    }
    
    private void sendItemizeInfo(int index, RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs, char iMask) throws ChannelException {
        assert newAttrs != null;
        char iFlags = (char) (iMask | this.itemizeFlags(curAttrsOrNull, newAttrs));
        if (_log.isLoggable(Level.FINE)) {
            _log.fine("(Generator) sending itemizeFlags=" + (int) iFlags);
        }
        this._out.encodeIndex(index);
        this._out.putChar(iFlags);
    }
    
    /**
     * @throws TextConversionException
     */
    void sendMessage(final MessageCode code, final String text) throws InterruptedException {
        assert code != null;
        assert text != null;
        final Message message = this.toMessage(code, text);
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this._out.putMessage(message);
            }
            
            @Override
            public String toString() {
                return String.format("sendMessage(%s, %s)", code, text);
            }
        };
        this.appendJob(j);
    }
    
    void sendSegmentDone() throws InterruptedException {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this._out.encodeIndex(Filelist.DONE);
            }
            
            @Override
            public String toString() {
                return "sendSegmentDone()";
            }
        };
        this.appendJob(j);
    }
    
    synchronized void setFileAttributeManager(FileAttributeManager fileAttributeManager) {
        this._fileAttributeManager = fileAttributeManager;
    }
    
    void stop() throws InterruptedException {
        Job job = new Job() {
            @Override
            public void process() {
                Generator.this._isRunning = false;
            }
            
            @Override
            public String toString() {
                return "stop()";
            }
        };
        this.appendJob(job);
    }
    
    private Collection<FileInfo> toInitialListing(Filelist.Segment segment) {
        assert this._fileSelection == FileSelection.RECURSE;
        assert segment.directory() == null;
        boolean listFirstDotDir = true;
        Collection<FileInfo> res = new ArrayList<>(segment.files().size());
        for (FileInfo f : segment.files()) {
            if (!f.attrs().isDirectory()) {
                res.add(f);
            } else if (listFirstDotDir) {
                if (((FileInfoImpl) f).isDotDir()) {
                    res.add(f);
                }
                listFirstDotDir = false;
            }
        }
        return res;
    }
    
    private Collection<FileInfo> toListing(Filelist.Segment segment) {
        assert this._fileSelection == FileSelection.RECURSE;
        assert segment.directory() != null;
        Collection<FileInfo> res = new ArrayList<>(segment.files().size());
        res.add(segment.directory());
        for (FileInfo f : segment.files()) {
            if (!f.attrs().isDirectory()) {
                res.add(f);
            }
        }
        return res;
    }
    
    Collection<Pair<Boolean, FileInfo>> toListingPair(Collection<FileInfo> files) {
        Collection<Pair<Boolean, FileInfo>> listing = new ArrayList<>(files.size());
        for (FileInfo f : files) {
            listing.add(new Pair<>(true, f));
        }
        return listing;
    }
    
    /**
     * @throws TextConversionException
     */
    private Message toMessage(MessageCode code, String text) {
        ByteBuffer payload = ByteBuffer.wrap(this._characterEncoder.encode(text));
        return new Message(code, payload);
    }
    
    @Override
    public String toString() {
        return String.format("%s(" + "isAlwaysItemize=%b, " + "isDelete=%b, " + "isIgnoreTimes=%b, " + "isInterruptible=%b, " + "isNumericIds=%b, " + "isPreserveDevices=%b, " + "isPreserveLinks=%b, "
                + "isPreservePermissions=%b, " + "isPreserveSpecials=%b, " + "isPreserveTimes=%b, " + "isPreserveUser=%b, " + "isPreserveGroup=%b, " + "checksumSeed=%s, " + "fileSelection=%s" + ")",
                this.getClass().getSimpleName(), this._isAlwaysItemize, this._isDelete, this._isIgnoreTimes, this._isInterruptible, this._isNumericIds, this._isPreserveDevices, this._isPreserveLinks,
                this._isPreservePermissions, this._isPreserveSpecials, this._isPreserveTimes, this._isPreserveUser, this._isPreserveGroup, Text.bytesToString(this._checksumSeed), this._fileSelection);
    }
    
    private void unlinkFilesInDirNotAtSender(Path dir, Collection<FileInfo> files) throws IOException, ChannelException {
        assert this._isDelete && this._isDeletionsEnabled;
        Set<Path> senderPaths = new HashSet<>(files.size());
        for (FileInfo f : files) {
            if (f instanceof LocatableFileInfo) {
                LocatableFileInfo lf = (LocatableFileInfo) f;
                senderPaths.add(lf.path());
            }
        }
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (!senderPaths.contains(entry)) {
                    try {
                        if (_log.isLoggable(Level.INFO)) {
                            _log.info("deleting extraneous " + entry);
                        }
                        FileOps.unlink(entry);
                    } catch (IOException e) {
                        String msg = String.format("failed to delete %s: %s", entry, e);
                        if (_log.isLoggable(Level.WARNING)) {
                            _log.warning(msg);
                        }
                        this._out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                        this._returnStatus++;
                        
                    }
                }
            }
        }
    }
    
    private void updateAttrsIfDiffer(Path path, RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs) throws IOException {
        assert path != null;
        assert newAttrs != null;
        
        if (this._isPreservePermissions && (curAttrsOrNull == null || curAttrsOrNull.mode() != newAttrs.mode())) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) %s: updating mode %o -> %o", path, curAttrsOrNull == null ? 0 : curAttrsOrNull.mode(), newAttrs.mode()));
            }
            this._fileAttributeManager.setFileMode(path, newAttrs.mode(), LinkOption.NOFOLLOW_LINKS);
        }
        if (this._isPreserveTimes && (curAttrsOrNull == null || curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime())) {
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("(Generator) %s: updating mtime %s -> %s", path,
                        curAttrsOrNull == null ? this._dateFormat.format(new Date(FileTime.from(0L, TimeUnit.SECONDS).toMillis()))
                                : this._dateFormat.format(new Date(FileTime.from(curAttrsOrNull.lastModifiedTime(), TimeUnit.SECONDS).toMillis())),
                        this._dateFormat.format(new Date(FileTime.from(newAttrs.lastModifiedTime(), TimeUnit.SECONDS).toMillis()))));
                
            }
            this._fileAttributeManager.setLastModifiedTime(path, newAttrs.lastModifiedTime(), LinkOption.NOFOLLOW_LINKS);
        }
        // NOTE: keep this one last in the method, in case we fail due to
        // insufficient permissions (the other ones are more likely to
        // succeed).
        // NOTE: we cannot detect if we have the capabilities to change
        // ownership (knowing if UID 0 is not sufficient)
        if (this._isPreserveUser) {
            if (!this._isNumericIds && !newAttrs.user().name().isEmpty() && (curAttrsOrNull == null || !curAttrsOrNull.user().name().equals(newAttrs.user().name()))) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("(Generator) %s: updating ownership %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.user(), newAttrs.user()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                this._fileAttributeManager.setOwner(path, newAttrs.user(), LinkOption.NOFOLLOW_LINKS);
            } else if ((this._isNumericIds || newAttrs.user().name().isEmpty()) && (curAttrsOrNull == null || curAttrsOrNull.user().id() != newAttrs.user().id())) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("(Generator) %s: updating ownership %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.user().id(), newAttrs.user().id()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                this._fileAttributeManager.setUserId(path, newAttrs.user().id(), LinkOption.NOFOLLOW_LINKS);
            }
        }
        
        if (this._isPreserveGroup) {
            if (!this._isNumericIds && !newAttrs.group().name().isEmpty() && (curAttrsOrNull == null || !curAttrsOrNull.group().name().equals(newAttrs.group().name()))) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("(Generator) %s: updating group %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.group(), newAttrs.group()));
                }
                this._fileAttributeManager.setGroup(path, newAttrs.group(), LinkOption.NOFOLLOW_LINKS);
            } else if ((this._isNumericIds || newAttrs.group().name().isEmpty()) && (curAttrsOrNull == null || curAttrsOrNull.group().id() != newAttrs.group().id())) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("(Generator) %s: updating gid %s -> %d", path, curAttrsOrNull == null ? "" : curAttrsOrNull.group().id(), newAttrs.group().id()));
                }
                this._fileAttributeManager.setGroupId(path, newAttrs.group().id(), LinkOption.NOFOLLOW_LINKS);
            }
        }
    }
}
