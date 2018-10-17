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
        private Charset charset;
        private final byte[] checksumSeed;
        private FileSelection fileSelection = FileSelection.EXACT;
        private boolean alwaysItemize;
        private boolean delete;
        private boolean ignoreTimes;
        private boolean interruptible = true;
        private boolean numericIds;
        private boolean preserveDevices;
        private boolean preserveGroup;
        private boolean preserveLinks;
        private boolean preservePermissions;
        private boolean preserveSpecials;
        private boolean preserveTimes;
        private boolean preserveUser;
        private final WritableByteChannel out;
        
        public Builder(WritableByteChannel out, byte[] checksumSeed) {
            assert out != null;
            assert checksumSeed != null;
            this.out = out;
            this.checksumSeed = checksumSeed;
        }
        
        public Generator build() {
            assert !this.delete || this.fileSelection != FileSelection.EXACT;
            return new Generator(this);
        }
        
        public Builder charset(Charset charset) {
            assert charset != null;
            this.charset = charset;
            return this;
        }
        
        public Builder fileSelection(FileSelection fileSelection) {
            assert fileSelection != null;
            this.fileSelection = fileSelection;
            return this;
        }
        
        public Builder isAlwaysItemize(boolean isAlwaysItemize) {
            this.alwaysItemize = isAlwaysItemize;
            return this;
        }
        
        public Builder isDelete(boolean isDelete) {
            this.delete = isDelete;
            return this;
        }
        
        public Builder isIgnoreTimes(boolean isIgnoreTimes) {
            this.ignoreTimes = isIgnoreTimes;
            return this;
        }
        
        public Builder isInterruptible(boolean isInterruptible) {
            this.interruptible = isInterruptible;
            return this;
        }
        
        public Builder isNumericIds(boolean isNumericIds) {
            this.numericIds = isNumericIds;
            return this;
        }
        
        public Builder isPreserveDevices(boolean isPreserveDevices) {
            this.preserveDevices = isPreserveDevices;
            return this;
        }
        
        public Builder isPreserveGroup(boolean isPreserveGroup) {
            this.preserveGroup = isPreserveGroup;
            return this;
        }
        
        public Builder isPreserveLinks(boolean isPreserveLinks) {
            this.preserveLinks = isPreserveLinks;
            return this;
        }
        
        public Builder isPreservePermissions(boolean isPreservePermissions) {
            this.preservePermissions = isPreservePermissions;
            return this;
        }
        
        public Builder isPreserveSpecials(boolean isPreserveSpecials) {
            this.preserveSpecials = isPreserveSpecials;
            return this;
        }
        
        public Builder isPreserveTimes(boolean isPreserveTimes) {
            this.preserveTimes = isPreserveTimes;
            return this;
        }
        
        public Builder isPreserveUser(boolean isPreserveUser) {
            this.preserveUser = isPreserveUser;
            return this;
        }
    }
    
    private interface Job {
        void process() throws RsyncException;
    }
    
    private static final Logger LOG = Logger.getLogger(Generator.class.getName());
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
    
    private final TextEncoder characterEncoder;
    private final byte[] checksumSeed;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat();
    private final Deque<Job> deferredJobs = new ArrayDeque<>();
    private FileAttributeManager fileAttributeManager;
    private final Filelist fileList;
    private final FileSelection fileSelection;
    private final List<Filelist.Segment> generated = new LinkedList<>();
    private final boolean alwaysItemize;
    private final boolean delete;
    private volatile boolean deletionsEnabled;
    private final boolean ignoreTimes;
    private final boolean interruptible;
    private final boolean numericIds;
    private final boolean preserveDevices;
    private final boolean preserveGroup;
    private final boolean preserveLinks;
    private final boolean preservePermissions;
    private final boolean preserveSpecials;
    
    private final boolean preserveTimes;
    private final boolean preserveUser;
    private boolean running = true;
    private final LinkedBlockingQueue<Job> jobs = new LinkedBlockingQueue<>();
    
    private final BlockingQueue<Pair<Boolean, FileInfo>> listing = new LinkedBlockingQueue<>();
    
    private final RsyncOutChannel out;
    
    private final BitSet pruned = new BitSet();
    
    private int returnStatus;
    
    private Generator(Builder builder) {
        this.checksumSeed = builder.checksumSeed;
        this.fileSelection = builder.fileSelection;
        this.fileList = new ConcurrentFilelist(this.fileSelection == FileSelection.RECURSE, true);
        this.out = new RsyncOutChannel(builder.out, OUTPUT_CHANNEL_BUF_SIZE);
        this.characterEncoder = TextEncoder.newStrict(builder.charset);
        this.alwaysItemize = builder.alwaysItemize;
        this.delete = builder.delete;
        this.deletionsEnabled = this.fileSelection != FileSelection.EXACT;
        this.ignoreTimes = builder.ignoreTimes;
        this.interruptible = builder.interruptible;
        this.preserveDevices = builder.preserveDevices;
        this.preserveLinks = builder.preserveLinks;
        this.preservePermissions = builder.preservePermissions;
        this.preserveSpecials = builder.preserveSpecials;
        this.preserveTimes = builder.preserveTimes;
        this.preserveUser = builder.preserveUser;
        this.preserveGroup = builder.preserveGroup;
        this.numericIds = builder.numericIds;
    }
    
    private void appendJob(Job job) throws InterruptedException {
        assert job != null;
        this.jobs.put(job);
    }
    
    @Override
    public Boolean call() throws InterruptedException, RsyncException {
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(this.toString());
            }
            this.processJobQueueBatched();
            return this.returnStatus == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            Pair<Boolean, FileInfo> poisonPill = new Pair<>(false, null);
            this.listing.add(poisonPill);
        }
    }
    
    Charset getCharset() {
        return this.characterEncoder.charset();
    }
    
    @Override
    public void closeChannel() throws ChannelException {
        this.out.close();
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
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    Generator.this.out.putMessage(Generator.this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    Generator.this.returnStatus++;
                }
            }
        };
        this.deferredJobs.addFirst(j);
    }
    
    RsyncFileAttributes deleteIfDifferentType(LocatableFileInfo fileInfo) throws IOException {
        // null if file does not exist; throws IOException on any other error
        RsyncFileAttributes curAttrsOrNull = this.fileAttributeManager.statIfExists(fileInfo.getPath());
        if (curAttrsOrNull != null && curAttrsOrNull.getFileType() != fileInfo.getAttributes().getFileType()) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("deleting %s %s (expecting a %s)", FileOps.fileType(curAttrsOrNull.getMode()), fileInfo.getPath(), FileOps.fileType(fileInfo.getAttributes().getMode())));
            }
            FileOps.unlink(fileInfo.getPath());
            return null;
        } else {
            return curAttrsOrNull;
        }
    }
    
    void disableDelete() {
        if (this.delete && this.deletionsEnabled) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning("--delete disabled due to receiving error " + "notification from peer sender");
            }
            this.deletionsEnabled = false;
        }
    }
    
    Filelist getFileList() {
        return this.fileList;
    }
    
    public BlockingQueue<Pair<Boolean, FileInfo>> getFiles() {
        return this.listing;
    }
    
    FileSelection getFileSelection() {
        return this.fileSelection;
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
                    String msg = String.format("failed to generate file meta " + "data for %s (index %d): %s", fileInfo.getPath(), fileIndex, e.getMessage());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    Generator.this.out.putMessage(Generator.this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    Generator.this.returnStatus++;
                }
            }
            
            @Override
            public String toString() {
                return String.format("generateFile (%s, %d, %s)", segment, fileIndex, fileInfo.getPath());
            }
        };
        this.appendJob(j);
    }
    
    void generateSegment(final Path targetPath, final Filelist.Segment segment, FilterRuleConfiguration filterRuleConfiguration) throws InterruptedException {
        assert segment != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this.sendChecksumForSegment(targetPath, segment, filterRuleConfiguration);
                Generator.this.generated.add(segment);
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
        return this.interruptible;
    }
    
    boolean isNumericIds() {
        return this.numericIds;
    }
    
    boolean isPreserveDevices() {
        return this.preserveDevices;
    }
    
    boolean isPreserveGroup() {
        return this.preserveGroup;
    }
    
    boolean isPreserveLinks() {
        return this.preserveLinks;
    }
    
    boolean isPreservePermissions() {
        return this.preservePermissions;
    }
    
    boolean isPreserveSpecials() {
        return this.preserveSpecials;
    }
    
    boolean isPreserveTimes() {
        return this.preserveTimes;
    }
    
    boolean isPreserveUser() {
        return this.preserveUser;
    }
    
    boolean isPruned(int index) {
        return this.pruned.get(index);
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
            this.sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.getAttributes(), Item.LOCAL_CHANGE);
            this.mkdir(fileInfo); // throws IOException
        } else {
            if (this.alwaysItemize) {
                this.sendItemizeInfo(index, curAttrsOrNull, fileInfo.getAttributes(), Item.NO_CHANGE);
            }
            if (!curAttrsOrNull.equals(fileInfo.getAttributes())) {
                this.deferUpdateAttrsIfDiffer(fileInfo.getPath(), curAttrsOrNull, fileInfo.getAttributes());
            }
        }
    }
    
    private boolean itemizeFile(int index, LocatableFileInfo fileInfo, int digestLength) throws ChannelException, IOException {
        assert fileInfo != null;
        
        RsyncFileAttributes curAttrsOrNull = this.deleteIfDifferentType(fileInfo);
        
        // NOTE: native opens the file first though even if its file size is
        // zero
        if (FileOps.isDataModified(curAttrsOrNull, fileInfo.getAttributes()) || this.ignoreTimes) {
            if (curAttrsOrNull == null) {
                this.sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.getAttributes(), Item.TRANSFER);
                this.sendChecksumHeader(ZERO_SUM);
            } else {
                this.sendItemizeAndChecksums(index, fileInfo, curAttrsOrNull, digestLength);
            }
            return true;
        }
        
        if (this.alwaysItemize) {
            this.sendItemizeInfo(index, curAttrsOrNull, fileInfo.getAttributes(), Item.NO_CHANGE);
        }
        
        try {
            this.updateAttrsIfDiffer(fileInfo.getPath(), curAttrsOrNull, fileInfo.getAttributes());
        } catch (IOException e) {
            String msg = String.format("received an I/O error while applying attributes on %s: %s", fileInfo.getPath(), e.getMessage());
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(msg);
            }
            this.out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            this.returnStatus++;
        }
        return false;
    }
    
    private char itemizeFlags(RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs) {
        assert newAttrs != null;
        
        if (curAttrsOrNull == null) {
            return Item.IS_NEW;
        }
        char iFlags = Item.NO_CHANGE;
        if (this.preservePermissions && curAttrsOrNull.getMode() != newAttrs.getMode()) {
            iFlags |= Item.REPORT_PERMS;
        }
        if (this.preserveTimes && curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime()) {
            iFlags |= Item.REPORT_TIME;
        }
        if (this.preserveUser && !curAttrsOrNull.getUser().equals(newAttrs.getUser())) {
            iFlags |= Item.REPORT_OWNER;
        }
        if (this.preserveGroup && !curAttrsOrNull.getGroup().equals(newAttrs.getGroup())) {
            iFlags |= Item.REPORT_GROUP;
        }
        if (curAttrsOrNull.isRegularFile() && curAttrsOrNull.getSize() != newAttrs.getSize()) {
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
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("(Generator) generating %s, index %d", lf.getPath(), index));
                    }
                    if (f.getAttributes().isRegularFile()) {
                        isTransfer = this.itemizeFile(index, lf, Checksum.MIN_DIGEST_LENGTH);
                    } else if (f.getAttributes().isDirectory()) {
                        if (this.fileSelection != FileSelection.RECURSE) {
                            this.itemizeDirectory(index, lf);
                        }
                    } else if (f instanceof LocatableDeviceInfo && this.preserveDevices && (f.getAttributes().isBlockDevice() || f.getAttributes().isCharacterDevice())) {
                        this.itemizeDevice(index, (LocatableDeviceInfo) f);
                    } else if (f instanceof LocatableDeviceInfo && this.preserveSpecials && (f.getAttributes().isFifo() || f.getAttributes().isSocket())) {
                        this.itemizeDevice(index, (LocatableDeviceInfo) f);
                    } else if (this.preserveLinks && f instanceof LocatableSymlinkInfo) {
                        this.itemizeSymlink(index, (LocatableSymlinkInfo) f);
                    } else {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("(Generator) Skipping " + lf.getPath());
                        }
                    }
                } catch (IOException e) {
                    if (lf.getAttributes().isDirectory()) {
                        // we cannot remove the corresponding segment since we
                        // may not have received it yet
                        this.prune(index);
                    }
                    String msg = String.format("failed to generate %s %s (index %d): %s", FileOps.fileTypeToString(lf.getAttributes().getMode()), lf.getPath(), index, e.getMessage());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    this.out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    numErrors++;
                }
            } else {
                if (LOG.isLoggable(Level.WARNING)) {
                    LOG.warning("skipping untransferrable " + f);
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
                Path curTarget = Files.readSymbolicLink(linkInfo.getPath());
                if (curTarget.toString().equals(linkInfo.getTargetPathName())) {
                    if (this.alwaysItemize) {
                        this.sendItemizeInfo(index, curAttrsOrNull, linkInfo.getAttributes(), Item.NO_CHANGE);
                    }
                    return;
                }
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("deleting symlink %s -> %s", linkInfo.getPath(), curTarget));
                }
                FileOps.unlink(linkInfo.getPath());
            }
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("creating symlink %s -> %s", linkInfo.getPath(), linkInfo.getTargetPathName()));
            }
            Path targetPath = linkInfo.getPath().getFileSystem().getPath(linkInfo.getTargetPathName());
            Files.createSymbolicLink(linkInfo.getPath(), targetPath);
            
            this.sendItemizeInfo(index, null /* curAttrsOrNull */, linkInfo.getAttributes(), (char) (Item.LOCAL_CHANGE | Item.REPORT_CHANGE));
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
                if (Generator.this.fileSelection != FileSelection.RECURSE) {
                    c = segment.getFiles();
                } else if (segment.getDirectory() == null) {
                    c = Generator.this.toInitialListing(segment);
                } else {
                    c = Generator.this.toListing(segment);
                }
                Generator.this.listing.addAll(Generator.this.toListingPair(c));
                segment.removeAll();
                Filelist.Segment deleted = Generator.this.fileList.deleteFirstSegment();
                if (deleted != segment) {
                    throw new IllegalStateException(String.format("%s != %s", deleted, segment));
                }
                Generator.this.out.encodeIndex(Filelist.DONE);
                
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
        
        RsyncFileAttributes attrs = this.fileAttributeManager.statOrNull(dir.getPath());
        if (attrs == null) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("(Generator) creating directory " + dir.getPath());
            }
            Files.createDirectories(dir.getPath());
        }
        this.deferUpdateAttrsIfDiffer(dir.getPath(), attrs, dir.getAttributes());
    }
    
    synchronized long getNumBytesWritten() {
        return this.out.getNumBytesWritten();
    }
    
    void processDeferredJobs() throws InterruptedException {
        Job job = new Job() {
            @Override
            public void process() throws RsyncException {
                for (Job j : Generator.this.deferredJobs) {
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
        while (this.running) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("(Generator) awaiting next jobs...");
            }
            
            jobList.add(this.jobs.take());
            this.jobs.drainTo(jobList);
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) got %d job(s)", jobList.size()));
            }
            
            for (Job job : jobList) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("(Generator) processing " + job);
                }
                job.process();
            }
            jobList.clear();
            if (this.jobs.isEmpty()) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) flushing %d bytes", this.out.getNumBytesBuffered()));
                }
                this.out.flush();
            }
        }
    }
    
    void prune(int index) {
        this.pruned.set(index);
    }
    
    void purgeFile(final Filelist.Segment segment, final int index) throws InterruptedException {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException, RsyncProtocolException {
                if (segment != null) {
                    segment.remove(index);
                } else {
                    Filelist.Segment tmpSegment = Generator.this.fileList.getSegmentWith(index);
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
        for (Iterator<Filelist.Segment> it = this.generated.iterator(); it.hasNext();) {
            Filelist.Segment segment = it.next();
            if (!segment.isFinished()) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s not finished yet", segment));
                }
                break;
            }
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) removing finished " + "segment %s and sending index %d", segment, Filelist.DONE));
            }
            Filelist.Segment deleted = this.fileList.deleteFirstSegment();
            // identity comparison
            if (deleted != segment) {
                throw new IllegalStateException(String.format("%s != %s", deleted, segment));
            }
            // NOTE: remove before notifying peer
            it.remove();
            this.out.encodeIndex(Filelist.DONE);
        }
    }
    
    // used for sending empty filter rules only
    void sendBytes(final ByteBuffer buf) throws InterruptedException {
        assert buf != null;
        
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this.out.put(buf);
            }
            
            @Override
            public String toString() {
                return String.format("sendBytes(%s)", buf.duplicate());
            }
        };
        this.appendJob(j);
    }
    
    private void sendChecksumForSegment(final Path targetPath, Filelist.Segment segment, FilterRuleConfiguration filterRuleConfiguration) throws ChannelException {
        assert segment != null;
        
        final int dirIndex = segment.getDirectoryIndex();
        if (segment.getDirectory() != null && !(segment.getDirectory() instanceof LocatableFileInfo)) {
            segment.removeAll();
            return;
        }
        LocatableFileInfo dir = (LocatableFileInfo) segment.getDirectory();
        if (dir != null && (this.isPruned(dirIndex) || dir.getPath() == null)) {
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
        if (this.delete && this.deletionsEnabled) {
            try {
                this.unlinkFilesInDirNotAtSender(targetPath, dir.getPath(), segment.getFiles(), filterRuleConfiguration);
            } catch (IOException e) {
                if (Files.exists(dir.getPath(), LinkOption.NOFOLLOW_LINKS)) {
                    String msg = String.format("failed to delete %s and all " + "its files: %s", dir, e);
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    this.out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    this.returnStatus++;
                }
            }
        }
        try {
            if (dir.getAttributes().isDirectory()) {
                this.mkdir(dir);
            }
            if (!isInitialFileList) {
                this.itemizeDirectory(dirIndex, dir);
            }
            this.returnStatus += this.itemizeSegment(segment);
        } catch (IOException e) {
            String msg = String.format("failed to generate files below dir %s (index %d): %s", dir.getPath(), dirIndex, e.getMessage());
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(msg);
            }
            this.out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            segment.removeAll();
            this.returnStatus++;
        }
    }
    
    private void sendChecksumHeader(Checksum.Header header) throws ChannelException {
        Connection.sendChecksumHeader(this.out, header);
    }
    
    private void sendItemizeAndChecksums(int index, LocatableFileInfo fileInfo, RsyncFileAttributes curAttrs, int minDigestLength) throws ChannelException {
        assert fileInfo != null;
        assert curAttrs != null;
        
        long currentSize = curAttrs.getSize();
        int blockLength = getBlockLengthFor(currentSize);
        int windowLength = blockLength;
        int digestLength = currentSize > 0 ? Math.max(minDigestLength, getDigestLength(currentSize, blockLength)) : 0;
        // new FileView() throws FileViewOpenFailed
        try (FileView fv = new FileView(fileInfo.getPath(), currentSize, blockLength, windowLength)) {
            
            // throws ChunkCountOverflow
            Checksum.Header header = new Checksum.Header(blockLength, digestLength, currentSize);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) generating file %s, " + "index %d, checksum %s", fileInfo, index, header));
            }
            
            this.sendItemizeInfo(index, curAttrs, fileInfo.getAttributes(), Item.TRANSFER);
            this.sendChecksumHeader(header);
            
            MessageDigest md = MD5.newInstance();
            
            while (fv.getWindowLength() > 0) {
                int rolling = Rolling.compute(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
                this.out.putInt(rolling);
                md.update(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
                md.update(this.checksumSeed);
                this.out.put(md.digest(), 0, digestLength);
                fv.slide(fv.getWindowLength());
            }
        } catch (FileViewOpenFailed | Checksum.ChunkOverflow e) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(String.format("(Generator) received I/O error during checksum " + "generation (%s)", e.getMessage()));
            }
            this.sendItemizeInfo(index, null, fileInfo.getAttributes(), Item.TRANSFER);
            this.sendChecksumHeader(ZERO_SUM);
        } catch (FileViewException e) {
            // occurs at FileView.close() - if there were any I/O errors during
            // file read
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning("(Generator) Warning got I/O errors during " + "checksum generation. Errors ignored and data " + "filled with zeroes): " + e.getMessage());
            }
        }
    }
    
    private void sendItemizeInfo(int index, RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs, char iMask) throws ChannelException {
        assert newAttrs != null;
        char iFlags = (char) (iMask | this.itemizeFlags(curAttrsOrNull, newAttrs));
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("(Generator) sending itemizeFlags=" + (int) iFlags);
        }
        this.out.encodeIndex(index);
        this.out.putChar(iFlags);
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
                Generator.this.out.putMessage(message);
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
                Generator.this.out.encodeIndex(Filelist.DONE);
            }
            
            @Override
            public String toString() {
                return "sendSegmentDone()";
            }
        };
        this.appendJob(j);
    }
    
    synchronized void setFileAttributeManager(FileAttributeManager fileAttributeManager) {
        this.fileAttributeManager = fileAttributeManager;
    }
    
    void stop() throws InterruptedException {
        Job job = new Job() {
            @Override
            public void process() {
                Generator.this.running = false;
            }
            
            @Override
            public String toString() {
                return "stop()";
            }
        };
        this.appendJob(job);
    }
    
    private Collection<FileInfo> toInitialListing(Filelist.Segment segment) {
        assert this.fileSelection == FileSelection.RECURSE;
        assert segment.getDirectory() == null;
        boolean listFirstDotDir = true;
        Collection<FileInfo> res = new ArrayList<>(segment.getFiles().size());
        for (FileInfo f : segment.getFiles()) {
            if (!f.getAttributes().isDirectory()) {
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
        assert this.fileSelection == FileSelection.RECURSE;
        assert segment.getDirectory() != null;
        Collection<FileInfo> res = new ArrayList<>(segment.getFiles().size());
        res.add(segment.getDirectory());
        for (FileInfo f : segment.getFiles()) {
            if (!f.getAttributes().isDirectory()) {
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
        ByteBuffer payload = ByteBuffer.wrap(this.characterEncoder.encode(text));
        return new Message(code, payload);
    }
    
    @Override
    public String toString() {
        return String.format("%s(" + "isAlwaysItemize=%b, " + "isDelete=%b, " + "isIgnoreTimes=%b, " + "isInterruptible=%b, " + "isNumericIds=%b, " + "isPreserveDevices=%b, " + "isPreserveLinks=%b, "
                + "isPreservePermissions=%b, " + "isPreserveSpecials=%b, " + "isPreserveTimes=%b, " + "isPreserveUser=%b, " + "isPreserveGroup=%b, " + "checksumSeed=%s, " + "fileSelection=%s" + ")",
                this.getClass().getSimpleName(), this.alwaysItemize, this.delete, this.ignoreTimes, this.interruptible, this.numericIds, this.preserveDevices, this.preserveLinks,
                this.preservePermissions, this.preserveSpecials, this.preserveTimes, this.preserveUser, this.preserveGroup, Text.bytesToString(this.checksumSeed), this.fileSelection);
    }
    
    private void unlinkFilesInDirNotAtSender(final Path targetPath, Path dir, Collection<FileInfo> files, FilterRuleConfiguration cfg) throws IOException, ChannelException {
        assert this.delete && this.deletionsEnabled;

        Set<Path> senderPaths = new HashSet<>(files.size());
        for (FileInfo f : files) {
            if (f instanceof LocatableFileInfo) {
                LocatableFileInfo lf = (LocatableFileInfo) f;
                senderPaths.add(lf.getPath());
            }
        }
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (!senderPaths.contains(entry)) {
                    boolean isDirectory = Files.isDirectory(entry);
                    // detect protection
                    String filename = "./" + targetPath.relativize(entry).normalize().toString();
                    if (cfg.protect(filename, isDirectory)) {
                        System.err.println("Entry is proteced from deletion...");
                        continue;
                    }
                    // detect exclusion, TODO: check path conversion
                    boolean isEntryExcluded = cfg.exclude(filename, isDirectory);
                    
                    if (!isEntryExcluded) {
                        try {
                            if (LOG.isLoggable(Level.INFO)) {
                                LOG.info("deleting extraneous " + entry);
                            }
                            FileOps.unlink(entry);
                        } catch (IOException e) {
                            String msg = String.format("failed to delete %s: %s", entry, e);
                            if (LOG.isLoggable(Level.WARNING)) {
                                LOG.warning(msg);
                            }
                            this.out.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                            this.returnStatus++;
                            
                        }
                    } else {
                        System.err.println("Entry is excluded from deletion...");
                    }
                }
            }
        }
    }
    
    private void updateAttrsIfDiffer(Path path, RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs) throws IOException {
        assert path != null;
        assert newAttrs != null;
        
        if (this.preservePermissions && (curAttrsOrNull == null || curAttrsOrNull.getMode() != newAttrs.getMode())) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) %s: updating mode %o -> %o", path, curAttrsOrNull == null ? 0 : curAttrsOrNull.getMode(), newAttrs.getMode()));
            }
            this.fileAttributeManager.setFileMode(path, newAttrs.getMode(), LinkOption.NOFOLLOW_LINKS);
        }
        if (this.preserveTimes && (curAttrsOrNull == null || curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime())) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) %s: updating mtime %s -> %s", path,
                        curAttrsOrNull == null ? this.dateFormat.format(new Date(FileTime.from(0L, TimeUnit.SECONDS).toMillis()))
                                : this.dateFormat.format(new Date(FileTime.from(curAttrsOrNull.lastModifiedTime(), TimeUnit.SECONDS).toMillis())),
                        this.dateFormat.format(new Date(FileTime.from(newAttrs.lastModifiedTime(), TimeUnit.SECONDS).toMillis()))));
                
            }
            this.fileAttributeManager.setLastModifiedTime(path, newAttrs.lastModifiedTime(), LinkOption.NOFOLLOW_LINKS);
        }
        // NOTE: keep this one last in the method, in case we fail due to
        // insufficient permissions (the other ones are more likely to
        // succeed).
        // NOTE: we cannot detect if we have the capabilities to change
        // ownership (knowing if UID 0 is not sufficient)
        if (this.preserveUser) {
            if (!this.numericIds && !newAttrs.getUser().getName().isEmpty() && (curAttrsOrNull == null || !curAttrsOrNull.getUser().getName().equals(newAttrs.getUser().getName()))) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating ownership %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getUser(), newAttrs.getUser()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                this.fileAttributeManager.setOwner(path, newAttrs.getUser(), LinkOption.NOFOLLOW_LINKS);
            } else if ((this.numericIds || newAttrs.getUser().getName().isEmpty()) && (curAttrsOrNull == null || curAttrsOrNull.getUser().getId() != newAttrs.getUser().getId())) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating ownership %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getUser().getId(), newAttrs.getUser().getId()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                this.fileAttributeManager.setUserId(path, newAttrs.getUser().getId(), LinkOption.NOFOLLOW_LINKS);
            }
        }
        
        if (this.preserveGroup) {
            if (!this.numericIds && !newAttrs.getGroup().getName().isEmpty() && (curAttrsOrNull == null || !curAttrsOrNull.getGroup().getName().equals(newAttrs.getGroup().getName()))) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating group %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getGroup(), newAttrs.getGroup()));
                }
                this.fileAttributeManager.setGroup(path, newAttrs.getGroup(), LinkOption.NOFOLLOW_LINKS);
            } else if ((this.numericIds || newAttrs.getGroup().getName().isEmpty()) && (curAttrsOrNull == null || curAttrsOrNull.getGroup().getId() != newAttrs.getGroup().getId())) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating gid %s -> %d", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getGroup().getId(), newAttrs.getGroup().getId()));
                }
                this.fileAttributeManager.setGroupId(path, newAttrs.getGroup().getId(), LinkOption.NOFOLLOW_LINKS);
            }
        }
    }
}
