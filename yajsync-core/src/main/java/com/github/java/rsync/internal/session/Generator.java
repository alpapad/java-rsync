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
package com.github.java.rsync.internal.session;

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

import com.github.java.rsync.FileSelection;
import com.github.java.rsync.RsyncException;
import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.attr.LocatableDeviceInfo;
import com.github.java.rsync.attr.LocatableFileInfo;
import com.github.java.rsync.attr.LocatableSymlinkInfo;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.channels.Message;
import com.github.java.rsync.internal.channels.MessageCode;
import com.github.java.rsync.internal.channels.RsyncOutChannel;
import com.github.java.rsync.internal.io.FileView;
import com.github.java.rsync.internal.io.FileViewException;
import com.github.java.rsync.internal.io.FileViewOpenFailed;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.text.TextConversionException;
import com.github.java.rsync.internal.text.TextEncoder;
import com.github.java.rsync.internal.util.FileOps;
import com.github.java.rsync.internal.util.MD5;
import com.github.java.rsync.internal.util.Pair;
import com.github.java.rsync.internal.util.Rolling;
import com.github.java.rsync.internal.util.RuntimeInterruptException;
import com.github.java.rsync.internal.util.Util;

public class Generator implements RsyncTask {
    public static class Builder {
        private boolean alwaysItemize;
        private Charset charset;
        private final byte[] checksumSeed;
        private boolean delete;
        private FileSelection fileSelection = FileSelection.EXACT;
        private boolean ignoreTimes;
        private boolean interruptible = true;
        private boolean numericIds;
        private final WritableByteChannel out;
        private boolean preserveDevices;
        private boolean preserveGroup;
        private boolean preserveLinks;
        private boolean preservePermissions;
        private boolean preserveSpecials;
        private boolean preserveTimes;
        private boolean preserveUser;

        public Builder(WritableByteChannel out, byte[] checksumSeed) {
            assert out != null;
            assert checksumSeed != null;
            this.out = out;
            this.checksumSeed = checksumSeed;
        }

        public Generator build() {
            assert !delete || fileSelection != FileSelection.EXACT;
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
            alwaysItemize = isAlwaysItemize;
            return this;
        }

        public Builder isDelete(boolean isDelete) {
            delete = isDelete;
            return this;
        }

        public Builder isIgnoreTimes(boolean isIgnoreTimes) {
            ignoreTimes = isIgnoreTimes;
            return this;
        }

        public Builder isInterruptible(boolean isInterruptible) {
            interruptible = isInterruptible;
            return this;
        }

        public Builder isNumericIds(boolean isNumericIds) {
            numericIds = isNumericIds;
            return this;
        }

        public Builder isPreserveDevices(boolean isPreserveDevices) {
            preserveDevices = isPreserveDevices;
            return this;
        }

        public Builder isPreserveGroup(boolean isPreserveGroup) {
            preserveGroup = isPreserveGroup;
            return this;
        }

        public Builder isPreserveLinks(boolean isPreserveLinks) {
            preserveLinks = isPreserveLinks;
            return this;
        }

        public Builder isPreservePermissions(boolean isPreservePermissions) {
            preservePermissions = isPreservePermissions;
            return this;
        }

        public Builder isPreserveSpecials(boolean isPreserveSpecials) {
            preserveSpecials = isPreserveSpecials;
            return this;
        }

        public Builder isPreserveTimes(boolean isPreserveTimes) {
            preserveTimes = isPreserveTimes;
            return this;
        }

        public Builder isPreserveUser(boolean isPreserveUser) {
            preserveUser = isPreserveUser;
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

    private final boolean alwaysItemize;
    private final TextEncoder characterEncoder;
    private final byte[] checksumSeed;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat();
    private final Deque<Job> deferredJobs = new ArrayDeque<>();
    private final boolean delete;
    private volatile boolean deletionsEnabled;
    private FileAttributeManager fileAttributeManager;
    private final Filelist fileList;
    private final FileSelection fileSelection;
    private final List<Filelist.Segment> generated = new LinkedList<>();
    private final boolean ignoreTimes;
    private final boolean interruptible;
    private final LinkedBlockingQueue<Job> jobs = new LinkedBlockingQueue<>();
    private final BlockingQueue<Pair<Boolean, FileInfo>> listing = new LinkedBlockingQueue<>();
    private final boolean numericIds;
    private final RsyncOutChannel out;
    private final boolean preserveDevices;
    private final boolean preserveGroup;

    private final boolean preserveLinks;
    private final boolean preservePermissions;
    private final boolean preserveSpecials;
    private final boolean preserveTimes;

    private final boolean preserveUser;

    private final BitSet pruned = new BitSet();

    private int returnStatus;

    private boolean running = true;

    private Generator(Builder builder) {
        checksumSeed = builder.checksumSeed;
        fileSelection = builder.fileSelection;
        fileList = new ConcurrentFilelist(fileSelection == FileSelection.RECURSE, true);
        out = new RsyncOutChannel(builder.out, OUTPUT_CHANNEL_BUF_SIZE);
        characterEncoder = TextEncoder.newStrict(builder.charset);
        alwaysItemize = builder.alwaysItemize;
        delete = builder.delete;
        deletionsEnabled = fileSelection != FileSelection.EXACT;
        ignoreTimes = builder.ignoreTimes;
        interruptible = builder.interruptible;
        preserveDevices = builder.preserveDevices;
        preserveLinks = builder.preserveLinks;
        preservePermissions = builder.preservePermissions;
        preserveSpecials = builder.preserveSpecials;
        preserveTimes = builder.preserveTimes;
        preserveUser = builder.preserveUser;
        preserveGroup = builder.preserveGroup;
        numericIds = builder.numericIds;
    }

    private void appendJob(Job job) throws InterruptedException {
        assert job != null;
        jobs.put(job);
    }

    @Override
    public Boolean call() throws InterruptedException, RsyncException {
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(toString());
            }
            processJobQueueBatched();
            return returnStatus == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            Pair<Boolean, FileInfo> poisonPill = new Pair<>(false, null);
            listing.add(poisonPill);
        }
    }

    @Override
    public void closeChannel() throws ChannelException {
        out.close();
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
                    out.putMessage(Generator.this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    returnStatus++;
                }
            }
        };
        deferredJobs.addFirst(j);
    }

    RsyncFileAttributes deleteIfDifferentType(LocatableFileInfo fileInfo) throws IOException {
        // null if file does not exist; throws IOException on any other error
        RsyncFileAttributes curAttrsOrNull = fileAttributeManager.statIfExists(fileInfo.getPath());
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
        if (delete && deletionsEnabled) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning("--delete disabled due to receiving error " + "notification from peer sender");
            }
            deletionsEnabled = false;
        }
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
                    out.putMessage(Generator.this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    returnStatus++;
                }
            }

            @Override
            public String toString() {
                return String.format("generateFile (%s, %d, %s)", segment, fileIndex, fileInfo.getPath());
            }
        };
        appendJob(j);
    }

    void generateSegment(final Path targetPath, final Filelist.Segment segment, FilterRuleConfiguration filterRuleConfiguration) throws InterruptedException {
        assert segment != null;

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                Generator.this.sendChecksumForSegment(targetPath, segment, filterRuleConfiguration);
                generated.add(segment);
                Generator.this.removeAllFinishedSegmentsAndNotifySender();
            }

            @Override
            public String toString() {
                return String.format("generateSegment(%s)", segment);
            }
        };
        appendJob(j);
    }

    Charset getCharset() {
        return characterEncoder.charset();
    }

    Filelist getFileList() {
        return fileList;
    }

    public BlockingQueue<Pair<Boolean, FileInfo>> getFiles() {
        return listing;
    }

    FileSelection getFileSelection() {
        return fileSelection;
    }

    synchronized long getNumBytesWritten() {
        return out.getNumBytesWritten();
    }

    @Override
    public boolean isInterruptible() {
        return interruptible;
    }

    boolean isNumericIds() {
        return numericIds;
    }

    boolean isPreserveDevices() {
        return preserveDevices;
    }

    boolean isPreserveGroup() {
        return preserveGroup;
    }

    boolean isPreserveLinks() {
        return preserveLinks;
    }

    boolean isPreservePermissions() {
        return preservePermissions;
    }

    boolean isPreserveSpecials() {
        return preserveSpecials;
    }

    boolean isPreserveTimes() {
        return preserveTimes;
    }

    boolean isPreserveUser() {
        return preserveUser;
    }

    boolean isPruned(int index) {
        return pruned.get(index);
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

        RsyncFileAttributes curAttrsOrNull = deleteIfDifferentType(fileInfo);
        if (curAttrsOrNull == null) {
            sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.getAttributes(), Item.LOCAL_CHANGE);
            mkdir(fileInfo); // throws IOException
        } else {
            if (alwaysItemize) {
                sendItemizeInfo(index, curAttrsOrNull, fileInfo.getAttributes(), Item.NO_CHANGE);
            }
            if (!curAttrsOrNull.equals(fileInfo.getAttributes())) {
                deferUpdateAttrsIfDiffer(fileInfo.getPath(), curAttrsOrNull, fileInfo.getAttributes());
            }
        }
    }

    private boolean itemizeFile(int index, LocatableFileInfo fileInfo, int digestLength) throws ChannelException, IOException {
        assert fileInfo != null;

        RsyncFileAttributes curAttrsOrNull = deleteIfDifferentType(fileInfo);

        // NOTE: native opens the file first though even if its file size is
        // zero
        if (FileOps.isDataModified(curAttrsOrNull, fileInfo.getAttributes()) || ignoreTimes) {
            if (curAttrsOrNull == null) {
                sendItemizeInfo(index, null /* curAttrsOrNull */, fileInfo.getAttributes(), Item.TRANSFER);
                sendChecksumHeader(ZERO_SUM);
            } else {
                sendItemizeAndChecksums(index, fileInfo, curAttrsOrNull, digestLength);
            }
            return true;
        }

        if (alwaysItemize) {
            sendItemizeInfo(index, curAttrsOrNull, fileInfo.getAttributes(), Item.NO_CHANGE);
        }

        try {
            updateAttrsIfDiffer(fileInfo.getPath(), curAttrsOrNull, fileInfo.getAttributes());
        } catch (IOException e) {
            String msg = String.format("received an I/O error while applying attributes on %s: %s", fileInfo.getPath(), e.getMessage());
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(msg);
            }
            out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            returnStatus++;
        }
        return false;
    }

    private char itemizeFlags(RsyncFileAttributes curAttrsOrNull, RsyncFileAttributes newAttrs) {
        assert newAttrs != null;

        if (curAttrsOrNull == null) {
            return Item.IS_NEW;
        }
        char iFlags = Item.NO_CHANGE;
        if (preservePermissions && curAttrsOrNull.getMode() != newAttrs.getMode()) {
            iFlags |= Item.REPORT_PERMS;
        }
        if (preserveTimes && curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime()) {
            iFlags |= Item.REPORT_TIME;
        }
        if (preserveUser && !curAttrsOrNull.getUser().equals(newAttrs.getUser())) {
            iFlags |= Item.REPORT_OWNER;
        }
        if (preserveGroup && !curAttrsOrNull.getGroup().equals(newAttrs.getGroup())) {
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
                        isTransfer = itemizeFile(index, lf, Checksum.MIN_DIGEST_LENGTH);
                    } else if (f.getAttributes().isDirectory()) {
                        if (fileSelection != FileSelection.RECURSE) {
                            itemizeDirectory(index, lf);
                        }
                    } else if (f instanceof LocatableDeviceInfo && preserveDevices && (f.getAttributes().isBlockDevice() || f.getAttributes().isCharacterDevice())) {
                        itemizeDevice(index, (LocatableDeviceInfo) f);
                    } else if (f instanceof LocatableDeviceInfo && preserveSpecials && (f.getAttributes().isFifo() || f.getAttributes().isSocket())) {
                        itemizeDevice(index, (LocatableDeviceInfo) f);
                    } else if (preserveLinks && f instanceof LocatableSymlinkInfo) {
                        itemizeSymlink(index, (LocatableSymlinkInfo) f);
                    } else {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("(Generator) Skipping " + lf.getPath());
                        }
                    }
                } catch (IOException e) {
                    if (lf.getAttributes().isDirectory()) {
                        // we cannot remove the corresponding segment since we
                        // may not have received it yet
                        prune(index);
                    }
                    String msg = String.format("failed to generate %s %s (index %d): %s", FileOps.fileTypeToString(lf.getAttributes().getMode()), lf.getPath(), index, e.getMessage());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
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
            RsyncFileAttributes curAttrsOrNull = deleteIfDifferentType(linkInfo);
            if (curAttrsOrNull != null) {
                Path curTarget = Files.readSymbolicLink(linkInfo.getPath());
                if (curTarget.toString().equals(linkInfo.getTargetPathName())) {
                    if (alwaysItemize) {
                        sendItemizeInfo(index, curAttrsOrNull, linkInfo.getAttributes(), Item.NO_CHANGE);
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

            sendItemizeInfo(index, null /* curAttrsOrNull */, linkInfo.getAttributes(), (char) (Item.LOCAL_CHANGE | Item.REPORT_CHANGE));
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
                if (fileSelection != FileSelection.RECURSE) {
                    c = segment.getFiles();
                } else if (segment.getDirectory() == null) {
                    c = Generator.this.toInitialListing(segment);
                } else {
                    c = Generator.this.toListing(segment);
                }
                listing.addAll(Generator.this.toListingPair(c));
                segment.removeAll();
                Filelist.Segment deleted = fileList.deleteFirstSegment();
                if (deleted != segment) {
                    throw new IllegalStateException(String.format("%s != %s", deleted, segment));
                }
                out.encodeIndex(Filelist.DONE);

            }

            @Override
            public String toString() {
                return String.format("listSegment(%s)", segment);
            }
        };
        appendJob(j);
    }

    // NOTE: no error if dir already exists
    private void mkdir(LocatableFileInfo dir) throws IOException {
        assert dir != null;

        RsyncFileAttributes attrs = fileAttributeManager.statOrNull(dir.getPath());
        if (attrs == null) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("(Generator) creating directory " + dir.getPath());
            }
            Files.createDirectories(dir.getPath());
        }
        deferUpdateAttrsIfDiffer(dir.getPath(), attrs, dir.getAttributes());
    }

    void processDeferredJobs() throws InterruptedException {
        Job job = new Job() {
            @Override
            public void process() throws RsyncException {
                for (Job j : deferredJobs) {
                    j.process();
                }
            }

            @Override
            public String toString() {
                return "processDeferredJobs()";
            }
        };
        appendJob(job);
    }

    private void processJobQueueBatched() throws InterruptedException, RsyncException {
        List<Job> jobList = new LinkedList<>();
        while (running) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("(Generator) awaiting next jobs...");
            }

            jobList.add(jobs.take());
            jobs.drainTo(jobList);

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
            if (jobs.isEmpty()) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) flushing %d bytes", out.getNumBytesBuffered()));
                }
                out.flush();
            }
        }
    }

    void prune(int index) {
        pruned.set(index);
    }

    void purgeFile(final Filelist.Segment segment, final int index) throws InterruptedException {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException, RsyncProtocolException {
                if (segment != null) {
                    segment.remove(index);
                } else {
                    Filelist.Segment tmpSegment = fileList.getSegmentWith(index);
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
        appendJob(j);
    }

    private void removeAllFinishedSegmentsAndNotifySender() throws ChannelException {
        for (Iterator<Filelist.Segment> it = generated.iterator(); it.hasNext();) {
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
            Filelist.Segment deleted = fileList.deleteFirstSegment();
            // identity comparison
            if (deleted != segment) {
                throw new IllegalStateException(String.format("%s != %s", deleted, segment));
            }
            // NOTE: remove before notifying peer
            it.remove();
            out.encodeIndex(Filelist.DONE);
        }
    }

    // used for sending empty filter rules only
    void sendBytes(final ByteBuffer buf) throws InterruptedException {
        assert buf != null;

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                out.put(buf);
            }

            @Override
            public String toString() {
                return String.format("sendBytes(%s)", buf.duplicate());
            }
        };
        appendJob(j);
    }

    private void sendChecksumForSegment(final Path targetPath, Filelist.Segment segment, FilterRuleConfiguration filterRuleConfiguration) throws ChannelException {
        assert segment != null;

        final int dirIndex = segment.getDirectoryIndex();
        if (segment.getDirectory() != null && !(segment.getDirectory() instanceof LocatableFileInfo)) {
            segment.removeAll();
            return;
        }
        LocatableFileInfo dir = (LocatableFileInfo) segment.getDirectory();
        if (dir != null && (isPruned(dirIndex) || dir.getPath() == null)) {
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
        if (delete && deletionsEnabled) {
            try {
                unlinkFilesInDirNotAtSender(targetPath, dir.getPath(), segment.getFiles(), filterRuleConfiguration);
            } catch (IOException e) {
                if (Files.exists(dir.getPath(), LinkOption.NOFOLLOW_LINKS)) {
                    String msg = String.format("failed to delete %s and all " + "its files: %s", dir, e);
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    returnStatus++;
                }
            }
        }
        try {
            if (dir.getAttributes().isDirectory()) {
                mkdir(dir);
            }
            if (!isInitialFileList) {
                itemizeDirectory(dirIndex, dir);
            }
            returnStatus += itemizeSegment(segment);
        } catch (IOException e) {
            String msg = String.format("failed to generate files below dir %s (index %d): %s", dir.getPath(), dirIndex, e.getMessage());
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(msg);
            }
            out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            segment.removeAll();
            returnStatus++;
        }
    }

    private void sendChecksumHeader(Checksum.Header header) throws ChannelException {
        Connection.sendChecksumHeader(out, header);
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

            sendItemizeInfo(index, curAttrs, fileInfo.getAttributes(), Item.TRANSFER);
            sendChecksumHeader(header);

            MessageDigest md = MD5.newInstance();

            while (fv.getWindowLength() > 0) {
                int rolling = Rolling.compute(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
                out.putInt(rolling);
                md.update(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
                md.update(checksumSeed);
                out.put(md.digest(), 0, digestLength);
                fv.slide(fv.getWindowLength());
            }
        } catch (FileViewOpenFailed | Checksum.ChunkOverflow e) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(String.format("(Generator) received I/O error during checksum " + "generation (%s)", e.getMessage()));
            }
            sendItemizeInfo(index, null, fileInfo.getAttributes(), Item.TRANSFER);
            sendChecksumHeader(ZERO_SUM);
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
        char iFlags = (char) (iMask | itemizeFlags(curAttrsOrNull, newAttrs));
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("(Generator) sending itemizeFlags=" + (int) iFlags);
        }
        out.encodeIndex(index);
        out.putChar(iFlags);
    }

    /**
     * @throws TextConversionException
     */
    void sendMessage(final MessageCode code, final String text) throws InterruptedException {
        assert code != null;
        assert text != null;
        final Message message = toMessage(code, text);

        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                out.putMessage(message);
            }

            @Override
            public String toString() {
                return String.format("sendMessage(%s, %s)", code, text);
            }
        };
        appendJob(j);
    }

    void sendSegmentDone() throws InterruptedException {
        Job j = new Job() {
            @Override
            public void process() throws ChannelException {
                out.encodeIndex(Filelist.DONE);
            }

            @Override
            public String toString() {
                return "sendSegmentDone()";
            }
        };
        appendJob(j);
    }

    synchronized void setFileAttributeManager(FileAttributeManager fileAttributeManager) {
        this.fileAttributeManager = fileAttributeManager;
    }

    void stop() throws InterruptedException {
        Job job = new Job() {
            @Override
            public void process() {
                running = false;
            }

            @Override
            public String toString() {
                return "stop()";
            }
        };
        appendJob(job);
    }

    private Collection<FileInfo> toInitialListing(Filelist.Segment segment) {
        assert fileSelection == FileSelection.RECURSE;
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
        assert fileSelection == FileSelection.RECURSE;
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
        ByteBuffer payload = ByteBuffer.wrap(characterEncoder.encode(text));
        return new Message(code, payload);
    }

    @Override
    public String toString() {
        return String.format("%s(" + "isAlwaysItemize=%b, " + "isDelete=%b, " + "isIgnoreTimes=%b, " + "isInterruptible=%b, " + "isNumericIds=%b, " + "isPreserveDevices=%b, " + "isPreserveLinks=%b, "
                + "isPreservePermissions=%b, " + "isPreserveSpecials=%b, " + "isPreserveTimes=%b, " + "isPreserveUser=%b, " + "isPreserveGroup=%b, " + "checksumSeed=%s, " + "fileSelection=%s" + ")",
                this.getClass().getSimpleName(), alwaysItemize, delete, ignoreTimes, interruptible, numericIds, preserveDevices, preserveLinks, preservePermissions, preserveSpecials, preserveTimes,
                preserveUser, preserveGroup, Text.bytesToString(checksumSeed), fileSelection);
    }

    private void unlinkFilesInDirNotAtSender(final Path targetPath, Path dir, Collection<FileInfo> files, FilterRuleConfiguration cfg) throws IOException, ChannelException {
        assert delete && deletionsEnabled;
        
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
                            out.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                            returnStatus++;

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

        if (preservePermissions && (curAttrsOrNull == null || curAttrsOrNull.getMode() != newAttrs.getMode())) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) %s: updating mode %o -> %o", path, curAttrsOrNull == null ? 0 : curAttrsOrNull.getMode(), newAttrs.getMode()));
            }
            fileAttributeManager.setFileMode(path, newAttrs.getMode(), LinkOption.NOFOLLOW_LINKS);
        }
        if (preserveTimes && (curAttrsOrNull == null || curAttrsOrNull.lastModifiedTime() != newAttrs.lastModifiedTime())) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("(Generator) %s: updating mtime %s -> %s", path,
                        curAttrsOrNull == null ? dateFormat.format(new Date(FileTime.from(0L, TimeUnit.SECONDS).toMillis()))
                                : dateFormat.format(new Date(FileTime.from(curAttrsOrNull.lastModifiedTime(), TimeUnit.SECONDS).toMillis())),
                        dateFormat.format(new Date(FileTime.from(newAttrs.lastModifiedTime(), TimeUnit.SECONDS).toMillis()))));

            }
            fileAttributeManager.setLastModifiedTime(path, newAttrs.lastModifiedTime(), LinkOption.NOFOLLOW_LINKS);
        }
        // NOTE: keep this one last in the method, in case we fail due to
        // insufficient permissions (the other ones are more likely to
        // succeed).
        // NOTE: we cannot detect if we have the capabilities to change
        // ownership (knowing if UID 0 is not sufficient)
        if (preserveUser) {
            if (!numericIds && !newAttrs.getUser().getName().isEmpty() && (curAttrsOrNull == null || !curAttrsOrNull.getUser().getName().equals(newAttrs.getUser().getName()))) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating ownership %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getUser(), newAttrs.getUser()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                fileAttributeManager.setOwner(path, newAttrs.getUser(), LinkOption.NOFOLLOW_LINKS);
            } else if ((numericIds || newAttrs.getUser().getName().isEmpty()) && (curAttrsOrNull == null || curAttrsOrNull.getUser().getId() != newAttrs.getUser().getId())) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating ownership %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getUser().getId(), newAttrs.getUser().getId()));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                fileAttributeManager.setUserId(path, newAttrs.getUser().getId(), LinkOption.NOFOLLOW_LINKS);
            }
        }

        if (preserveGroup) {
            if (!numericIds && !newAttrs.getGroup().getName().isEmpty() && (curAttrsOrNull == null || !curAttrsOrNull.getGroup().getName().equals(newAttrs.getGroup().getName()))) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating group %s -> %s", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getGroup(), newAttrs.getGroup()));
                }
                fileAttributeManager.setGroup(path, newAttrs.getGroup(), LinkOption.NOFOLLOW_LINKS);
            } else if ((numericIds || newAttrs.getGroup().getName().isEmpty()) && (curAttrsOrNull == null || curAttrsOrNull.getGroup().getId() != newAttrs.getGroup().getId())) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(Generator) %s: updating gid %s -> %d", path, curAttrsOrNull == null ? "" : curAttrsOrNull.getGroup().getId(), newAttrs.getGroup().getId()));
                }
                fileAttributeManager.setGroupId(path, newAttrs.getGroup().getId(), LinkOption.NOFOLLOW_LINKS);
            }
        }
    }
}
