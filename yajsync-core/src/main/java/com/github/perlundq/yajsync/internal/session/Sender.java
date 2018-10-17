/*
 * Processing of incoming file information from peer
 * Generator and sending of file lists and file data to peer Receiver
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
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.FileSelection;
import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.Statistics;
import com.github.perlundq.yajsync.attr.DeviceInfo;
import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.attr.Group;
import com.github.perlundq.yajsync.attr.LocatableFileInfo;
import com.github.perlundq.yajsync.attr.RsyncFileAttributes;
import com.github.perlundq.yajsync.attr.SymlinkInfo;
import com.github.perlundq.yajsync.attr.User;
import com.github.perlundq.yajsync.internal.channels.AutoFlushableRsyncDuplexChannel;
import com.github.perlundq.yajsync.internal.channels.ChannelEOFException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.channels.Message;
import com.github.perlundq.yajsync.internal.channels.MessageCode;
import com.github.perlundq.yajsync.internal.channels.MessageHandler;
import com.github.perlundq.yajsync.internal.channels.RsyncInChannel;
import com.github.perlundq.yajsync.internal.channels.RsyncOutChannel;
import com.github.perlundq.yajsync.internal.io.FileView;
import com.github.perlundq.yajsync.internal.io.FileViewException;
import com.github.perlundq.yajsync.internal.io.FileViewNotFound;
import com.github.perlundq.yajsync.internal.io.FileViewOpenFailed;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.text.TextDecoder;
import com.github.perlundq.yajsync.internal.text.TextEncoder;
import com.github.perlundq.yajsync.internal.util.ArgumentParsingError;
import com.github.perlundq.yajsync.internal.util.Environment;
import com.github.perlundq.yajsync.internal.util.FileOps;
import com.github.perlundq.yajsync.internal.util.MD5;
import com.github.perlundq.yajsync.internal.util.PathOps;
import com.github.perlundq.yajsync.internal.util.Rolling;
import com.github.perlundq.yajsync.internal.util.RuntimeInterruptException;
import com.github.perlundq.yajsync.internal.util.StatusResult;

public final class Sender implements RsyncTask, MessageHandler {
    public static class Builder {
        public static Builder newClient(ReadableByteChannel in, WritableByteChannel out, Iterable<Path> sourceFiles, byte[] checksumSeed) {
            Builder builder = new Builder(in, out, sourceFiles, checksumSeed);
            builder.sendStatistics = false;
            builder.exitEarlyIfEmptyList = false;
            builder.exitAfterEOF = true;
            return builder;
            
        }
        
        public static Builder newServer(ReadableByteChannel in, WritableByteChannel out, Iterable<Path> sourceFiles, byte[] checksumSeed) {
            Builder builder = new Builder(in, out, sourceFiles, checksumSeed);
            builder.sendStatistics = true;
            builder.exitEarlyIfEmptyList = true;
            builder.exitAfterEOF = false;
            return builder;
        }
        
        private Charset charset = Charset.forName(Text.UTF8_NAME);
        private final byte[] checksumSeed;
        public int defaultDirectoryPermissions = Environment.DEFAULT_DIR_PERMS;
        public int defaultFilePermissions = Environment.DEFAULT_FILE_PERMS;
        public Group defaultGroup = Group.NOBODY;
        public User defaultUser = User.NOBODY;
        private FileSelection fileSelection = FileSelection.EXACT;
        private FilterMode filterMode = FilterMode.NONE;
        private FilterRuleConfiguration filterRuleConfiguration;
        private final ReadableByteChannel in;
        private boolean exitAfterEOF;
        private boolean exitEarlyIfEmptyList;
        private boolean interruptible = true;
        private boolean numericIds;
        private boolean preserveDevices;
        private boolean preserveGroup;
        private boolean preserveLinks;
        private boolean preserveSpecials;
        private boolean preserveUser;
        private boolean safeFileList = true;
        private boolean sendStatistics;
        
        private final WritableByteChannel out;
        
        private final Iterable<Path> sourceFiles;
        
        public Builder(ReadableByteChannel in, WritableByteChannel out, Iterable<Path> sourceFiles, byte[] checksumSeed) {
            assert in != null;
            assert out != null;
            assert sourceFiles != null;
            assert checksumSeed != null;
            this.in = in;
            this.out = out;
            this.sourceFiles = sourceFiles;
            this.checksumSeed = checksumSeed;
        }
        
        public Sender build() {
            return new Sender(this);
        }
        
        public Builder charset(Charset charset) {
            assert charset != null;
            this.charset = charset;
            return this;
        }
        
        public Builder defaultDirectoryPermissions(int defaultDirectoryPermissions) {
            this.defaultDirectoryPermissions = defaultDirectoryPermissions;
            return this;
        }
        
        public Builder defaultFilePermissions(int defaultFilePermissions) {
            this.defaultFilePermissions = defaultFilePermissions;
            return this;
        }
        
        public Builder defaultGroup(Group defaultGroup) {
            this.defaultGroup = defaultGroup;
            return this;
        }
        
        public Builder defaultUser(User defaultUser) {
            this.defaultUser = defaultUser;
            return this;
        }
        
        public Builder fileSelection(FileSelection fileSelection) {
            assert fileSelection != null;
            this.fileSelection = fileSelection;
            return this;
        }
        
        public Builder filterMode(FilterMode filterMode) {
            assert filterMode != null;
            this.filterMode = filterMode;
            return this;
        }
        
        public Builder isExitEarlyIfEmptyList(boolean isExitEarlyIfEmptyList) {
            this.exitEarlyIfEmptyList = isExitEarlyIfEmptyList;
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
        
        public Builder isPreserveSpecials(boolean isPreserveSpecials) {
            this.preserveSpecials = isPreserveSpecials;
            return this;
        }
        
        public Builder isPreserveUser(boolean isPreserveUser) {
            this.preserveUser = isPreserveUser;
            return this;
        }
        
        public Builder isSafeFileList(boolean isSafeFileList) {
            this.safeFileList = isSafeFileList;
            return this;
        }
        
        public Builder filterRuleConfiguration(FilterRuleConfiguration filterRuleConfiguration) {
            this.filterRuleConfiguration = filterRuleConfiguration;
            return this;
        }
    }
    
    private static final Logger LOG = Logger.getLogger(Sender.class.getName());
    private static final int CHUNK_SIZE = 8 * 1024;
    private static final int INPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final int OUTPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final int PARTIAL_FILE_LIST_SIZE = 1024;
    
    private static void createIncorrectChecksum(byte[] checksum) {
        checksum[0]++;
    }
    
    private static int lengthOfLargestCommonPrefix(byte[] left, byte[] right) {
        int index = 0;
        while (index < left.length && index < right.length && left[index] == right[index]) {
            index++;
        }
        return index;
    }
    
    private final TextDecoder characterDecoder;
    private final TextEncoder characterEncoder;
    private final byte[] checksumSeed;
    private int curSegmentIndex;
    private final int defaultDirectoryPermissions;
    private final int defaultFilePermissions;
    private final Group defaultGroup;
    private final User defaultUser;
    private final AutoFlushableRsyncDuplexChannel duplexChannel;
    private FileAttributeManager fileAttributeManager;
    private final FileInfoCache fileInfoCache = new FileInfoCache();
    private final FileSelection fileSelection;
    private final FilterMode filterMode;
    private final FilterRuleConfiguration filterRuleConfiguration;
    private int ioError;
    private final boolean exitAfterEOF;
    private final boolean exitEarlyIfEmptyList;
    private final boolean interruptible;
    private final boolean numericIds;
    private final boolean preserveDevices;
    private final boolean preserveGroup;
    private final boolean preserveLinks;
    private final boolean preserveSpecials;
    private final boolean preserveUser;
    private final boolean safeFileList;
    private final boolean sendStatistics;
    
    private final Iterable<Path> sourceFiles;
    private final SessionStatistics stats = new SessionStatistics();
    private final BitSet transferred = new BitSet();
    
    private final Set<Group> transferredGroupNames = new LinkedHashSet<>();
    
    private final Set<User> transferredUserNames = new LinkedHashSet<>();
    
    private Sender(Builder builder) {
        this.duplexChannel = new AutoFlushableRsyncDuplexChannel(new RsyncInChannel(builder.in, this, INPUT_CHANNEL_BUF_SIZE), new RsyncOutChannel(builder.out, OUTPUT_CHANNEL_BUF_SIZE));
        this.exitAfterEOF = builder.exitAfterEOF;
        this.exitEarlyIfEmptyList = builder.exitEarlyIfEmptyList;
        this.interruptible = builder.interruptible;
        this.preserveDevices = builder.preserveDevices;
        this.preserveLinks = builder.preserveLinks;
        this.preserveSpecials = builder.preserveSpecials;
        this.preserveUser = builder.preserveUser;
        this.preserveGroup = builder.preserveGroup;
        this.numericIds = builder.numericIds;
        this.safeFileList = builder.safeFileList;
        this.sendStatistics = builder.sendStatistics;
        this.checksumSeed = builder.checksumSeed;
        this.fileSelection = builder.fileSelection;
        this.filterMode = builder.filterMode;
        this.filterRuleConfiguration = builder.filterRuleConfiguration;
        this.sourceFiles = builder.sourceFiles;
        this.characterDecoder = TextDecoder.newStrict(builder.charset);
        this.characterEncoder = TextEncoder.newStrict(builder.charset);
        this.defaultUser = builder.defaultUser;
        this.defaultGroup = builder.defaultGroup;
        this.defaultFilePermissions = builder.defaultFilePermissions;
        this.defaultDirectoryPermissions = builder.defaultDirectoryPermissions;
    }
    
    @Override
    public Boolean call() throws ChannelException, InterruptedException, RsyncProtocolException {
        Filelist fileList = new Filelist(this.fileSelection == FileSelection.RECURSE, false);
        FilterRuleConfiguration filterRuleConfiguration;
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(this.toString());
            }
            
            if (this.filterMode == FilterMode.RECEIVE) {
                // read remote filter rules if server
                try {
                    filterRuleConfiguration = new FilterRuleConfiguration(this.receiveFilterRules());
                } catch (ArgumentParsingError e) {
                    throw new RsyncProtocolException(e);
                }
            } else if (this.filterMode == FilterMode.SEND) {
                filterRuleConfiguration = this.filterRuleConfiguration;
                this.sendFilterRules();
            } else {
                try {
                    filterRuleConfiguration = new FilterRuleConfiguration(Collections.emptyList());
                } catch (ArgumentParsingError e) {
                    throw new RsyncProtocolException(e);
                }
            }
            
            long t1 = System.currentTimeMillis();
            
            StatusResult<List<FileInfo>> expandResult = this.initialExpand(this.sourceFiles, filterRuleConfiguration);
            boolean isInitialListOK = expandResult.isOK();
            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);
            builder.addAll(expandResult.getValue());
            Filelist.Segment initialSegment = fileList.newSegment(builder);
            long numBytesWritten = this.duplexChannel.numBytesWritten();
            for (FileInfo f : initialSegment.getFiles()) {
                this.sendFileMetaData((LocatableFileInfo) f);
            }
            long t2 = System.currentTimeMillis();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("expanded segment: " + initialSegment.toString());
            }
            if (isInitialListOK) {
                this.sendSegmentDone();
            } else {
                this.sendFileListErrorNotification();
            }
            long t3 = System.currentTimeMillis();
            
            if (this.preserveUser && !this.numericIds && this.fileSelection != FileSelection.RECURSE) {
                this.sendUserList();
            }
            if (this.preserveGroup && !this.numericIds && this.fileSelection != FileSelection.RECURSE) {
                this.sendGroupList();
            }
            
            this.stats.fileListBuildTime = Math.max(1, t2 - t1);
            this.stats.fileListTransferTime = Math.max(0, t3 - t2);
            long segmentSize = this.duplexChannel.numBytesWritten() - numBytesWritten;
            this.stats.totalFileListSize += segmentSize;
            if (!this.safeFileList && !isInitialListOK) {
                this.sendIntMessage(MessageCode.IO_ERROR, IoError.GENERAL);
            }
            
            if (initialSegment.isFinished() && this.exitEarlyIfEmptyList) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("empty file list - exiting early");
                }
                if (this.fileSelection == FileSelection.RECURSE) {
                    this.duplexChannel.encodeIndex(Filelist.EOF);
                }
                this.duplexChannel.flush();
                if (this.exitAfterEOF) {
                    this.readAllMessagesUntilEOF();
                }
                return isInitialListOK && this.ioError == 0;
            }
            
            int ioError = this.sendFiles(fileList, filterRuleConfiguration);
            if (ioError != 0) {
                this.sendIntMessage(MessageCode.IO_ERROR, ioError);
            }
            this.duplexChannel.encodeIndex(Filelist.DONE);
            
            // we update the statistics in finally clause to guarantee that the
            // statistics are updated even if there's an error
            if (this.sendStatistics) {
                this.stats.totalFileSize = fileList.getTotalFileSize();
                this.stats.totalBytesRead = this.duplexChannel.numBytesRead();
                this.stats.totalBytesWritten = this.duplexChannel.numBytesWritten();
                this.stats.numFiles = fileList.getNumFiles();
                this.sendStatistics(this.stats);
            }
            
            int index = this.duplexChannel.decodeIndex();
            if (index != Filelist.DONE) {
                throw new RsyncProtocolException(String.format("Invalid packet at end of run (%d)", index));
            }
            if (this.exitAfterEOF) {
                this.readAllMessagesUntilEOF();
            }
            return isInitialListOK && (ioError | this.ioError) == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            this.stats.totalFileSize = fileList.getTotalFileSize();
            this.stats.totalBytesRead = this.duplexChannel.numBytesRead();
            this.stats.totalBytesWritten = this.duplexChannel.numBytesWritten();
            this.stats.numFiles = fileList.getNumFiles();
        }
    }
    
    @Override
    public void closeChannel() throws ChannelException {
        this.duplexChannel.close();
    }
    
    private StatusResult<List<FileInfo>> expand(LocatableFileInfo directory, FilterRuleConfiguration parentFilterRuleConfiguration) throws ChannelException {
        assert directory != null;
        
        List<FileInfo> fileset = new ArrayList<>();
        boolean isOK = true;
        final Path dir = directory.getPath();
        final Path localDir = this.localPathTo(directory);
        // that should never happen
        
        FilterRuleConfiguration localFilterRuleConfiguration;
        try {
            localFilterRuleConfiguration = new FilterRuleConfiguration(parentFilterRuleConfiguration, directory.getPath());
        } catch (ArgumentParsingError e) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(String.format("Got argument parsing error " + "at %s: %s", directory.getPath(), e.getMessage()));
            }
            isOK = false;
            return new StatusResult<>(isOK, fileset);
        }
        boolean filterByRules = localFilterRuleConfiguration.isFilterAvailable();
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (!PathOps.isPathPreservable(entry.getFileName())) {
                    String msg = String.format("Skipping %s - unable to " + "preserve file name", entry.getFileName());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    isOK = false;
                    continue;
                }
                
                RsyncFileAttributes attrs;
                try {
                    attrs = this.fileAttributeManager.stat(entry);
                } catch (IOException e) {
                    String msg = String.format("Failed to stat %s: %s", entry, e.getMessage());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    isOK = false;
                    continue;
                }
                
                Path relativePath = localDir.relativize(entry).normalize();
                String relativePathName = Text.withSlashAsPathSepator(relativePath);
                byte[] pathNameBytes = this.characterEncoder.encodeOrNull(relativePathName);
                if (pathNameBytes != null) {
                    // use filter
                    if (filterByRules) {
                        boolean isDirectory = attrs.isDirectory();
                        if (localFilterRuleConfiguration.exclude(relativePathName, isDirectory)) {
                            continue;
                        }
                        if (localFilterRuleConfiguration.hide(relativePathName, isDirectory)) {
                            continue;
                        }
                    }
                    
                    LocatableFileInfo f;
                    if (this.preserveLinks && attrs.isSymbolicLink()) {
                        Path symlinkTarget = FileOps.readLinkTarget(entry);
                        f = new LocatableSymlinkInfoImpl(relativePathName, pathNameBytes, attrs, symlinkTarget.toString(), entry);
                    } else if (this.preserveDevices && (attrs.isBlockDevice() || attrs.isCharacterDevice())) {
                        String msg = String.format("unable to retrieve major and minor ID of " + "%s %s", FileOps.fileTypeToString(attrs.getMode()), entry);
                        if (LOG.isLoggable(Level.WARNING)) {
                            LOG.warning(msg);
                        }
                        this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                        isOK = false;
                        continue;
                    } else if (this.preserveSpecials && (attrs.isFifo() || attrs.isSocket())) {
                        String msg = String.format("unable to retrieve major ID of %s %s", FileOps.fileTypeToString(attrs.getMode()), entry);
                        if (LOG.isLoggable(Level.WARNING)) {
                            LOG.warning(msg);
                        }
                        this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                        isOK = false;
                        continue;
                    } else {
                        // throws IllegalArgumentException but that cannot
                        // happen here
                        f = new LocatableFileInfoImpl(relativePathName, pathNameBytes, attrs, entry);
                    }
                    
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("adding %s to segment", f));
                    }
                    fileset.add(f);
                } else {
                    String msg = String.format("Failed to encode %s using %s", relativePathName, this.characterEncoder.charset());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    isOK = false;
                }
            }
        } catch (IOException e) {
            String msg;
            if (e instanceof AccessDeniedException) {
                msg = String.format("Failed to read directory %s: %s", directory.getPath(), e);
            } else {
                msg = String.format("Got I/O error during expansion of %s: %s", directory.getPath(), e);
            }
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(msg);
            }
            this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            isOK = false;
        }
        return new StatusResult<>(isOK, fileset);
    }
    
    private StatusResult<Integer> expandAndSendSegments(Filelist fileList, int limit, FilterRuleConfiguration parentFilterRuleConfiguration) throws ChannelException {
        boolean isOK = true;
        int numFilesSent = 0;
        int numSegmentsSent = 0;
        long numBytesWritten = this.duplexChannel.numBytesWritten();
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("expanding segments until at least %d " + "files have been sent", limit));
        }
        
        while (fileList.isExpandable() && numFilesSent < limit) {
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("sending segment index %d (as %d)", this.curSegmentIndex, Filelist.OFFSET - this.curSegmentIndex));
            }
            
            assert this.curSegmentIndex >= 0;
            LocatableFileInfo directory = (LocatableFileInfo) fileList.getStubDirectoryOrNull(this.curSegmentIndex);
            assert directory != null;
            this.duplexChannel.encodeIndex(Filelist.OFFSET - this.curSegmentIndex);
            
            StatusResult<List<FileInfo>> expandResult = this.expand(directory, parentFilterRuleConfiguration);
            boolean isExpandOK = expandResult.isOK();
            if (!isExpandOK && LOG.isLoggable(Level.WARNING)) {
                LOG.warning("initial file list expansion returned an error");
            }
            
            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(directory);
            builder.addAll(expandResult.getValue());
            Filelist.Segment segment = fileList.newSegment(builder);
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("expanded segment with segment index" + " %d", this.curSegmentIndex));
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(segment.toString());
                }
            }
            
            for (FileInfo fileInfo : segment.getFiles()) {
                this.sendFileMetaData((LocatableFileInfo) fileInfo);
                numFilesSent++;
            }
            
            if (isExpandOK) {
                this.sendSegmentDone();
            } else {
                // NOTE: once an error happens for native it will send an error
                // for each file list segment for the same loop block - we don't
                isOK = false;
                this.sendFileListErrorNotification();
            }
            this.curSegmentIndex++;
            numSegmentsSent++;
        }
        
        long segmentSize = this.duplexChannel.numBytesWritten() - numBytesWritten;
        this.stats.totalFileListSize += segmentSize;
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("sent meta data for %d segments and %d " + "files", numSegmentsSent, numFilesSent));
        }
        
        return new StatusResult<>(isOK, numFilesSent);
    }
    
    /**
     * @throws RsyncProtocolException if peer sends a message we cannot decode
     */
    @Override
    public void handleMessage(Message message) throws RsyncProtocolException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("got message " + message);
        }
        switch (message.getHeader().getMessageType()) {
            case IO_ERROR:
                this.ioError |= message.getPayload().getInt();
                break;
            case ERROR:
            case ERROR_XFER:
                this.ioError |= IoError.TRANSFER; // fall through
            case INFO:
            case WARNING:
            case LOG:
                if (LOG.isLoggable(message.logLevelOrNull())) {
                    this.printMessage(message);
                }
                break;
            default:
                throw new RuntimeException("TODO: (not yet implemented) missing case statement for " + message);
        }
    }
    
    private StatusResult<List<FileInfo>> initialExpand(Iterable<Path> files, FilterRuleConfiguration parentFilterRuleConfiguration) throws ChannelException {
        boolean isOK = true;
        List<FileInfo> fileset = new LinkedList<>();
        
        for (Path p : files) {
            try {
                if (this.fileAttributeManager == null) {
                    this.setFileAttributeManager(p.getFileSystem());
                }
                
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("expanding " + p);
                }
                
                LocatableFileInfo fileInfo = this.statAndEncode(p);
                
                if (this.fileSelection == FileSelection.EXACT && fileInfo.getAttributes().isDirectory()) {
                    if (LOG.isLoggable(Level.INFO)) {
                        LOG.info("skipping directory " + fileInfo);
                    }
                } else {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("adding %s to segment", fileInfo));
                    }
                    fileset.add(fileInfo);
                    if (((LocatableFileInfoImpl) fileInfo).isDotDir()) {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("expanding dot dir " + fileInfo);
                        }
                        StatusResult<List<FileInfo>> expandResult = this.expand(fileInfo, parentFilterRuleConfiguration);
                        isOK = isOK && expandResult.isOK();
                        for (FileInfo f2 : expandResult.getValue()) {
                            fileset.add(f2);
                        }
                        this.curSegmentIndex++;
                    }
                }
            } catch (IOException e) {
                String msg = String.format("Failed to add %s to initial file " + "list: %s", p, e);
                if (LOG.isLoggable(Level.WARNING)) {
                    LOG.warning(msg);
                }
                this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                isOK = false;
            } catch (TextConversionException e) {
                String msg = String.format("Failed to encode %s using %s", p, this.characterEncoder.charset());
                if (LOG.isLoggable(Level.WARNING)) {
                    LOG.warning(msg);
                }
                this.duplexChannel.putMessage(this.toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                isOK = false;
            }
        }
        return new StatusResult<>(isOK, fileset);
    }
    
    @Override
    public boolean isInterruptible() {
        return this.interruptible;
    }
    
    private boolean isTransferred(int index) {
        return this.transferred.get(index);
    }
    
    // i.e. if full path is /a/b/c/d and pathNamebytes is c/d this returns /a/b
    private Path localPathTo(LocatableFileInfo fileInfo) {
        String pathName = fileInfo.getPathName(); /* never null */
        FileSystem fs = fileInfo.getPath().getFileSystem();
        Path relativePath = fs.getPath(pathName);
        return PathOps.subtractPathOrNull(fileInfo.getPath(), relativePath);
    }
    
    /**
     * @throws RsyncProtocolException if peer sends a message we cannot decode
     */
    private void printMessage(Message message) throws RsyncProtocolException {
        assert message.isText();
        try {
            MessageCode msgType = message.getHeader().getMessageType();
            // throws TextConversionException
            String text = this.characterDecoder.decode(message.getPayload());
            // Receiver here means the opposite of Sender - not the process
            // (which actually is the Generator)
            LOG.log(message.logLevelOrNull(), String.format("<RECEIVER> %s: %s", msgType, Text.stripLast(text)));
        } catch (TextConversionException e) {
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.severe(String.format("Peer sent a message but we failed to convert all " + "characters in message. %s (%s)", e, message.toString()));
            }
            throw new RsyncProtocolException(e);
        }
    }
    
    private void readAllMessagesUntilEOF() throws ChannelException, RsyncProtocolException {
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("reading final messages until EOF");
            }
            // dummy read to get any final messages from peer
            byte dummy = this.duplexChannel.getByte();
            // we're not expected to get this far, getByte should throw
            // ChannelEOFException
            ByteBuffer buf = ByteBuffer.allocate(1024);
            try {
                buf.put(dummy);
                while (buf.hasRemaining()) {
                    dummy = this.duplexChannel.getByte();
                    buf.put(dummy);
                }
            } catch (ChannelEOFException ignored) {
                // ignored
            }
            buf.flip();
            throw new RsyncProtocolException(String.format("Unexpectedly got %d bytes from peer during connection " + "tear down: %s", buf.remaining(), Text.byteBufferToString(buf)));
            
        } catch (ChannelEOFException e) {
            // It's OK, we expect EOF without having received any data
        }
    }
    
    private Checksum.Header receiveChecksumHeader() throws ChannelException, RsyncProtocolException {
        return Connection.receiveChecksumHeader(this.duplexChannel);
    }

    
    private Checksum receiveChecksumsFor(Checksum.Header header) throws ChannelException {
        Checksum checksum = new Checksum(header);
        for (int i = 0; i < header.getChunkCount(); i++) {
            int rolling = this.duplexChannel.getInt();
            byte[] md5sum = new byte[header.getDigestLength()];
            this.duplexChannel.get(md5sum, 0, md5sum.length);
            checksum.addChunkInformation(rolling, md5sum);
        }
        return checksum;
    }
    
    /**
     * @throws RsyncProtocolException if failing to decode the filter rules
     */
    private List<String> receiveFilterRules() throws ChannelException, RsyncProtocolException {
        int numBytesToRead;
        List<String> list = new ArrayList<>();
        
        try {
            
            while ((numBytesToRead = this.duplexChannel.getInt()) > 0) {
                ByteBuffer buf = this.duplexChannel.get(numBytesToRead);
                list.add(this.characterDecoder.decode(buf));
            }
            
            return list;
            
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }
    
    private void sendChecksumHeader(Checksum.Header header) throws ChannelException {
        Connection.sendChecksumHeader(this.duplexChannel, header);
    }
    
    private void sendDataFrom(byte[] buf, int startOffset, int length) throws ChannelException {
        assert buf != null;
        assert startOffset >= 0;
        assert length >= 0;
        assert startOffset + length <= buf.length;
        
        int endOffset = startOffset + length - 1;
        int currentOffset = startOffset;
        while (currentOffset <= endOffset) {
            int len = Math.min(CHUNK_SIZE, endOffset - currentOffset + 1);
            assert len > 0;
            this.duplexChannel.putInt(len);
            this.duplexChannel.put(buf, currentOffset, len);
            currentOffset += len;
        }
    }
//    
//    private void sendEmptyFilterRules() throws ChannelException {
//        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
//        buf.putInt(0);
//        buf.flip();
//        this._duplexChannel.put(buf);
//    }
    
    private void sendEncodedInt(int i) throws ChannelException {
        this.sendEncodedLong(i, 1);
    }
    
    private void sendEncodedLong(long l, int minBytes) throws ChannelException {
        ByteBuffer b = IntegerCoder.encodeLong(l, minBytes);
        this.duplexChannel.put(b);
    }
    
    private void sendFileListErrorNotification() throws ChannelException {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("sending file list error notification to peer");
        }
        if (this.safeFileList) {
            this.duplexChannel.putChar((char) (0xFFFF & (TransmitFlags.EXTENDED_FLAGS | TransmitFlags.IO_ERROR_ENDLIST)));
            this.sendEncodedInt(IoError.GENERAL);
        } else {
            this.duplexChannel.putByte((byte) 0);
        }
    }
    
    private void sendFileMetaData(LocatableFileInfo fileInfo) throws ChannelException {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("sending meta data for " + fileInfo.getPath());
        }
        
        char xflags = 0;
        
        RsyncFileAttributes attrs = fileInfo.getAttributes();
        if (attrs.isDirectory()) {
            xflags = 1;
        }
        
        if (this.preserveDevices && fileInfo instanceof DeviceInfo && (attrs.isBlockDevice() || attrs.isCharacterDevice())) {
            DeviceInfo dev = (DeviceInfo) fileInfo;
            if (dev.getMajor() == this.fileInfoCache.getPrevMajor()) {
                xflags |= TransmitFlags.SAME_RDEV_MAJOR;
            } else {
                this.fileInfoCache.setPrevMajor(dev.getMajor());
            }
        } else if (this.preserveSpecials && fileInfo instanceof DeviceInfo && (attrs.isFifo() || attrs.isSocket())) {
            xflags |= TransmitFlags.SAME_RDEV_MAJOR;
        }
        
        int mode = attrs.getMode();
        if (mode == this.fileInfoCache.getPrevMode()) {
            xflags |= TransmitFlags.SAME_MODE;
        } else {
            this.fileInfoCache.setPrevMode(mode);
        }
        
        User user = fileInfo.getAttributes().getUser();
        if (this.preserveUser && !user.equals(this.fileInfoCache.getPrevUser())) {
            this.fileInfoCache.setPrevUser(user);
            if (!this.numericIds && !user.equals(User.ROOT)) {
                if (this.fileSelection == FileSelection.RECURSE && !this.transferredUserNames.contains(user)) {
                    xflags |= TransmitFlags.USER_NAME_FOLLOWS;
                } // else send in batch later
                this.transferredUserNames.add(user);
            }
        } else {
            xflags |= TransmitFlags.SAME_UID;
        }
        
        Group group = fileInfo.getAttributes().getGroup();
        if (this.preserveGroup && !group.equals(this.fileInfoCache.getPrevGroup())) {
            this.fileInfoCache.setPrevGroup(group);
            if (!this.numericIds && !group.equals(Group.ROOT)) {
                if (this.fileSelection == FileSelection.RECURSE && !this.transferredGroupNames.contains(group)) {
                    xflags |= TransmitFlags.GROUP_NAME_FOLLOWS;
                } // else send in batch later
                this.transferredGroupNames.add(group);
            }
        } else {
            xflags |= TransmitFlags.SAME_GID;
        }
        
        long lastModified = attrs.lastModifiedTime();
        if (lastModified == this.fileInfoCache.getPrevLastModified()) {
            xflags |= TransmitFlags.SAME_TIME;
        } else {
            this.fileInfoCache.setPrevLastModified(lastModified);
        }
        
        byte[] fileNameBytes = fileInfo.getPathName().getBytes(this.characterEncoder.charset());
        int commonPrefixLength = lengthOfLargestCommonPrefix(this.fileInfoCache.getPrevFileNameBytes(), fileNameBytes);
        byte[] prefixBytes = Arrays.copyOfRange(fileNameBytes, 0, commonPrefixLength);
        byte[] suffixBytes = Arrays.copyOfRange(fileNameBytes, commonPrefixLength, fileNameBytes.length);
        int numSuffixBytes = suffixBytes.length;
        int numPrefixBytes = Math.min(prefixBytes.length, 255);
        if (numPrefixBytes > 0) {
            xflags |= TransmitFlags.SAME_NAME;
        }
        if (numSuffixBytes > 255) {
            xflags |= TransmitFlags.LONG_NAME;
        }
        this.fileInfoCache.setPrevFileNameBytes(fileNameBytes);
        
        if (xflags == 0 && !attrs.isDirectory()) {
            xflags |= TransmitFlags.TOP_DIR;
        }
        if (xflags == 0 || (xflags & 0xFF00) != 0) {
            xflags |= TransmitFlags.EXTENDED_FLAGS;
            this.duplexChannel.putChar(xflags);
        } else {
            this.duplexChannel.putByte((byte) xflags);
        }
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sent flags " + Integer.toBinaryString(xflags));
        }
        
        if ((xflags & TransmitFlags.SAME_NAME) != 0) {
            this.duplexChannel.putByte((byte) numPrefixBytes);
        }
        
        if ((xflags & TransmitFlags.LONG_NAME) != 0) {
            this.sendEncodedInt(numSuffixBytes);
        } else {
            this.duplexChannel.putByte((byte) numSuffixBytes);
        }
        this.duplexChannel.put(ByteBuffer.wrap(suffixBytes));
        
        this.sendEncodedLong(attrs.getSize(), 3);
        
        if ((xflags & TransmitFlags.SAME_TIME) == 0) {
            this.sendEncodedLong(lastModified, 4);
        }
        
        if ((xflags & TransmitFlags.SAME_MODE) == 0) {
            this.duplexChannel.putInt(mode);
        }
        
        if (this.preserveUser && (xflags & TransmitFlags.SAME_UID) == 0) {
            this.sendUserId(user.getId());
            if ((xflags & TransmitFlags.USER_NAME_FOLLOWS) != 0) {
                this.sendUserName(user.getName());
            }
        }
        
        if (this.preserveGroup && (xflags & TransmitFlags.SAME_GID) == 0) {
            this.sendGroupId(group.getId());
            if ((xflags & TransmitFlags.GROUP_NAME_FOLLOWS) != 0) {
                this.sendGroupName(group.getName());
            }
        }
        
        if ((this.preserveDevices || this.preserveSpecials) && fileInfo instanceof DeviceInfo) {
            DeviceInfo dev = (DeviceInfo) fileInfo;
            if ((xflags & TransmitFlags.SAME_RDEV_MAJOR) == 0) {
                this.sendEncodedInt(dev.getMajor());
            }
            this.sendEncodedInt(dev.getMinor());
        } else if (this.preserveLinks && fileInfo instanceof SymlinkInfo) {
            String symlinkTarget = ((SymlinkInfo) fileInfo).getTargetPathName();
            byte[] symlinkTargetBytes = this.characterEncoder.encode(symlinkTarget);
            this.sendEncodedInt(symlinkTargetBytes.length);
            this.duplexChannel.put(ByteBuffer.wrap(symlinkTargetBytes));
        }
    }
    
    private int sendFiles(Filelist fileList, FilterRuleConfiguration parentFilterRuleConfiguration) throws ChannelException, RsyncProtocolException {
        boolean sentEOF = false;
        TransferPhase phase = TransferPhase.TRANSFER;
        int ioError = 0;
        Filelist.Segment segment = fileList.getFirstSegment();
        int numFilesInTransit = segment.getFiles().size();
        
        while (phase != TransferPhase.STOP) {
            // We must send a new segment when we have at least one segment
            // active to avoid deadlocking when talking to rsync
            if (fileList.isExpandable() && (fileList.getExpandedSegments() == 1 || numFilesInTransit < PARTIAL_FILE_LIST_SIZE / 2)) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("expanding file list. In transit: %d files, " + "%d segments", numFilesInTransit, fileList.getExpandedSegments()));
                }
                int lim = Math.max(1, PARTIAL_FILE_LIST_SIZE - numFilesInTransit);
                StatusResult<Integer> res = this.expandAndSendSegments(fileList, lim, parentFilterRuleConfiguration);
                numFilesInTransit += res.getValue();
                if (!res.isOK()) {
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning("got I/O error during file list " + "expansion, notifying peer");
                    }
                    ioError |= IoError.GENERAL;
                    this.sendIntMessage(MessageCode.IO_ERROR, ioError);
                }
            }
            if (this.fileSelection == FileSelection.RECURSE && !fileList.isExpandable() && !sentEOF) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("sending file list EOF");
                }
                this.duplexChannel.encodeIndex(Filelist.EOF);
                sentEOF = true;
            }
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("num bytes buffered: %d, num bytes available to read: %d", this.duplexChannel.getNumBytesBuffered(), this.duplexChannel.numBytesAvailable()));
            }
            
            final int index = this.duplexChannel.decodeIndex();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received index " + index);
            }
            
            if (index == Filelist.DONE) {
                if (this.fileSelection == FileSelection.RECURSE && !fileList.isEmpty()) {
                    // we're unable to delete the segment opportunistically
                    // because we're not being notified about all files that
                    // the receiver is finished with
                    Filelist.Segment removed = fileList.deleteFirstSegment();
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("Deleting segment: " + removed);
//                        if (LOG.isLoggable(Level.FINEST)) {
//                            LOG.finest(removed.filesToString());
//                        }
                    }
                    if (!fileList.isEmpty()) {
                        this.duplexChannel.encodeIndex(Filelist.DONE);
                    }
                    numFilesInTransit -= removed.getFiles().size();
                }
                if (this.fileSelection != FileSelection.RECURSE || fileList.isEmpty()) {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("phase transition %s -> %s", phase, phase.next()));
                    }
                    phase = phase.next();
                    if (phase != TransferPhase.STOP) {
                        this.duplexChannel.encodeIndex(Filelist.DONE);
                    }
                }
            } else if (index >= 0) {
                char iFlags = this.duplexChannel.getChar();
                if (!Item.isValidItem(iFlags)) {
                    throw new IllegalStateException(String.format("got flags %s - not supported", Integer.toBinaryString(iFlags)));
                }
                if ((iFlags & Item.TRANSFER) == 0) {
                    if (segment == null || segment.getFileWithIndexOrNull(index) == null) {
                        segment = fileList.getSegmentWith(index);
                    }
                    assert segment != null;
                    if (this.fileSelection == FileSelection.RECURSE && segment == null) {
                        throw new RsyncProtocolException(String.format("Received invalid file/directory index %d from " + "peer", index));
                    }
                    if (index != segment.getDirectoryIndex()) {
                        FileInfo removed = segment.remove(index);
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine(String.format("Deleting file/dir %s %d", removed, index));
                        }
                        numFilesInTransit--;
                    }
                    this.sendIndexAndIflags(index, iFlags);
                } else if (phase == TransferPhase.TRANSFER) {
                    LocatableFileInfo fileInfo = null;
                    if (segment != null) {
                        fileInfo = (LocatableFileInfo) segment.getFileWithIndexOrNull(index);
                    }
                    if (fileInfo == null) {
                        segment = fileList.getSegmentWith(index);
                    }
                    if (segment == null) {
                        throw new RsyncProtocolException(String.format("Received invalid file index %d from peer", index));
                    }
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("caching segment: " + segment);
//                        if (LOG.isLoggable(Level.FINEST)) {
//                            LOG.finest(segment.filesToString());
//                        }
                    }
                    
                    fileInfo = (LocatableFileInfo) segment.getFileWithIndexOrNull(index);
                    if (fileInfo == null || !fileInfo.getAttributes().isRegularFile()) {
                        throw new RsyncProtocolException(String.format("index %d is not a regular file (%s)", index, fileInfo));
                    }
                    
                    if (LOG.isLoggable(Level.FINE)) {
                        if (this.isTransferred(index)) {
                            LOG.fine("Re-sending " + fileInfo);
                        } else {
                            LOG.fine("sending " + fileInfo);
                        }
                    }
                    
                    Checksum.Header header = this.receiveChecksumHeader();
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("received peer checksum " + header);
                    }
                    Checksum checksum = this.receiveChecksumsFor(header);
                    
                    boolean isNew = header.getBlockLength() == 0;
                    int blockSize = isNew ? FileView.DEFAULT_BLOCK_SIZE : header.getBlockLength();
                    int blockFactor = isNew ? 1 : 10;
                    long fileSize = fileInfo.getAttributes().getSize();
                    
                    byte[] fileMD5sum = null;
                    try (FileView fv = new FileView(fileInfo.getPath(), fileInfo.getAttributes().getSize(), blockSize, blockSize * blockFactor)) {
                        
                        this.sendIndexAndIflags(index, iFlags);
                        this.sendChecksumHeader(header);
                        
                        if (isNew) {
                            fileMD5sum = this.skipMatchSendData(fv, fileSize);
                        } else {
                            fileMD5sum = this.sendMatchesAndData(fv, checksum, fileSize);
                        }
                    } catch (FileViewOpenFailed e) { // on FileView.open()
                        if (LOG.isLoggable(Level.WARNING)) {
                            LOG.warning(String.format("Error: cannot open %s: %s", fileInfo, e.getMessage()));
                        }
                        if (e instanceof FileViewNotFound) {
                            ioError |= IoError.VANISHED;
                        } else {
                            ioError |= IoError.GENERAL;
                        }
                        
                        FileInfo removed = segment.remove(index);
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine(String.format("Purging %s index=%d", removed, index));
                        }
                        this.sendIntMessage(MessageCode.NO_SEND, index);
                        continue;
                    } catch (FileViewException e) { // on FileView.close()
                        if (LOG.isLoggable(Level.WARNING)) {
                            LOG.warning(String.format("Error: general I/O error on %s (ignored and" + " skipped): %s", fileInfo, e.getMessage()));
                        }
                        // fileMD5sum is only null for FileViewOpenFailed - not
                        // FileViewReadError which is caused by FileView.close()
                        createIncorrectChecksum(fileMD5sum);
                    }
                    
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.finer(String.format("sending checksum for %s: %s", fileInfo.getPath(), Text.bytesToString(fileMD5sum)));
                    }
                    this.duplexChannel.put(fileMD5sum, 0, fileMD5sum.length);
                    this.setIsTransferred(index);
                    
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("sent %s (%d bytes)", fileInfo.getPath(), fileSize));
                    }
                    
                    this.stats.numTransferredFiles++;
                    this.stats.totalTransferredSize += fileInfo.getAttributes().getSize();
                } else {
                    throw new RsyncProtocolException(String.format("Error: received index in wrong phase (%s)", phase));
                }
            } else {
                throw new RsyncProtocolException(String.format("Error: received invalid index %d from peer", index));
            }
        }
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("finished sending files");
        }
        
        return ioError;
    }
    
    private void sendFilterRules() throws InterruptedException, ChannelException {
        if (this.filterRuleConfiguration != null) {
            for (FilterRuleList.FilterRule rule : this.filterRuleConfiguration.getFilterRuleListForSending()._rules) {
                byte[] encodedRule = this.characterEncoder.encode(rule.toString());
                
                ByteBuffer buf = ByteBuffer.allocate(4 + encodedRule.length).order(ByteOrder.LITTLE_ENDIAN);
                buf.putInt(encodedRule.length);
                buf.put(encodedRule);
                buf.flip();
                this.duplexChannel.put(buf);
            }
        }
        
        // send stop signal
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0);
        buf.flip();
        this.duplexChannel.put(buf);
    }
    
    private void sendGroupId(int gid) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending group id " + gid);
        }
        this.sendEncodedInt(gid);
    }
    
    private void sendGroupList() throws ChannelException {
        for (Group group : this.transferredGroupNames) {
            assert group.getId() != Group.ROOT.getId();
            this.sendGroupId(group.getId());
            this.sendGroupName(group.getName());
        }
        this.sendEncodedInt(0);
    }
    
    private void sendGroupName(String name) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending group name " + name);
        }
        ByteBuffer buf = ByteBuffer.wrap(this.characterEncoder.encode(name));
        if (buf.remaining() > 0xFF) {
            throw new IllegalStateException(String.format("encoded length of group name %s is %d, which is larger than " + "what fits in a byte (255)", name, buf.remaining()));
        }
        this.duplexChannel.putByte((byte) buf.remaining());
        this.duplexChannel.put(buf);
    }
    
    private void sendIndexAndIflags(int index, char iFlags) throws ChannelException {
        if (!Item.isValidItem(iFlags)) {
            throw new IllegalStateException(String.format("got flags %s - not supported", Integer.toBinaryString(iFlags)));
        }
        this.duplexChannel.encodeIndex(index);
        this.duplexChannel.putChar(iFlags);
    }
    
    private void sendIntMessage(MessageCode code, int value) throws ChannelException {
        ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        payload.putInt(0, value);
        Message message = new Message(code, payload);
        this.duplexChannel.putMessage(message);
    }
    
    private byte[] sendMatchesAndData(FileView fv, Checksum peerChecksum, long fileSize) throws ChannelException {
        assert fv != null;
        assert peerChecksum != null;
        assert peerChecksum.getHeader().getBlockLength() > 0;
        assert fileSize > 0;
        
        MessageDigest fileDigest = MD5.newInstance();
        MessageDigest chunkDigest = MD5.newInstance();
        
        int rolling = Rolling.compute(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
        int preferredIndex = 0;
        long sizeLiteral = 0;
        long sizeMatch = 0;
        byte[] localChunkMd5sum = null;
        fv.setMarkRelativeToStart(0);
        
        while (fv.getWindowLength() >= peerChecksum.getHeader().getSmallestChunkSize()) {
            
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest(fv.toString());
            }
            
            for (Checksum.Chunk chunk : peerChecksum.getCandidateChunks(rolling, fv.getWindowLength(), preferredIndex)) {
                
                if (localChunkMd5sum == null) {
                    chunkDigest.update(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
                    chunkDigest.update(this.checksumSeed);
                    localChunkMd5sum = Arrays.copyOf(chunkDigest.digest(), chunk.getMd5Checksum().length);
                }
                
                if (Arrays.equals(localChunkMd5sum, chunk.getMd5Checksum())) {
                    if (LOG.isLoggable(Level.FINER)) {
                        LOG.finer(String.format("match %s == %s %s", MD5.md5DigestToString(localChunkMd5sum), MD5.md5DigestToString(chunk.getMd5Checksum()), fv));
                    }
                    sizeMatch += fv.getWindowLength();
                    this.sendDataFrom(fv.getArray(), fv.getMarkOffset(), fv.getNumBytesMarked());
                    sizeLiteral += fv.getNumBytesMarked();
                    fileDigest.update(fv.getArray(), fv.getMarkOffset(), fv.getTotalBytes());
                    
                    this.duplexChannel.putInt(-(chunk.getChunkIndex() + 1));
                    preferredIndex = chunk.getChunkIndex() + 1;
                    // we have sent all literal data until start of this
                    // chunk which in turn is matching peer's checksum,
                    // reset cursor:
                    fv.setMarkRelativeToStart(fv.getWindowLength());
                    // slide start to 1 byte left of mark offset,
                    // will be subtracted immediately after break of loop
                    fv.slide(fv.getWindowLength() - 1);
                    // TODO: optimize away an unnecessary expensive compact
                    // operation here while we only have 1 byte to compact,
                    // before reading in more data (if we're at the last block)
                    rolling = Rolling.compute(fv.getArray(), fv.getStartOffset(), fv.getWindowLength());
                    localChunkMd5sum = null;
                    break;
                }
            }
            
            rolling = Rolling.subtract(rolling, fv.getWindowLength(), fv.valueAt(fv.getStartOffset()));
            
            if (fv.isFull()) {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer("view is full " + fv);
                }
                this.sendDataFrom(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
                sizeLiteral += fv.getTotalBytes();
                fileDigest.update(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
                fv.setMarkRelativeToStart(fv.getWindowLength());
                fv.slide(fv.getWindowLength());
            } else {
                fv.slide(1);
            }
            
            // i.e. not at the end of the file
            if (fv.getWindowLength() == peerChecksum.getHeader().getBlockLength()) {
                rolling = Rolling.add(rolling, fv.valueAt(fv.getEndOffset()));
            }
        }
        
        this.sendDataFrom(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
        sizeLiteral += fv.getTotalBytes();
        fileDigest.update(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
        this.duplexChannel.putInt(0);
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("%d%% match: matched %d bytes, sent %d" + " bytes (file size %d bytes) %s", Math.round(100 * ((float) sizeMatch / (sizeMatch + sizeLiteral))), sizeMatch,
                    sizeLiteral, fileSize, fv));
        }
        
        this.stats.totalLiteralSize += sizeLiteral;
        this.stats.totalMatchedSize += sizeMatch;
        assert sizeLiteral + sizeMatch == fileSize;
        return fileDigest.digest();
    }
    
    private void sendSegmentDone() throws ChannelException {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("sending segment done");
        }
        this.duplexChannel.putByte((byte) 0);
    }
    
    private void sendStatistics(Statistics stats) throws ChannelException {
        this.sendEncodedLong(stats.getTotalBytesRead(), 3);
        this.sendEncodedLong(stats.getTotalBytesWritten(), 3);
        this.sendEncodedLong(stats.getTotalFileSize(), 3);
        this.sendEncodedLong(stats.getFileListBuildTime(), 3);
        this.sendEncodedLong(stats.getFileListTransferTime(), 3);
    }
    
    private void sendUserId(int uid) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending user id " + uid);
        }
        this.sendEncodedInt(uid);
    }
    
    private void sendUserList() throws ChannelException {
        for (User user : this.transferredUserNames) {
            assert user.getId() != User.ROOT.getId();
            this.sendUserId(user.getId());
            this.sendUserName(user.getName());
        }
        this.sendEncodedInt(0);
    }
    
    private void sendUserName(String name) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending user name " + name);
        }
        ByteBuffer buf = ByteBuffer.wrap(this.characterEncoder.encode(name));
        // unlikely scenario, we could also recover from this (by truncating or
        // falling back to nobody)
        if (buf.remaining() > 0xFF) {
            throw new IllegalStateException(String.format("encoded length of user name %s is %d, which is larger than " + "what fits in a byte (255)", name, buf.remaining()));
        }
        this.duplexChannel.putByte((byte) buf.remaining());
        this.duplexChannel.put(buf);
    }
    
    private void setFileAttributeManager(FileSystem fs) {
        this.fileAttributeManager = FileAttributeManagerFactory.getMostPerformant(fs, this.preserveUser, this.preserveGroup, this.preserveDevices, this.preserveSpecials,
                this.numericIds, this.defaultUser, this.defaultGroup, this.defaultFilePermissions, this.defaultDirectoryPermissions);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("fileAttributeManager=" + this.fileAttributeManager);
        }
    }
    
    private void setIsTransferred(int index) {
        this.transferred.set(index);
    }
    
    private byte[] skipMatchSendData(FileView view, long fileSize) throws ChannelException {
        MessageDigest fileDigest = MD5.newInstance();
        long bytesSent = 0;
        while (view.getWindowLength() > 0) {
            this.sendDataFrom(view.getArray(), view.getStartOffset(), view.getWindowLength());
            bytesSent += view.getWindowLength();
            fileDigest.update(view.getArray(), view.getStartOffset(), view.getWindowLength());
            view.slide(view.getWindowLength());
        }
        this.stats.totalLiteralSize += fileSize;
        this.duplexChannel.putInt(0);
        assert bytesSent == fileSize;
        return fileDigest.digest();
    }
    
    /**
     * @throws TextConversionException
     * @throws IOException
     */
    private LocatableFileInfo statAndEncode(Path path) throws IOException {
        RsyncFileAttributes attrs = this.fileAttributeManager.stat(path);
        String fileName = path.getFileName().toString();
        // throws TextConversionException
        byte[] nameBytes = this.characterEncoder.encode(fileName);
        
        if (attrs.isRegularFile() || attrs.isDirectory()) {
            return new LocatableFileInfoImpl(fileName, nameBytes, attrs, path);
        } else if (this.preserveLinks && attrs.isSymbolicLink()) {
            String linkTarget = FileOps.readLinkTarget(path).toString();
            return new LocatableSymlinkInfoImpl(fileName, nameBytes, attrs, linkTarget, path);
        } else if (this.preserveDevices && (attrs.isBlockDevice() || attrs.isCharacterDevice())) {
            throw new IOException(String.format("unable to retrieve major and minor ID of %s %s", FileOps.fileTypeToString(attrs.getMode()), path));
        } else if (this.preserveSpecials && (attrs.isFifo() || attrs.isSocket())) {
            throw new IOException(String.format("unable to retrieve major ID of %s %s", FileOps.fileTypeToString(attrs.getMode()), path));
        }
        throw new AssertionError(attrs + " " + this);
    }
    
    public Statistics statistics() {
        return this.stats;
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
        return String.format(
                "%s(" + "isExitAfterEOF=%b, " + "isExitEarlyIfEmptyList=%b, " + "interruptible=%b, " + "isNumericIds=%b, " + "isPreserveDevices=%b, " + "isPreserveLinks=%b, "
                        + "isPreserveSpecials=%b, " + "isPreserveUser=%b, " + "isPreserveGroup=%b, " + "isSafeFileList=%b, " + "isSendStatistics=%b, " + "checksumSeed=%s, " + "fileSelection=%s, "
                        + "filterMode=%s, " + "sourceFiles=%s" + ")",
                this.getClass().getSimpleName(), this.exitAfterEOF, this.exitEarlyIfEmptyList, this.interruptible, this.numericIds, this.preserveDevices, this.preserveLinks,
                this.preserveSpecials, this.preserveUser, this.preserveGroup, this.safeFileList, this.sendStatistics, Text.bytesToString(this.checksumSeed), this.fileSelection,
                this.filterMode, this.sourceFiles);
    }
}
