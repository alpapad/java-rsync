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
package com.github.java.rsync.internal.session;

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

import com.github.java.rsync.FileSelection;
import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.Statistics;
import com.github.java.rsync.attr.DeviceInfo;
import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.LocatableFileInfo;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.SymlinkInfo;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.channels.AutoFlushableRsyncDuplexChannel;
import com.github.java.rsync.internal.channels.ChannelEOFException;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.channels.Message;
import com.github.java.rsync.internal.channels.MessageCode;
import com.github.java.rsync.internal.channels.MessageHandler;
import com.github.java.rsync.internal.channels.RsyncInChannel;
import com.github.java.rsync.internal.channels.RsyncOutChannel;
import com.github.java.rsync.internal.io.FileView;
import com.github.java.rsync.internal.io.FileViewException;
import com.github.java.rsync.internal.io.FileViewNotFound;
import com.github.java.rsync.internal.io.FileViewOpenFailed;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.text.TextConversionException;
import com.github.java.rsync.internal.text.TextDecoder;
import com.github.java.rsync.internal.text.TextEncoder;
import com.github.java.rsync.internal.util.ArgumentParsingError;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.FileOps;
import com.github.java.rsync.internal.util.MD5;
import com.github.java.rsync.internal.util.PathOps;
import com.github.java.rsync.internal.util.Rolling;
import com.github.java.rsync.internal.util.RuntimeInterruptException;
import com.github.java.rsync.internal.util.StatusResult;

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
        private boolean exitAfterEOF;
        private boolean exitEarlyIfEmptyList;
        private FileSelection fileSelection = FileSelection.EXACT;
        private FilterMode filterMode = FilterMode.NONE;
        private FilterRuleConfiguration filterRuleConfiguration;
        private final ReadableByteChannel in;
        private boolean interruptible = true;
        private boolean numericIds;
        private final WritableByteChannel out;
        private boolean preserveDevices;
        private boolean preserveGroup;
        private boolean preserveLinks;
        private boolean preserveSpecials;
        private boolean preserveUser;
        private boolean safeFileList = true;

        private boolean sendStatistics;

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

        public Builder filterRuleConfiguration(FilterRuleConfiguration filterRuleConfiguration) {
            this.filterRuleConfiguration = filterRuleConfiguration;
            return this;
        }

        public Builder isExitEarlyIfEmptyList(boolean isExitEarlyIfEmptyList) {
            exitEarlyIfEmptyList = isExitEarlyIfEmptyList;
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

        public Builder isPreserveSpecials(boolean isPreserveSpecials) {
            preserveSpecials = isPreserveSpecials;
            return this;
        }

        public Builder isPreserveUser(boolean isPreserveUser) {
            preserveUser = isPreserveUser;
            return this;
        }

        public Builder isSafeFileList(boolean isSafeFileList) {
            safeFileList = isSafeFileList;
            return this;
        }
    }

    private static final int CHUNK_SIZE = 8 * 1024;
    private static final int INPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    private static final Logger LOG = Logger.getLogger(Sender.class.getName());
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
    private final boolean exitAfterEOF;
    private final boolean exitEarlyIfEmptyList;
    private FileAttributeManager fileAttributeManager;
    private final FileInfoCache fileInfoCache = new FileInfoCache();
    private final FileSelection fileSelection;
    private final FilterMode filterMode;
    private final FilterRuleConfiguration filterRuleConfiguration;
    private final boolean interruptible;
    private int ioError;
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
        duplexChannel = new AutoFlushableRsyncDuplexChannel(new RsyncInChannel(builder.in, this, INPUT_CHANNEL_BUF_SIZE), new RsyncOutChannel(builder.out, OUTPUT_CHANNEL_BUF_SIZE));
        exitAfterEOF = builder.exitAfterEOF;
        exitEarlyIfEmptyList = builder.exitEarlyIfEmptyList;
        interruptible = builder.interruptible;
        preserveDevices = builder.preserveDevices;
        preserveLinks = builder.preserveLinks;
        preserveSpecials = builder.preserveSpecials;
        preserveUser = builder.preserveUser;
        preserveGroup = builder.preserveGroup;
        numericIds = builder.numericIds;
        safeFileList = builder.safeFileList;
        sendStatistics = builder.sendStatistics;
        checksumSeed = builder.checksumSeed;
        fileSelection = builder.fileSelection;
        filterMode = builder.filterMode;
        filterRuleConfiguration = builder.filterRuleConfiguration;
        sourceFiles = builder.sourceFiles;
        characterDecoder = TextDecoder.newStrict(builder.charset);
        characterEncoder = TextEncoder.newStrict(builder.charset);
        defaultUser = builder.defaultUser;
        defaultGroup = builder.defaultGroup;
        defaultFilePermissions = builder.defaultFilePermissions;
        defaultDirectoryPermissions = builder.defaultDirectoryPermissions;
    }

    @Override
    public Boolean call() throws ChannelException, InterruptedException, RsyncProtocolException {
        Filelist fileList = new Filelist(fileSelection == FileSelection.RECURSE, false);
        FilterRuleConfiguration filterRuleConfiguration;
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(toString());
            }

            if (filterMode == FilterMode.RECEIVE) {
                // read remote filter rules if server
                try {
                    filterRuleConfiguration = new FilterRuleConfiguration(receiveFilterRules());
                } catch (ArgumentParsingError e) {
                    throw new RsyncProtocolException(e);
                }
            } else if (filterMode == FilterMode.SEND) {
                filterRuleConfiguration = this.filterRuleConfiguration;
                sendFilterRules();
            } else {
                try {
                    filterRuleConfiguration = new FilterRuleConfiguration(Collections.emptyList());
                } catch (ArgumentParsingError e) {
                    throw new RsyncProtocolException(e);
                }
            }

            long t1 = System.currentTimeMillis();

            StatusResult<List<FileInfo>> expandResult = initialExpand(sourceFiles, filterRuleConfiguration);
            boolean isInitialListOK = expandResult.isOK();
            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);
            builder.addAll(expandResult.getValue());
            Filelist.Segment initialSegment = fileList.newSegment(builder);
            long numBytesWritten = duplexChannel.numBytesWritten();
            for (FileInfo f : initialSegment.getFiles()) {
                sendFileMetaData((LocatableFileInfo) f);
            }
            long t2 = System.currentTimeMillis();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("expanded segment: " + initialSegment.toString());
            }
            if (isInitialListOK) {
                sendSegmentDone();
            } else {
                sendFileListErrorNotification();
            }
            long t3 = System.currentTimeMillis();

            if (preserveUser && !numericIds && fileSelection != FileSelection.RECURSE) {
                sendUserList();
            }
            if (preserveGroup && !numericIds && fileSelection != FileSelection.RECURSE) {
                sendGroupList();
            }

            stats.fileListBuildTime = Math.max(1, t2 - t1);
            stats.fileListTransferTime = Math.max(0, t3 - t2);
            long segmentSize = duplexChannel.numBytesWritten() - numBytesWritten;
            stats.totalFileListSize += segmentSize;
            if (!safeFileList && !isInitialListOK) {
                sendIntMessage(MessageCode.IO_ERROR, IoError.GENERAL);
            }

            if (initialSegment.isFinished() && exitEarlyIfEmptyList) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("empty file list - exiting early");
                }
                if (fileSelection == FileSelection.RECURSE) {
                    duplexChannel.encodeIndex(Filelist.EOF);
                }
                duplexChannel.flush();
                if (exitAfterEOF) {
                    readAllMessagesUntilEOF();
                }
                return isInitialListOK && ioError == 0;
            }

            int ioError = sendFiles(fileList, filterRuleConfiguration);
            if (ioError != 0) {
                sendIntMessage(MessageCode.IO_ERROR, ioError);
            }
            duplexChannel.encodeIndex(Filelist.DONE);

            // we update the statistics in finally clause to guarantee that the
            // statistics are updated even if there's an error
            if (sendStatistics) {
                stats.totalFileSize = fileList.getTotalFileSize();
                stats.totalBytesRead = duplexChannel.numBytesRead();
                stats.totalBytesWritten = duplexChannel.numBytesWritten();
                stats.numFiles = fileList.getNumFiles();
                sendStatistics(stats);
            }

            int index = duplexChannel.decodeIndex();
            if (index != Filelist.DONE) {
                throw new RsyncProtocolException(String.format("Invalid packet at end of run (%d)", index));
            }
            if (exitAfterEOF) {
                readAllMessagesUntilEOF();
            }
            return isInitialListOK && (ioError | this.ioError) == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            stats.totalFileSize = fileList.getTotalFileSize();
            stats.totalBytesRead = duplexChannel.numBytesRead();
            stats.totalBytesWritten = duplexChannel.numBytesWritten();
            stats.numFiles = fileList.getNumFiles();
        }
    }

    @Override
    public void closeChannel() throws ChannelException {
        duplexChannel.close();
    }

    private StatusResult<List<FileInfo>> expand(LocatableFileInfo directory, FilterRuleConfiguration parentFilterRuleConfiguration) throws ChannelException {
        assert directory != null;

        List<FileInfo> fileset = new ArrayList<>();
        boolean isOK = true;
        final Path dir = directory.getPath();
        final Path localDir = localPathTo(directory);
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
                    duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    isOK = false;
                    continue;
                }

                RsyncFileAttributes attrs;
                try {
                    attrs = fileAttributeManager.stat(entry);
                } catch (IOException e) {
                    String msg = String.format("Failed to stat %s: %s", entry, e.getMessage());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                    isOK = false;
                    continue;
                }

                Path relativePath = localDir.relativize(entry).normalize();
                String relativePathName = Text.withSlashAsPathSepator(relativePath);
                byte[] pathNameBytes = characterEncoder.encodeOrNull(relativePathName);
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
                    if (preserveLinks && attrs.isSymbolicLink()) {
                        Path symlinkTarget = FileOps.readLinkTarget(entry);
                        f = new LocatableSymlinkInfoImpl(relativePathName, pathNameBytes, attrs, symlinkTarget.toString(), entry);
                    } else if (preserveDevices && (attrs.isBlockDevice() || attrs.isCharacterDevice())) {
                        String msg = String.format("unable to retrieve major and minor ID of " + "%s %s", FileOps.fileTypeToString(attrs.getMode()), entry);
                        if (LOG.isLoggable(Level.WARNING)) {
                            LOG.warning(msg);
                        }
                        duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                        isOK = false;
                        continue;
                    } else if (preserveSpecials && (attrs.isFifo() || attrs.isSocket())) {
                        String msg = String.format("unable to retrieve major ID of %s %s", FileOps.fileTypeToString(attrs.getMode()), entry);
                        if (LOG.isLoggable(Level.WARNING)) {
                            LOG.warning(msg);
                        }
                        duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
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
                    String msg = String.format("Failed to encode %s using %s", relativePathName, characterEncoder.charset());
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning(msg);
                    }
                    duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
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
            duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
            isOK = false;
        }
        return new StatusResult<>(isOK, fileset);
    }

    private StatusResult<Integer> expandAndSendSegments(Filelist fileList, int limit, FilterRuleConfiguration parentFilterRuleConfiguration) throws ChannelException {
        boolean isOK = true;
        int numFilesSent = 0;
        int numSegmentsSent = 0;
        long numBytesWritten = duplexChannel.numBytesWritten();

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("expanding segments until at least %d " + "files have been sent", limit));
        }

        while (fileList.isExpandable() && numFilesSent < limit) {

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("sending segment index %d (as %d)", curSegmentIndex, Filelist.OFFSET - curSegmentIndex));
            }

            assert curSegmentIndex >= 0;
            LocatableFileInfo directory = (LocatableFileInfo) fileList.getStubDirectoryOrNull(curSegmentIndex);
            assert directory != null;
            duplexChannel.encodeIndex(Filelist.OFFSET - curSegmentIndex);

            StatusResult<List<FileInfo>> expandResult = expand(directory, parentFilterRuleConfiguration);
            boolean isExpandOK = expandResult.isOK();
            if (!isExpandOK && LOG.isLoggable(Level.WARNING)) {
                LOG.warning("initial file list expansion returned an error");
            }

            Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(directory);
            builder.addAll(expandResult.getValue());
            Filelist.Segment segment = fileList.newSegment(builder);

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("expanded segment with segment index" + " %d", curSegmentIndex));
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer(segment.toString());
                }
            }

            for (FileInfo fileInfo : segment.getFiles()) {
                sendFileMetaData((LocatableFileInfo) fileInfo);
                numFilesSent++;
            }

            if (isExpandOK) {
                sendSegmentDone();
            } else {
                // NOTE: once an error happens for native it will send an error
                // for each file list segment for the same loop block - we don't
                isOK = false;
                sendFileListErrorNotification();
            }
            curSegmentIndex++;
            numSegmentsSent++;
        }

        long segmentSize = duplexChannel.numBytesWritten() - numBytesWritten;
        stats.totalFileListSize += segmentSize;

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
                ioError |= message.getPayload().getInt();
                break;
            case ERROR:
            case ERROR_XFER:
                ioError |= IoError.TRANSFER; // fall through
            case INFO:
            case WARNING:
            case LOG:
                if (LOG.isLoggable(message.logLevelOrNull())) {
                    printMessage(message);
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
                if (fileAttributeManager == null) {
                    setFileAttributeManager(p.getFileSystem());
                }

                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("expanding " + p);
                }

                LocatableFileInfo fileInfo = statAndEncode(p);

                if (fileSelection == FileSelection.EXACT && fileInfo.getAttributes().isDirectory()) {
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
                        StatusResult<List<FileInfo>> expandResult = expand(fileInfo, parentFilterRuleConfiguration);
                        isOK = isOK && expandResult.isOK();
                        for (FileInfo f2 : expandResult.getValue()) {
                            fileset.add(f2);
                        }
                        curSegmentIndex++;
                    }
                }
            } catch (IOException e) {
                String msg = String.format("Failed to add %s to initial file " + "list: %s", p, e);
                if (LOG.isLoggable(Level.WARNING)) {
                    LOG.warning(msg);
                }
                duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                isOK = false;
            } catch (TextConversionException e) {
                String msg = String.format("Failed to encode %s using %s", p, characterEncoder.charset());
                if (LOG.isLoggable(Level.WARNING)) {
                    LOG.warning(msg);
                }
                duplexChannel.putMessage(toMessage(MessageCode.ERROR_XFER, msg + '\n'));
                isOK = false;
            }
        }
        return new StatusResult<>(isOK, fileset);
    }

    @Override
    public boolean isInterruptible() {
        return interruptible;
    }

    private boolean isTransferred(int index) {
        return transferred.get(index);
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
            String text = characterDecoder.decode(message.getPayload());
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
            byte dummy = duplexChannel.getByte();
            // we're not expected to get this far, getByte should throw
            // ChannelEOFException
            ByteBuffer buf = ByteBuffer.allocate(1024);
            try {
                buf.put(dummy);
                while (buf.hasRemaining()) {
                    dummy = duplexChannel.getByte();
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
        return Connection.receiveChecksumHeader(duplexChannel);
    }
    
    private Checksum receiveChecksumsFor(Checksum.Header header) throws ChannelException {
        Checksum checksum = new Checksum(header);
        for (int i = 0; i < header.getChunkCount(); i++) {
            int rolling = duplexChannel.getInt();
            byte[] md5sum = new byte[header.getDigestLength()];
            duplexChannel.get(md5sum, 0, md5sum.length);
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

            while ((numBytesToRead = duplexChannel.getInt()) > 0) {
                ByteBuffer buf = duplexChannel.get(numBytesToRead);
                list.add(characterDecoder.decode(buf));
            }

            return list;

        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }

    private void sendChecksumHeader(Checksum.Header header) throws ChannelException {
        Connection.sendChecksumHeader(duplexChannel, header);
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
            duplexChannel.putInt(len);
            duplexChannel.put(buf, currentOffset, len);
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
        sendEncodedLong(i, 1);
    }

    private void sendEncodedLong(long l, int minBytes) throws ChannelException {
        ByteBuffer b = IntegerCoder.encodeLong(l, minBytes);
        duplexChannel.put(b);
    }

    private void sendFileListErrorNotification() throws ChannelException {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("sending file list error notification to peer");
        }
        if (safeFileList) {
            duplexChannel.putChar((char) (0xFFFF & (TransmitFlags.EXTENDED_FLAGS | TransmitFlags.IO_ERROR_ENDLIST)));
            sendEncodedInt(IoError.GENERAL);
        } else {
            duplexChannel.putByte((byte) 0);
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

        if (preserveDevices && fileInfo instanceof DeviceInfo && (attrs.isBlockDevice() || attrs.isCharacterDevice())) {
            DeviceInfo dev = (DeviceInfo) fileInfo;
            if (dev.getMajor() == fileInfoCache.getPrevMajor()) {
                xflags |= TransmitFlags.SAME_RDEV_MAJOR;
            } else {
                fileInfoCache.setPrevMajor(dev.getMajor());
            }
        } else if (preserveSpecials && fileInfo instanceof DeviceInfo && (attrs.isFifo() || attrs.isSocket())) {
            xflags |= TransmitFlags.SAME_RDEV_MAJOR;
        }

        int mode = attrs.getMode();
        if (mode == fileInfoCache.getPrevMode()) {
            xflags |= TransmitFlags.SAME_MODE;
        } else {
            fileInfoCache.setPrevMode(mode);
        }

        User user = fileInfo.getAttributes().getUser();
        if (preserveUser && !user.equals(fileInfoCache.getPrevUser())) {
            fileInfoCache.setPrevUser(user);
            if (!numericIds && !user.equals(User.ROOT)) {
                if (fileSelection == FileSelection.RECURSE && !transferredUserNames.contains(user)) {
                    xflags |= TransmitFlags.USER_NAME_FOLLOWS;
                } // else send in batch later
                transferredUserNames.add(user);
            }
        } else {
            xflags |= TransmitFlags.SAME_UID;
        }

        Group group = fileInfo.getAttributes().getGroup();
        if (preserveGroup && !group.equals(fileInfoCache.getPrevGroup())) {
            fileInfoCache.setPrevGroup(group);
            if (!numericIds && !group.equals(Group.ROOT)) {
                if (fileSelection == FileSelection.RECURSE && !transferredGroupNames.contains(group)) {
                    xflags |= TransmitFlags.GROUP_NAME_FOLLOWS;
                } // else send in batch later
                transferredGroupNames.add(group);
            }
        } else {
            xflags |= TransmitFlags.SAME_GID;
        }

        long lastModified = attrs.lastModifiedTime();
        if (lastModified == fileInfoCache.getPrevLastModified()) {
            xflags |= TransmitFlags.SAME_TIME;
        } else {
            fileInfoCache.setPrevLastModified(lastModified);
        }

        byte[] fileNameBytes = fileInfo.getPathName().getBytes(characterEncoder.charset());
        int commonPrefixLength = lengthOfLargestCommonPrefix(fileInfoCache.getPrevFileNameBytes(), fileNameBytes);
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
        fileInfoCache.setPrevFileNameBytes(fileNameBytes);

        if (xflags == 0 && !attrs.isDirectory()) {
            xflags |= TransmitFlags.TOP_DIR;
        }
        if (xflags == 0 || (xflags & 0xFF00) != 0) {
            xflags |= TransmitFlags.EXTENDED_FLAGS;
            duplexChannel.putChar(xflags);
        } else {
            duplexChannel.putByte((byte) xflags);
        }
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sent flags " + Integer.toBinaryString(xflags));
        }

        if ((xflags & TransmitFlags.SAME_NAME) != 0) {
            duplexChannel.putByte((byte) numPrefixBytes);
        }

        if ((xflags & TransmitFlags.LONG_NAME) != 0) {
            sendEncodedInt(numSuffixBytes);
        } else {
            duplexChannel.putByte((byte) numSuffixBytes);
        }
        duplexChannel.put(ByteBuffer.wrap(suffixBytes));

        sendEncodedLong(attrs.getSize(), 3);

        if ((xflags & TransmitFlags.SAME_TIME) == 0) {
            sendEncodedLong(lastModified, 4);
        }

        if ((xflags & TransmitFlags.SAME_MODE) == 0) {
            duplexChannel.putInt(mode);
        }

        if (preserveUser && (xflags & TransmitFlags.SAME_UID) == 0) {
            sendUserId(user.getId());
            if ((xflags & TransmitFlags.USER_NAME_FOLLOWS) != 0) {
                sendUserName(user.getName());
            }
        }

        if (preserveGroup && (xflags & TransmitFlags.SAME_GID) == 0) {
            sendGroupId(group.getId());
            if ((xflags & TransmitFlags.GROUP_NAME_FOLLOWS) != 0) {
                sendGroupName(group.getName());
            }
        }

        if ((preserveDevices || preserveSpecials) && fileInfo instanceof DeviceInfo) {
            DeviceInfo dev = (DeviceInfo) fileInfo;
            if ((xflags & TransmitFlags.SAME_RDEV_MAJOR) == 0) {
                sendEncodedInt(dev.getMajor());
            }
            sendEncodedInt(dev.getMinor());
        } else if (preserveLinks && fileInfo instanceof SymlinkInfo) {
            String symlinkTarget = ((SymlinkInfo) fileInfo).getTargetPathName();
            byte[] symlinkTargetBytes = characterEncoder.encode(symlinkTarget);
            sendEncodedInt(symlinkTargetBytes.length);
            duplexChannel.put(ByteBuffer.wrap(symlinkTargetBytes));
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
                StatusResult<Integer> res = expandAndSendSegments(fileList, lim, parentFilterRuleConfiguration);
                numFilesInTransit += res.getValue();
                if (!res.isOK()) {
                    if (LOG.isLoggable(Level.WARNING)) {
                        LOG.warning("got I/O error during file list " + "expansion, notifying peer");
                    }
                    ioError |= IoError.GENERAL;
                    sendIntMessage(MessageCode.IO_ERROR, ioError);
                }
            }
            if (fileSelection == FileSelection.RECURSE && !fileList.isExpandable() && !sentEOF) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("sending file list EOF");
                }
                duplexChannel.encodeIndex(Filelist.EOF);
                sentEOF = true;
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("num bytes buffered: %d, num bytes available to read: %d", duplexChannel.getNumBytesBuffered(), duplexChannel.numBytesAvailable()));
            }

            final int index = duplexChannel.decodeIndex();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received index " + index);
            }

            if (index == Filelist.DONE) {
                if (fileSelection == FileSelection.RECURSE && !fileList.isEmpty()) {
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
                        duplexChannel.encodeIndex(Filelist.DONE);
                    }
                    numFilesInTransit -= removed.getFiles().size();
                }
                if (fileSelection != FileSelection.RECURSE || fileList.isEmpty()) {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("phase transition %s -> %s", phase, phase.next()));
                    }
                    phase = phase.next();
                    if (phase != TransferPhase.STOP) {
                        duplexChannel.encodeIndex(Filelist.DONE);
                    }
                }
            } else if (index >= 0) {
                char iFlags = duplexChannel.getChar();
                if (!Item.isValidItem(iFlags)) {
                    throw new IllegalStateException(String.format("got flags %s - not supported", Integer.toBinaryString(iFlags)));
                }
                if ((iFlags & Item.TRANSFER) == 0) {
                    if (segment == null || segment.getFileWithIndexOrNull(index) == null) {
                        segment = fileList.getSegmentWith(index);
                    }
                    assert segment != null;
                    if (fileSelection == FileSelection.RECURSE && segment == null) {
                        throw new RsyncProtocolException(String.format("Received invalid file/directory index %d from " + "peer", index));
                    }
                    if (index != segment.getDirectoryIndex()) {
                        FileInfo removed = segment.remove(index);
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine(String.format("Deleting file/dir %s %d", removed, index));
                        }
                        numFilesInTransit--;
                    }
                    sendIndexAndIflags(index, iFlags);
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
                        if (isTransferred(index)) {
                            LOG.fine("Re-sending " + fileInfo);
                        } else {
                            LOG.fine("sending " + fileInfo);
                        }
                    }

                    Checksum.Header header = receiveChecksumHeader();
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("received peer checksum " + header);
                    }
                    Checksum checksum = receiveChecksumsFor(header);

                    boolean isNew = header.getBlockLength() == 0;
                    int blockSize = isNew ? FileView.DEFAULT_BLOCK_SIZE : header.getBlockLength();
                    int blockFactor = isNew ? 1 : 10;
                    long fileSize = fileInfo.getAttributes().getSize();

                    byte[] fileMD5sum = null;
                    try (FileView fv = new FileView(fileInfo.getPath(), fileInfo.getAttributes().getSize(), blockSize, blockSize * blockFactor)) {

                        sendIndexAndIflags(index, iFlags);
                        sendChecksumHeader(header);

                        if (isNew) {
                            fileMD5sum = skipMatchSendData(fv, fileSize);
                        } else {
                            fileMD5sum = sendMatchesAndData(fv, checksum, fileSize);
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
                        sendIntMessage(MessageCode.NO_SEND, index);
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
                    duplexChannel.put(fileMD5sum, 0, fileMD5sum.length);
                    setIsTransferred(index);

                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("sent %s (%d bytes)", fileInfo.getPath(), fileSize));
                    }

                    stats.numTransferredFiles++;
                    stats.totalTransferredSize += fileInfo.getAttributes().getSize();
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
        if (filterRuleConfiguration != null) {
            for (FilterRuleList.FilterRule rule : filterRuleConfiguration.getFilterRuleListForSending()._rules) {
                byte[] encodedRule = characterEncoder.encode(rule.toString());

                ByteBuffer buf = ByteBuffer.allocate(4 + encodedRule.length).order(ByteOrder.LITTLE_ENDIAN);
                buf.putInt(encodedRule.length);
                buf.put(encodedRule);
                buf.flip();
                duplexChannel.put(buf);
            }
        }

        // send stop signal
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0);
        buf.flip();
        duplexChannel.put(buf);
    }

    private void sendGroupId(int gid) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending group id " + gid);
        }
        sendEncodedInt(gid);
    }

    private void sendGroupList() throws ChannelException {
        for (Group group : transferredGroupNames) {
            assert group.getId() != Group.ROOT.getId();
            sendGroupId(group.getId());
            sendGroupName(group.getName());
        }
        sendEncodedInt(0);
    }

    private void sendGroupName(String name) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending group name " + name);
        }
        ByteBuffer buf = ByteBuffer.wrap(characterEncoder.encode(name));
        if (buf.remaining() > 0xFF) {
            throw new IllegalStateException(String.format("encoded length of group name %s is %d, which is larger than " + "what fits in a byte (255)", name, buf.remaining()));
        }
        duplexChannel.putByte((byte) buf.remaining());
        duplexChannel.put(buf);
    }

    private void sendIndexAndIflags(int index, char iFlags) throws ChannelException {
        if (!Item.isValidItem(iFlags)) {
            throw new IllegalStateException(String.format("got flags %s - not supported", Integer.toBinaryString(iFlags)));
        }
        duplexChannel.encodeIndex(index);
        duplexChannel.putChar(iFlags);
    }

    private void sendIntMessage(MessageCode code, int value) throws ChannelException {
        ByteBuffer payload = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        payload.putInt(0, value);
        Message message = new Message(code, payload);
        duplexChannel.putMessage(message);
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
                    chunkDigest.update(checksumSeed);
                    localChunkMd5sum = Arrays.copyOf(chunkDigest.digest(), chunk.getMd5Checksum().length);
                }

                if (Arrays.equals(localChunkMd5sum, chunk.getMd5Checksum())) {
                    if (LOG.isLoggable(Level.FINER)) {
                        LOG.finer(String.format("match %s == %s %s", MD5.md5DigestToString(localChunkMd5sum), MD5.md5DigestToString(chunk.getMd5Checksum()), fv));
                    }
                    sizeMatch += fv.getWindowLength();
                    sendDataFrom(fv.getArray(), fv.getMarkOffset(), fv.getNumBytesMarked());
                    sizeLiteral += fv.getNumBytesMarked();
                    fileDigest.update(fv.getArray(), fv.getMarkOffset(), fv.getTotalBytes());

                    duplexChannel.putInt(-(chunk.getChunkIndex() + 1));
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
                sendDataFrom(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
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

        sendDataFrom(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
        sizeLiteral += fv.getTotalBytes();
        fileDigest.update(fv.getArray(), fv.getFirstOffset(), fv.getTotalBytes());
        duplexChannel.putInt(0);

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("%d%% match: matched %d bytes, sent %d" + " bytes (file size %d bytes) %s", Math.round(100 * ((float) sizeMatch / (sizeMatch + sizeLiteral))), sizeMatch,
                    sizeLiteral, fileSize, fv));
        }

        stats.totalLiteralSize += sizeLiteral;
        stats.totalMatchedSize += sizeMatch;
        assert sizeLiteral + sizeMatch == fileSize;
        return fileDigest.digest();
    }

    private void sendSegmentDone() throws ChannelException {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("sending segment done");
        }
        duplexChannel.putByte((byte) 0);
    }

    private void sendStatistics(Statistics stats) throws ChannelException {
        sendEncodedLong(stats.getTotalBytesRead(), 3);
        sendEncodedLong(stats.getTotalBytesWritten(), 3);
        sendEncodedLong(stats.getTotalFileSize(), 3);
        sendEncodedLong(stats.getFileListBuildTime(), 3);
        sendEncodedLong(stats.getFileListTransferTime(), 3);
    }

    private void sendUserId(int uid) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending user id " + uid);
        }
        sendEncodedInt(uid);
    }

    private void sendUserList() throws ChannelException {
        for (User user : transferredUserNames) {
            assert user.getId() != User.ROOT.getId();
            sendUserId(user.getId());
            sendUserName(user.getName());
        }
        sendEncodedInt(0);
    }

    private void sendUserName(String name) throws ChannelException {
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("sending user name " + name);
        }
        ByteBuffer buf = ByteBuffer.wrap(characterEncoder.encode(name));
        // unlikely scenario, we could also recover from this (by truncating or
        // falling back to nobody)
        if (buf.remaining() > 0xFF) {
            throw new IllegalStateException(String.format("encoded length of user name %s is %d, which is larger than " + "what fits in a byte (255)", name, buf.remaining()));
        }
        duplexChannel.putByte((byte) buf.remaining());
        duplexChannel.put(buf);
    }

    private void setFileAttributeManager(FileSystem fs) {
        fileAttributeManager = FileAttributeManagerFactory.getMostPerformant(fs, preserveUser, preserveGroup, preserveDevices, preserveSpecials, numericIds, defaultUser, defaultGroup,
                defaultFilePermissions, defaultDirectoryPermissions);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("fileAttributeManager=" + fileAttributeManager);
        }
    }

    private void setIsTransferred(int index) {
        transferred.set(index);
    }

    private byte[] skipMatchSendData(FileView view, long fileSize) throws ChannelException {
        MessageDigest fileDigest = MD5.newInstance();
        long bytesSent = 0;
        while (view.getWindowLength() > 0) {
            sendDataFrom(view.getArray(), view.getStartOffset(), view.getWindowLength());
            bytesSent += view.getWindowLength();
            fileDigest.update(view.getArray(), view.getStartOffset(), view.getWindowLength());
            view.slide(view.getWindowLength());
        }
        stats.totalLiteralSize += fileSize;
        duplexChannel.putInt(0);
        assert bytesSent == fileSize;
        return fileDigest.digest();
    }

    /**
     * @throws TextConversionException
     * @throws IOException
     */
    private LocatableFileInfo statAndEncode(Path path) throws IOException {
        RsyncFileAttributes attrs = fileAttributeManager.stat(path);
        String fileName = path.getFileName().toString();
        // throws TextConversionException
        byte[] nameBytes = characterEncoder.encode(fileName);

        if (attrs.isRegularFile() || attrs.isDirectory()) {
            return new LocatableFileInfoImpl(fileName, nameBytes, attrs, path);
        } else if (preserveLinks && attrs.isSymbolicLink()) {
            String linkTarget = FileOps.readLinkTarget(path).toString();
            return new LocatableSymlinkInfoImpl(fileName, nameBytes, attrs, linkTarget, path);
        } else if (preserveDevices && (attrs.isBlockDevice() || attrs.isCharacterDevice())) {
            throw new IOException(String.format("unable to retrieve major and minor ID of %s %s", FileOps.fileTypeToString(attrs.getMode()), path));
        } else if (preserveSpecials && (attrs.isFifo() || attrs.isSocket())) {
            throw new IOException(String.format("unable to retrieve major ID of %s %s", FileOps.fileTypeToString(attrs.getMode()), path));
        }
        throw new AssertionError(attrs + " " + this);
    }

    public Statistics statistics() {
        return stats;
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
        return String.format(
                "%s(" + "isExitAfterEOF=%b, " + "isExitEarlyIfEmptyList=%b, " + "interruptible=%b, " + "isNumericIds=%b, " + "isPreserveDevices=%b, " + "isPreserveLinks=%b, "
                        + "isPreserveSpecials=%b, " + "isPreserveUser=%b, " + "isPreserveGroup=%b, " + "isSafeFileList=%b, " + "isSendStatistics=%b, " + "checksumSeed=%s, " + "fileSelection=%s, "
                        + "filterMode=%s, " + "sourceFiles=%s" + ")",
                this.getClass().getSimpleName(), exitAfterEOF, exitEarlyIfEmptyList, interruptible, numericIds, preserveDevices, preserveLinks, preserveSpecials, preserveUser, preserveGroup,
                safeFileList, sendStatistics, Text.bytesToString(checksumSeed), fileSelection, filterMode, sourceFiles);
    }
}
