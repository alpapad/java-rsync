/*
 * Processing of incoming file lists and file data from Sender
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
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.FileSelection;
import com.github.java.rsync.RsyncException;
import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.RsyncSecurityException;
import com.github.java.rsync.Statistics;
import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.LocatableFileInfo;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.channels.ChannelEOFException;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.channels.Message;
import com.github.java.rsync.internal.channels.MessageCode;
import com.github.java.rsync.internal.channels.MessageHandler;
import com.github.java.rsync.internal.channels.RsyncInChannel;
import com.github.java.rsync.internal.io.AutoDeletable;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.text.TextConversionException;
import com.github.java.rsync.internal.text.TextDecoder;
import com.github.java.rsync.internal.text.TextEncoder;
import com.github.java.rsync.internal.util.ArgumentParsingError;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.FileOps;
import com.github.java.rsync.internal.util.MD5;
import com.github.java.rsync.internal.util.PathOps;
import com.github.java.rsync.internal.util.RuntimeInterruptException;
import com.github.java.rsync.internal.util.Util;

public class Receiver implements RsyncTask, MessageHandler {
    public static class Builder {
        public static Builder newClient(Generator generator, ReadableByteChannel in, Path targetPath) {
            return new Builder(generator, in, targetPath).isReceiveStatistics(true).isExitEarlyIfEmptyList(true).isExitAfterEOF(true);
        }
        
        public static Builder newListing(Generator generator, ReadableByteChannel in) {
            return new Builder(generator, in, null);
        }
        
        public static Builder newServer(Generator generator, ReadableByteChannel in, Path targetPath) {
            return new Builder(generator, in, targetPath)//
                    .isReceiveStatistics(false)//
                    .isExitEarlyIfEmptyList(false)//
                    .isExitAfterEOF(false);
        }
        
        public int defaultDirectoryPermissions = Environment.DEFAULT_DIR_PERMS;
        public int defaultFilePermissions = Environment.DEFAULT_FILE_PERMS;
        public Group defaultGroup = Group.NOBODY;
        public User defaultUser = User.NOBODY;
        private FilterMode filterMode = FilterMode.NONE;
        
        private final Generator generator;
        
        private final ReadableByteChannel in;
        private boolean deferWrite;
        private boolean exitAfterEOF;
        private boolean exitEarlyIfEmptyList;
        
        private boolean receiveStatistics;
        
        private boolean safeFileList = true;
        
        private final Path targetPath;
        
        public Builder(Generator generator, ReadableByteChannel in, Path targetPath) {
            assert generator != null;
            assert in != null;
            assert targetPath == null || targetPath.isAbsolute();
            this.generator = generator;
            this.in = in;
            this.targetPath = targetPath;
        }
        
        public Receiver build() {
            return new Receiver(this);
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
        
        public Builder filterMode(FilterMode filterMode) {
            assert filterMode != null;
            this.filterMode = filterMode;
            return this;
        }

        public Builder isDeferWrite(boolean isDeferWrite) {
            this.deferWrite = isDeferWrite;
            return this;
        }
        
        public Builder isExitAfterEOF(boolean isExitAfterEOF) {
            this.exitAfterEOF = isExitAfterEOF;
            return this;
        }
        
        public Builder isExitEarlyIfEmptyList(boolean isExitEarlyIfEmptyList) {
            this.exitEarlyIfEmptyList = isExitEarlyIfEmptyList;
            return this;
        }
        
        public Builder isReceiveStatistics(boolean isReceiveStatistics) {
            this.receiveStatistics = isReceiveStatistics;
            return this;
        }
        
        public Builder isSafeFileList(boolean isSafeFileList) {
            this.safeFileList = isSafeFileList;
            return this;
        }
    }
    
    private static class FileInfoStub {
        private RsyncFileAttributes attrs;
        private int major = -1;
        private int minor = -1;
        private byte[] pathNameBytes;
        private String pathNameOrNull;
        private String symlinkTargetOrNull;
        
        @Override
        public String toString() {
            return String.format("%s(%s, %s)", this.getClass().getSimpleName(), this.pathNameOrNull, this.attrs);
        }
    }
    
    private interface PathResolver {
        /**
         * @throws RsyncSecurityException
         */
        Path fullPathOf(Path relativePath) throws RsyncSecurityException;
        
        /**
         * @throws InvalidPathException
         * @throws RsyncSecurityException
         */
        Path relativePathOf(String pathName) throws RsyncSecurityException;
    }
    
    private static final Logger LOG = Logger.getLogger(Receiver.class.getName());
    private static final int INPUT_CHANNEL_BUF_SIZE = 8 * 1024;
    
    private static int blockSize(int index, Checksum.Header checksumHeader) {
        if (index == checksumHeader.getChunkCount() - 1 && checksumHeader.getRemainder() != 0) {
            return checksumHeader.getRemainder();
        }
        return checksumHeader.getBlockLength();
    }
    
    private static FileInfo createFileInfo(String pathNameOrNull, byte[] pathNameBytes, RsyncFileAttributes attrs, Path pathOrNull, String symlinkTargetOrNull, int major, int minor) {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("creating fileinfo given: pathName=%s attrs=%s, path=%s, " + "symlinkTarget=%s, major=%d, minor=%d", pathNameOrNull, attrs, pathOrNull, symlinkTargetOrNull, major,
                    minor));
        }
        if (pathNameOrNull == null) { /* untransferrable */
            return new FileInfoImpl(pathNameOrNull, pathNameBytes, attrs);
        } else if ((attrs.isBlockDevice() || attrs.isCharacterDevice() || attrs.isFifo() || attrs.isSocket()) && major >= 0 && minor >= 0) {
            if (pathOrNull == null) {
                return new DeviceInfoImpl(pathNameOrNull, pathNameBytes, attrs, major, minor);
            }
            return new LocatableDeviceInfoImpl(pathNameOrNull, pathNameBytes, attrs, major, minor, pathOrNull);
        } else if (attrs.isSymbolicLink() && symlinkTargetOrNull != null) {
            if (pathOrNull == null) {
                return new SymlinkInfoImpl(pathNameOrNull, pathNameBytes, attrs, symlinkTargetOrNull);
            }
            return new LocatableSymlinkInfoImpl(pathNameOrNull, pathNameBytes, attrs, symlinkTargetOrNull, pathOrNull);
        }
        // Note: these might be symlinks or device files:
        if (pathOrNull == null) {
            return new FileInfoImpl(pathNameOrNull, pathNameBytes, attrs);
        }
        return new LocatableFileInfoImpl(pathNameOrNull, pathNameBytes, attrs, pathOrNull);
    }
    
    private final TextDecoder characterDecoder;
    private final TextEncoder characterEncoder;
    private final FileAttributeManager fileAttributeManager;
    private final FileInfoCache fileInfoCache = new FileInfoCache();
    private final Filelist fileList;
    private final FileSelection fileSelection;
    private final FilterMode filterMode;
    private FilterRuleConfiguration filterRuleConfiguration;
    
    private final Generator generator;
    private final RsyncInChannel in;
    private int ioError;
    private final boolean deferWrite;
    private final boolean exitAfterEOF;
    private final boolean exitEarlyIfEmptyList;
    private final boolean interruptible;
    private final boolean listOnly;
    private final boolean numericIds;
    private final boolean preserveDevices;
    private final boolean preserveGroup;
    private final boolean preserveLinks;
    private final boolean preservePermissions;
    private final boolean preserveSpecials;
    private final boolean preserveTimes;
    private final boolean preserveUser;
    private final boolean receiveStatistics;
    private final boolean safeFileList;
    private PathResolver pathResolver;
    private final Map<Integer, Group> recursiveGidGroupMap = new HashMap<>();
    
    private final Map<Integer, User> recursiveUidUserMap = new HashMap<>();
    private final SessionStatistics stats = new SessionStatistics();
    
    private final Path targetPath; // is null if file listing
    
    private final BitSet transferred = new BitSet();
    
    private Receiver(Builder builder) {
        this.deferWrite = builder.deferWrite;
        this.exitAfterEOF = builder.exitAfterEOF;
        this.exitEarlyIfEmptyList = builder.exitEarlyIfEmptyList;
        this.receiveStatistics = builder.receiveStatistics;
        this.safeFileList = builder.safeFileList;
        this.generator = builder.generator;
        this.fileList = this.generator.getFileList();
        this.interruptible = this.generator.isInterruptible();
        this.preserveDevices = this.generator.isPreserveDevices();
        this.preserveLinks = this.generator.isPreserveLinks();
        this.preservePermissions = this.generator.isPreservePermissions();
        this.preserveSpecials = this.generator.isPreserveSpecials();
        this.preserveTimes = this.generator.isPreserveTimes();
        this.preserveUser = this.generator.isPreserveUser();
        this.preserveGroup = this.generator.isPreserveGroup();
        this.numericIds = this.generator.isNumericIds();
        this.fileSelection = this.generator.getFileSelection();
        this.filterMode = builder.filterMode;
        this.in = new RsyncInChannel(builder.in, this, INPUT_CHANNEL_BUF_SIZE);
        
        this.targetPath = builder.targetPath;
        this.listOnly = this.targetPath == null;
        this.characterDecoder = TextDecoder.newStrict(this.generator.getCharset());
        this.characterEncoder = TextEncoder.newStrict(this.generator.getCharset());
        
        if (!this.listOnly) {
            this.fileAttributeManager = FileAttributeManagerFactory.getMostPerformant(this.targetPath.getFileSystem(), this.preserveUser, this.preserveGroup, this.preserveDevices,
                    this.preserveSpecials, this.numericIds, builder.defaultUser, builder.defaultGroup, builder.defaultFilePermissions, builder.defaultDirectoryPermissions);
            this.generator.setFileAttributeManager(this.fileAttributeManager);
        } else {
            this.fileAttributeManager = null;
        }
    }
    
    private void addGroupNameToStubs(Map<Integer, Group> gidGroupMap, List<FileInfoStub> stubs) throws RsyncProtocolException {
        for (FileInfoStub stub : stubs) {
            RsyncFileAttributes incompleteAttrs = stub.attrs;
            boolean isComplete = incompleteAttrs.getGroup().getName().length() > 0;
            if (isComplete) {
                throw new RsyncProtocolException(String.format("expected group name of %s to be the empty string", incompleteAttrs));
            }
            Group completeGroup = gidGroupMap.get(incompleteAttrs.getGroup().getId());
            if (completeGroup != null) {
                RsyncFileAttributes completeAttrs = new RsyncFileAttributes(incompleteAttrs.getMode(), incompleteAttrs.getSize(), incompleteAttrs.lastModifiedTime(), incompleteAttrs.getUser(),
                        completeGroup);
                stub.attrs = completeAttrs;
            }
        }
    }
    
    private void addUserNameToStubs(Map<Integer, User> uidUserMap, List<FileInfoStub> stubs) throws RsyncProtocolException {
        for (FileInfoStub stub : stubs) {
            RsyncFileAttributes incompleteAttrs = stub.attrs;
            boolean isComplete = incompleteAttrs.getUser().getName().length() > 0;
            if (isComplete) {
                throw new RsyncProtocolException(String.format("expected user name of %s to be the empty string", incompleteAttrs));
            }
            User completeUser = uidUserMap.get(incompleteAttrs.getUser().getId());
            if (completeUser != null) {
                RsyncFileAttributes completeAttrs = new RsyncFileAttributes(incompleteAttrs.getMode(), incompleteAttrs.getSize(), incompleteAttrs.lastModifiedTime(), completeUser,
                        incompleteAttrs.getGroup());
                stub.attrs = completeAttrs;
            }
        }
    }
    
    /**
     * @throws ChannelException       if there is a communication failure with peer
     * @throws RsyncProtocolException if peer does not conform to the rsync protocol
     * @throws RsyncSecurityException if peer misbehaves such that the integrity of
     *                                the application is impacted
     * @throws RsyncException         if: 1. failing to stat target (it is OK if it
     *                                does not exist though) 2. target does not
     *                                exist and failing to create missing
     *                                directories to it 3. target exists but is
     *                                neither a directory nor a file nor a symbolic
     *                                link (at least until there is proper support
     *                                for additional file types) 4. target is an
     *                                existing file and there are two or more source
     *                                files 5. target is an existing file and the
     *                                first source argument is an existing directory
     */
    @Override
    public Boolean call() throws RsyncException, InterruptedException {
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(this.toString());
            }
            if (this.filterMode == FilterMode.SEND) {
                this.sendFilterRules();
            } else if (this.filterMode == FilterMode.RECEIVE) {
                // receive filter rules if server
                try {
                    this.filterRuleConfiguration = new FilterRuleConfiguration(this.receiveFilterRules());
                } catch (ArgumentParsingError e) {
                    throw new RsyncProtocolException(e);
                }
            }
            
            if (!this.numericIds && this.fileSelection == FileSelection.RECURSE) {
                if (this.preserveUser) {
                    this.recursiveUidUserMap.put(User.ROOT.getId(), User.ROOT);
                }
                if (this.preserveGroup) {
                    this.recursiveGidGroupMap.put(Group.ROOT.getId(), Group.ROOT);
                }
            }
            
            Filelist.Segment initialSegment = this.receiveInitialSegment();
            
            if (initialSegment.isFinished() && this.exitEarlyIfEmptyList) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("empty file list %s - exiting " + "early", initialSegment));
                }
                if (this.fileSelection == FileSelection.RECURSE) {
                    int dummyIndex = this.in.decodeIndex();
                    if (dummyIndex != Filelist.EOF) {
                        throw new RsyncProtocolException(String.format("expected peer to send index %d (EOF), but " + "got %d", Filelist.EOF, dummyIndex));
                    }
                }
                // NOTE: we never _receive_ any statistics if initial file list
                // is empty
                if (this.exitAfterEOF) {
                    this.readAllMessagesUntilEOF();
                }
                return this.ioError == 0;
            }
            
            if (this.listOnly) {
                this.generator.listSegment(initialSegment);
            } else {
                this.generator.generateSegment(this.targetPath, initialSegment, this.filterRuleConfiguration);
                // Path targetPath = PathOps.get(this._targetPath); // throws
                // InvalidPathException
                
            }
            
            this.ioError |= this.receiveFiles();
            this.stats.numFiles = this.fileList.getNumFiles();
            if (this.receiveStatistics) {
                this.receiveStatistics();
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("(local) Total file size: %d bytes, Total bytes sent:" + " %d, Total bytes received: %d", this.fileList.getTotalFileSize(),
                            this.generator.getNumBytesWritten(), this.in.getNumBytesRead()));
                }
            }
            
            if (this.exitAfterEOF) {
                this.readAllMessagesUntilEOF();
            }
            return this.ioError == 0;
        } catch (RuntimeInterruptException e) {
            throw new InterruptedException();
        } finally {
            this.generator.stop();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("exit status %d", this.ioError));
            }
        }
    }
    
    @Override
    public void closeChannel() throws ChannelException {
        this.in.close();
    }
    
    private boolean combineDataToFile(FileChannel replicaOrNull, FileChannel target, Checksum.Header checksumHeader, MessageDigest md) throws IOException, ChannelException, RsyncProtocolException {
        assert target != null;
        assert checksumHeader != null;
        assert md != null;
        
        boolean isDeferrable = this.deferWrite && replicaOrNull != null;
        long sizeLiteral = 0;
        long sizeMatch = 0;
        int expectedIndex = 0;
        
        while (true) {
            final int token = this.in.getInt();
            if (token == 0) {
                break;
            }
            // token correlates to a matching block index
            if (token < 0) {
                // blockIndex >= 0 && blockIndex <= Integer.MAX_VALUE
                final int blockIndex = -(token + 1);
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(String.format("got matching block index %d", blockIndex));
                }
                if (blockIndex > checksumHeader.getChunkCount() - 1) {
                    throw new RsyncProtocolException(
                            String.format("Received invalid block index from peer %d, which is " + "out of range for the supposed number of blocks %d", blockIndex, checksumHeader.getChunkCount()));
                } else if (checksumHeader.getBlockLength() == 0) {
                    throw new RsyncProtocolException(String.format("Received a matching block index from peer %d when we" + " never sent any blocks to peer (checksum " + "blockLength = %d)",
                            blockIndex, checksumHeader.getBlockLength()));
                } else if (replicaOrNull == null) {
                    // or we could alternatively read zeroes from replica and
                    // have the correct file size in the end?
                    //
                    // i.e. generator sent file info to sender and sender
                    // replies with a match but now our replica is gone
                    continue;
                }
                
                sizeMatch += blockSize(blockIndex, checksumHeader);
                
                if (isDeferrable) {
                    if (blockIndex == expectedIndex) {
                        expectedIndex++;
                        continue;
                    }
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("defer write disabled since " + "%d != %d", blockIndex, expectedIndex));
                    }
                    isDeferrable = false;
                    for (int i = 0; i < expectedIndex; i++) {
                        this.copyFromReplicaAndUpdateDigest(replicaOrNull, i, target, md, checksumHeader);
                    }
                }
                this.copyFromReplicaAndUpdateDigest(replicaOrNull, blockIndex, target, md, checksumHeader);
            } else if (token > 0) { // receive literal data from peer:
                if (isDeferrable) {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("defer write disabled since " + "we got literal data %d", token));
                    }
                    isDeferrable = false;
                    for (int i = 0; i < expectedIndex; i++) {
                        this.copyFromReplicaAndUpdateDigest(replicaOrNull, i, target, md, checksumHeader);
                    }
                }
                int length = token;
                sizeLiteral += length;
                this.copyFromPeerAndUpdateDigest(target, length, md);
            }
        }
        
        // rare truncation of multiples of checksum blocks
        if (isDeferrable && expectedIndex != checksumHeader.getChunkCount()) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("defer write disabled since " + "expectedIndex %d != " + "checksumHeader.chunkCount() %d", expectedIndex, checksumHeader.getChunkCount()));
            }
            isDeferrable = false;
            for (int i = 0; i < expectedIndex; i++) {
                this.copyFromReplicaAndUpdateDigest(replicaOrNull, i, target, md, checksumHeader);
            }
        }
        if (isDeferrable) {
            // expectedIndex == checksumHeader.chunkCount()
            for (int i = 0; i < expectedIndex; i++) {
                ByteBuffer replicaBuf = this.readFromReplica(replicaOrNull, i, checksumHeader);
                md.update(replicaBuf);
            }
        }
        
        if (LOG.isLoggable(Level.FINE)) {
            if (this.deferWrite && replicaOrNull != null && !isDeferrable) {
                LOG.fine("defer write disabled");
            }
            LOG.fine(String.format("total bytes = %d, num matched bytes = " + "%d, num literal bytes = %d, %f%% match", sizeMatch + sizeLiteral, sizeMatch, sizeLiteral,
                    100 * sizeMatch / (float) (sizeMatch + sizeLiteral)));
        }
        this.stats.totalLiteralSize += sizeLiteral;
        this.stats.totalMatchedSize += sizeMatch;
        return isDeferrable;
    }
    
    private void copyFromPeerAndUpdateDigest(FileChannel target, int length, MessageDigest md) throws ChannelException {
        int bytesReceived = 0;
        while (bytesReceived < length) {
            int chunkSize = Math.min(INPUT_CHANNEL_BUF_SIZE, length - bytesReceived);
            ByteBuffer literalData = this.in.get(chunkSize);
            bytesReceived += chunkSize;
            literalData.mark();
            this.writeToFile(target, literalData);
            literalData.reset();
            md.update(literalData);
        }
    }
    
    private void copyFromReplicaAndUpdateDigest(FileChannel replica, int blockIndex, FileChannel target, MessageDigest md, Checksum.Header checksumHeader) throws IOException {
        ByteBuffer replicaBuf = this.readFromReplica(replica, blockIndex, checksumHeader);
        this.writeToFile(target, replicaBuf);
        // it's OK to rewind since the buffer is newly allocated with an initial
        // position of zero
        replicaBuf.rewind();
        md.update(replicaBuf);
    }
    
    private String decodePathName(byte[] pathNameBytes) throws InterruptedException, ChannelException {
        String pathNameOrNull = this.characterDecoder.decodeOrNull(pathNameBytes);
        if (pathNameOrNull == null) {
            try {
                this.generator.sendMessage(MessageCode.ERROR, String.format("Error: unable to decode path name of " + "%s using character set %s. Result " + "with illegal characters replaced: %s\n",
                        Text.bytesToString(pathNameBytes), this.characterDecoder.charset(), new String(pathNameBytes, this.characterDecoder.charset())));
            } catch (TextConversionException e) {
                if (LOG.isLoggable(Level.SEVERE)) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }
            this.ioError |= IoError.GENERAL;
            return null;
        }
        if (!this.listOnly) {
            String separator = this.targetPath.getFileSystem().getSeparator();
            if (!PathOps.isDirectoryStructurePreservable(separator, pathNameOrNull)) {
                try {
                    this.generator.sendMessage(MessageCode.ERROR,
                            String.format("Illegal file name. \"%s\" contains this " + "OS' path name separator \"%s\" and " + "cannot be stored and later retrieved " + "using the same name again\n",
                                    pathNameOrNull, separator));
                } catch (TextConversionException e) {
                    if (LOG.isLoggable(Level.SEVERE)) {
                        LOG.log(Level.SEVERE, "", e);
                    }
                }
                this.ioError |= IoError.GENERAL;
                return null;
            }
        }
        return pathNameOrNull;
    }
    
    private void discardData(Checksum.Header checksumHeader) throws ChannelException {
        long sizeLiteral = 0;
        long sizeMatch = 0;
        while (true) {
            int token = this.in.getInt();
            if (token == 0) {
                break;
            } else if (token > 0) {
                int numBytes = token;
                this.in.skip(numBytes);
                sizeLiteral += numBytes;
            } else {
                // blockIndex >= 0 && blockIndex <= Integer.MAX_VALUE
                final int blockIndex = -(token + 1);
                sizeMatch += blockSize(blockIndex, checksumHeader);
            }
        }
        this.stats.totalLiteralSize += sizeLiteral;
        this.stats.totalMatchedSize += sizeMatch;
    }
    
    /**
     * file -> non_existing == non_existing file -> existing_file == existing_file *
     * -> existing_dir == existing_dir/* * -> non_existing == non_existing/* * ->
     * existing_not_dir/ == fail * -> existing_other == fail file_a file_b ->
     * existing_not_dir == fail -r dir_a -> existing_not_dir == fail
     *
     * Note: one special side effect of rsync is that it treats empty dirs specially
     * (due to giving importance to the total amount of files in the _two_ initial
     * file lists), i.e.: $ mkdir empty_dir $ rsync -r empty_dir non_existing $ ls
     * non_existing $ rsync -r empty_dir non_existing $ ls non_existing empty_dir
     * yajsync does not try to mimic this
     *
     * @throws RsyncException if: 1. failing to stat target (it is OK if it does not
     *                        exist though) 2. if target does not exist and failing
     *                        to create missing directories to it, 3. if target
     *                        exists but is neither a directory nor a file nor a
     *                        symbolic link (at least until there is proper support
     *                        for additional file types). 4. if target is an
     *                        existing file and there are two or more source files.
     *                        5. if target is an existing file and the first source
     *                        argument is an existing directory.
     */
    private PathResolver getPathResolver(final List<FileInfoStub> stubs) throws RsyncException {
        RsyncFileAttributes attrs;
        try {
            attrs = this.fileAttributeManager.statIfExists(this.targetPath);
        } catch (IOException e) {
            throw new RsyncException(String.format("unable to stat %s: %s", this.targetPath, e));
        }
        
        boolean isTargetExisting = attrs != null;
        boolean isTargetExistingDir = isTargetExisting && attrs.isDirectory();
        boolean isTargetExistingFile = isTargetExisting && !attrs.isDirectory();
        boolean isSourceSingleFile = stubs.size() == 1 && !stubs.get(0).attrs.isDirectory();
        boolean isTargetNonExistingFile = !isTargetExisting && !this.targetPath.endsWith(Text.DOT);
        
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer(String.format("targetPath=%s attrs=%s isTargetExisting=%s " + "isSourceSingleFile=%s " + "isTargetNonExistingFile=%s " + "#stubs=%d", this.targetPath, attrs, isTargetExisting,
                    isSourceSingleFile, isTargetNonExistingFile, stubs.size()));
        }
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest(stubs.toString());
        }
        
        // -> targetPath
        if (isSourceSingleFile && isTargetNonExistingFile || isSourceSingleFile && isTargetExistingFile) {
            return new PathResolver() {
                @Override
                public Path fullPathOf(Path relativePath) {
                    return Receiver.this.targetPath;
                }
                
                @Override
                public Path relativePathOf(String pathName) {
                    FileSystem fs = Receiver.this.targetPath.getFileSystem();
                    return fs.getPath(stubs.get(0).pathNameOrNull);
                }
                
                @Override
                public String toString() {
                    return "PathResolver(Single Source)";
                }
            };
        }
        // -> targetPath/*
        if (isTargetExistingDir || !isTargetExisting) {
            if (!isTargetExisting) {
                if (LOG.isLoggable(Level.FINER)) {
                    LOG.finer("creating directory (with parents) " + this.targetPath);
                }
                try {
                    Files.createDirectories(this.targetPath);
                } catch (IOException e) {
                    throw new RsyncException(String.format("unable to create directory structure to %s: %s", this.targetPath, e));
                }
            }
            return new PathResolver() {
                
                @Override
                public Path fullPathOf(Path relativePath) throws RsyncSecurityException {
                    Path fullPath = Receiver.this.targetPath.resolve(relativePath).normalize();
                    if (!fullPath.startsWith(Receiver.this.targetPath.normalize())) {
                        throw new RsyncSecurityException(String.format("%s is outside of receiver destination dir %s", fullPath, Receiver.this.targetPath));
                    }
                    return fullPath;
                }

                @Override
                public Path relativePathOf(String pathName) throws RsyncSecurityException {
                    // throws InvalidPathException
                    FileSystem fs = Receiver.this.targetPath.getFileSystem();
                    Path relativePath = fs.getPath(pathName);
                    if (relativePath.isAbsolute()) {
                        throw new RsyncSecurityException(relativePath + " is absolute");
                    }
                    Path normalizedRelativePath = PathOps.normalizeStrict(relativePath);
                    return normalizedRelativePath;
                }
                
                @Override
                public String toString() {
                    return "PathResolver(Complex)";
                }
                
            };
        }
        
        if (isTargetExisting && !attrs.isDirectory() && !attrs.isRegularFile() && !attrs.isSymbolicLink()) {
            throw new RsyncException(String.format("refusing to overwrite existing target path %s which is " + "neither a file nor a directory (%s)", this.targetPath, attrs));
        }
        if (isTargetExistingFile && stubs.size() >= 2) {
            throw new RsyncException(String.format("refusing to copy source files %s into file %s " + "(%s)", stubs, this.targetPath, attrs));
        }
        if (isTargetExistingFile && stubs.size() == 1 && stubs.get(0).attrs.isDirectory()) {
            throw new RsyncException(String.format("refusing to recursively copy directory %s into " + "non-directory %s (%s)", stubs.get(0), this.targetPath, attrs));
        }
        throw new AssertionError(String.format("BUG: stubs=%s targetPath=%s attrs=%s", stubs, this.targetPath, attrs));
    }
    
    private Group getPreviousGroup() throws RsyncProtocolException {
        Group group = this.fileInfoCache.getPrevGroup();
        if (group == null) {
            if (this.preserveGroup) {
                throw new RsyncProtocolException("expecting to receive group " + "information from peer");
            }
            return Group.JVM_GROUP;
        }
        return group;
    }
    
    private User getPreviousUser() throws RsyncProtocolException {
        User user = this.fileInfoCache.getPrevUser();
        if (user == null) {
            if (this.preserveUser) {
                throw new RsyncProtocolException("expecting to receive user " + "information from peer");
            }
            return User.JVM_USER;
        }
        return user;
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
                this.generator.disableDelete();
                break;
            case NO_SEND:
                int index = message.getPayload().getInt();
                this.handleMessageNoSend(index);
                break;
            case ERROR: // store this as an IoError.TRANSFER
            case ERROR_XFER:
                this.ioError |= IoError.TRANSFER;
                this.generator.disableDelete(); // NOTE: fall through
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
    
    private void handleMessageNoSend(int index) throws RsyncProtocolException {
        try {
            if (index < 0) {
                throw new RsyncProtocolException(String.format("received illegal MSG_NO_SEND index: %d < 0", index));
            }
            this.generator.purgeFile(null, index);
        } catch (InterruptedException e) {
            throw new RuntimeInterruptException(e);
        }
    }
    
    @Override
    public boolean isInterruptible() {
        return this.interruptible;
    }
    
    private boolean isRemoteAndLocalFileIdentical(Path localFile, MessageDigest md, LocatableFileInfo fileInfo) throws ChannelException {
        long tempSize = localFile == null ? -1 : FileOps.sizeOf(localFile);
        byte[] md5sum = md.digest();
        byte[] peerMd5sum = new byte[md5sum.length];
        this.in.get(ByteBuffer.wrap(peerMd5sum));
        boolean isIdentical = tempSize == fileInfo.getAttributes().getSize() && Arrays.equals(md5sum, peerMd5sum);
        
        // isIdentical = isIdentical && Util.randomChance(0.25);
        
        if (LOG.isLoggable(Level.FINE)) {
            if (isIdentical) {
                LOG.fine(String.format("%s data received OK (remote and " + "local checksum is %s)", fileInfo, MD5.md5DigestToString(md5sum)));
            } else {
                LOG.fine(String.format("%s checksum/size mismatch : " + "our=%s (size=%d), peer=%s (size=%d)", fileInfo, MD5.md5DigestToString(md5sum), tempSize, MD5.md5DigestToString(peerMd5sum),
                        fileInfo.getAttributes().getSize()));
            }
        }
        return isIdentical;
    }
    
    private boolean isTransferred(int index) {
        return this.transferred.get(index);
    }
    
    private int matchData(Filelist.Segment segment, int index, LocatableFileInfo fileInfo, Checksum.Header checksumHeader, Path tempFile)
            throws ChannelException, InterruptedException, RsyncProtocolException {
        int ioError = 0;
        MessageDigest md = MD5.newInstance();
        Path resultFile = this.mergeDataFromPeerAndReplica(fileInfo, tempFile, checksumHeader, md);
        if (this.isRemoteAndLocalFileIdentical(resultFile, md, fileInfo)) {
            try {
                if (this.preservePermissions || this.preserveTimes || this.preserveUser || this.preserveGroup) {
                    this.updateAttrsIfDiffer(resultFile, fileInfo.getAttributes());
                }
                if (!this.deferWrite || !resultFile.equals(fileInfo.getPath())) {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("moving %s -> %s", resultFile, fileInfo.getPath()));
                    }
                    ioError |= this.moveTempfileToTarget(resultFile, fileInfo.getPath());
                }
            } catch (IOException e) {
                ioError |= IoError.GENERAL;
                if (LOG.isLoggable(Level.SEVERE)) {
                    LOG.severe(String.format("failed to update attrs on %s: " + "%s", resultFile, e.getMessage()));
                }
            }
            this.generator.purgeFile(segment, index);
        } else {
            if (this.isTransferred(index)) {
                try {
                    ioError |= IoError.GENERAL;
                    this.generator.purgeFile(segment, index);
                    this.generator.sendMessage(MessageCode.ERROR_XFER, String.format("%s (index %d) failed " + "verification, update " + "discarded\n", fileInfo.getPath(), index));
                } catch (TextConversionException e) {
                    if (LOG.isLoggable(Level.SEVERE)) {
                        LOG.log(Level.SEVERE, "", e);
                    }
                }
            } else {
                this.generator.generateFile(segment, index, fileInfo);
                this.setIsTransferred(index);
            }
        }
        return ioError;
    }
    
    private Path mergeDataFromPeerAndReplica(LocatableFileInfo fileInfo, Path tempFile, Checksum.Header checksumHeader, MessageDigest md)
            throws ChannelException, InterruptedException, RsyncProtocolException {
        assert fileInfo != null;
        assert tempFile != null;
        assert checksumHeader != null;
        assert md != null;
        
        try (FileChannel target = FileChannel.open(tempFile, StandardOpenOption.WRITE)) {
            Path p = fileInfo.getPath();
            try (FileChannel replica = FileChannel.open(p, StandardOpenOption.READ)) {
                RsyncFileAttributes attrs = this.fileAttributeManager.stat(p);
                if (attrs.isRegularFile()) {
                    boolean isIntact = this.combineDataToFile(replica, target, checksumHeader, md);
                    if (isIntact) {
                        RsyncFileAttributes attrs2 = this.fileAttributeManager.statOrNull(p);
                        if (FileOps.isDataModified(attrs, attrs2)) {
                            String msg = String.format("%s modified during verification " + "(%s != %s)", p, attrs, attrs2);
                            if (LOG.isLoggable(Level.WARNING)) {
                                LOG.warning(msg);
                            }
                            this.generator.sendMessage(MessageCode.WARNING, msg);
                            md.update((byte) 0);
                        }
                        return p;
                    }
                    return tempFile;
                } // else discard later
            } catch (NoSuchFileException e) { // replica.open
                this.combineDataToFile(null, target, checksumHeader, md);
                return tempFile;
            }
        } catch (IOException e) { // target.open
            // discard below
        }
        this.discardData(checksumHeader);
        return null;
    }
    
    private int moveTempfileToTarget(Path tempFile, Path target) throws InterruptedException {
        boolean isOK = FileOps.atomicMove(tempFile, target);
        if (isOK) {
            return 0;
        } else {
            String msg = String.format("Error: when moving temporary file %s " + "to %s", tempFile, target);
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.severe(msg);
            }
            this.generator.sendMessage(MessageCode.ERROR_XFER, msg);
            return IoError.GENERAL;
        }
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
            LOG.log(message.logLevelOrNull(), String.format("<SENDER> %s: %s", msgType, Text.stripLast(text)));
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
            byte dummy = this.in.getByte();
            // we're not expected to get this far, getByte should throw
            // ChannelEOFException
            ByteBuffer buf = ByteBuffer.allocate(1024);
            try {
                buf.put(dummy);
                this.in.get(buf);
            } catch (ChannelEOFException ignored) {
                // ignored
            }
            buf.flip();
            throw new RsyncProtocolException(String.format("Unexpectedly got %d bytes from peer during connection " + "tear down: %s", buf.remaining(), Text.byteBufferToString(buf)));
            
        } catch (ChannelEOFException e) {
            // It's OK, we expect EOF without having received any data
        }
    }
    
    private char readFlags() throws ChannelException, RsyncProtocolException {
        char flags = (char) (this.in.getByte() & 0xFF);
        if ((flags & TransmitFlags.EXTENDED_FLAGS) != 0) {
            flags |= (this.in.getByte() & 0xFF) << 8;
            if (flags == (TransmitFlags.EXTENDED_FLAGS | TransmitFlags.IO_ERROR_ENDLIST)) {
                if (!this.safeFileList) {
                    throw new RsyncProtocolException("invalid flag " + Integer.toBinaryString(flags));
                }
                int ioError = this.receiveAndDecodeInt();
                this.ioError |= ioError;
                if (LOG.isLoggable(Level.WARNING)) {
                    LOG.warning(String.format("peer process returned an I/O " + "error (%d)", ioError));
                }
                this.generator.disableDelete();
                // flags are not used anymore
                return 0;
            }
        }
        // might be 0
        return flags;
    }
    
    private ByteBuffer readFromReplica(FileChannel replica, int blockIndex, Checksum.Header checksumHeader) throws IOException {
        int length = blockSize(blockIndex, checksumHeader);
        long offset = (long) blockIndex * checksumHeader.getBlockLength();
        return this.readFromReplica(replica, offset, length);
    }
    
    private ByteBuffer readFromReplica(FileChannel replica, long offset, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        int bytesRead = replica.read(buf, offset);
        if (buf.hasRemaining()) {
            throw new IllegalStateException(String.format("truncated read from replica (%s), read %d bytes but expected" + " %d more bytes", replica, bytesRead, buf.remaining()));
        }
        buf.flip();
        return buf;
    }
    
    private int receiveAndDecodeInt() throws ChannelException {
        return (int) this.receiveAndDecodeLong(1);
    }
    
    private long receiveAndDecodeLong(int minBytes) throws ChannelException {
        try {
            return IntegerCoder.decodeLong(this.in, minBytes);
        } catch (Exception e) {
            throw new ChannelException(e.getMessage());
        }
    }
    
    private int receiveAndMatch(Filelist.Segment segment, int index, LocatableFileInfo fileInfo) throws ChannelException, InterruptedException, RsyncProtocolException {
        int ioError = 0;
        Checksum.Header checksumHeader = this.receiveChecksumHeader();
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("received peer checksum " + checksumHeader);
        }
        
        try (AutoDeletable tempFile = new AutoDeletable(Files.createTempFile(fileInfo.getPath().getParent(), null, null))) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("created tempfile " + tempFile);
            }
            ioError |= this.matchData(segment, index, fileInfo, checksumHeader, tempFile.getPath());
        } catch (IOException e) {
            String msg = String.format("failed to create tempfile in %s: %s", fileInfo.getPath().getParent(), e.getMessage());
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.severe(msg);
            }
            this.generator.sendMessage(MessageCode.ERROR_XFER, msg + '\n');
            this.discardData(checksumHeader);
            this.in.skip(Checksum.MAX_DIGEST_LENGTH);
            ioError |= IoError.GENERAL;
            this.generator.purgeFile(segment, index);
        }
        return ioError;
    }
    
    private Checksum.Header receiveChecksumHeader() throws ChannelException, RsyncProtocolException {
        return Connection.receiveChecksumHeader(this.in);
    }
    
    private int[] receiveDeviceInfo(char flags, RsyncFileAttributes attrs) throws ChannelException {
        int[] res = { -1, -1 };
        if (this.preserveDevices && (attrs.isBlockDevice() || attrs.isCharacterDevice()) || this.preserveSpecials && (attrs.isFifo() || attrs.isSocket())) {
            if ((flags & TransmitFlags.SAME_RDEV_MAJOR) == 0) {
                res[0] = this.receiveAndDecodeInt();
                this.fileInfoCache.setPrevMajor(res[0]);
            } else {
                res[0] = this.fileInfoCache.getPrevMajor();
            }
            res[1] = this.receiveAndDecodeInt();
        }
        return res;
    }
    
    private FileInfo receiveFileInfo(char flags) throws InterruptedException, ChannelException, RsyncSecurityException, RsyncProtocolException {
        byte[] pathNameBytes = this.receivePathNameBytes(flags);
        RsyncFileAttributes attrs = this.receiveRsyncFileAttributes(flags);
        String pathNameOrNull = this.decodePathName(pathNameBytes);
        Path fullPathOrNull = this.resolvePathOrNull(pathNameOrNull);
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("Receiving file information for %s: %s", pathNameOrNull, attrs));
        }
        
        int[] deviceRes = this.receiveDeviceInfo(flags, attrs);
        int major = deviceRes[0];
        int minor = deviceRes[1];
        
        String symlinkTargetOrNull = null;
        if (this.preserveLinks && attrs.isSymbolicLink()) {
            symlinkTargetOrNull = this.receiveSymlinkTarget();
        }
        
        FileInfo fileInfo = createFileInfo(pathNameOrNull, pathNameBytes, attrs, fullPathOrNull, symlinkTargetOrNull, major, minor);
        
        if (!(fileInfo instanceof LocatableFileInfo)) {
            this.generator.disableDelete();
        }
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Finished receiving " + fileInfo);
        }
        
        return fileInfo;
    }
    
    private FileInfoStub receiveFileInfoStub(char flags) throws InterruptedException, ChannelException, RsyncProtocolException, RsyncSecurityException {
        byte[] pathNameBytes = this.receivePathNameBytes(flags);
        RsyncFileAttributes attrs = this.receiveRsyncFileAttributes(flags);
        String pathNameOrNull = this.decodePathName(pathNameBytes);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("Receiving file information for %s: %s", pathNameOrNull, attrs));
        }
        
        int[] deviceRes = this.receiveDeviceInfo(flags, attrs);
        int major = deviceRes[0];
        int minor = deviceRes[1];
        
        String symlinkTargetOrNull = null;
        if (this.preserveLinks && attrs.isSymbolicLink()) {
            symlinkTargetOrNull = this.receiveSymlinkTarget();
        }
        
        FileInfoStub stub = new FileInfoStub();
        stub.pathNameOrNull = pathNameOrNull;
        stub.pathNameBytes = pathNameBytes;
        stub.attrs = attrs;
        stub.symlinkTargetOrNull = symlinkTargetOrNull;
        stub.major = major;
        stub.minor = minor;
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Finished receiving " + stub);
        }
        
        return stub;
    }
    
    private int receiveFiles() throws ChannelException, InterruptedException, RsyncProtocolException, RsyncSecurityException {
        int ioError = 0;
        Filelist.Segment segment = null;
        int numSegmentsInProgress = 1;
        TransferPhase phase = TransferPhase.TRANSFER;
        boolean isEOF = this.fileSelection != FileSelection.RECURSE;
        
        while (phase != TransferPhase.STOP) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("num bytes available to read: %d", this.in.getNumBytesAvailable()));
            }
            
            final int index = this.in.decodeIndex();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("Received index %d", index));
            }
            
            if (index == Filelist.DONE) {
                if (this.fileSelection != FileSelection.RECURSE && !this.fileList.isEmpty()) {
                    throw new IllegalStateException("received file list DONE when not recursive and file " + "list is not empty: " + this.fileList);
                }
                numSegmentsInProgress--;
                if (numSegmentsInProgress <= 0 && this.fileList.isEmpty()) {
                    if (!isEOF) {
                        throw new IllegalStateException(
                                "got file list DONE with empty file list and at " + "least all ouststanding segment deletions " + "acknowledged but haven't received file list EOF");
                    }
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("phase transition %s -> %s", phase, phase.next()));
                    }
                    phase = phase.next();
                    if (phase == TransferPhase.TEAR_DOWN_1) {
                        this.generator.processDeferredJobs();
                    }
                    this.generator.sendSegmentDone(); // 3 after empty
                } else if (numSegmentsInProgress < 0 && !this.fileList.isEmpty()) {
                    throw new IllegalStateException("Received more acked deleted segments then we have " + "sent to peer: " + this.fileList);
                }
            } else if (index == Filelist.EOF) {
                if (isEOF) {
                    throw new IllegalStateException("received duplicate file " + "list EOF");
                }
                if (this.fileSelection != FileSelection.RECURSE) {
                    throw new IllegalStateException("Received file list EOF" + " from peer while not " + "doing incremental " + "recursing");
                }
                if (this.fileList.isExpandable()) {
                    throw new IllegalStateException("Received file list EOF " + "from peer while having " + "an expandable file " + "list: " + this.fileList);
                }
                isEOF = true;
            } else if (index < 0) {
                if (this.fileSelection != FileSelection.RECURSE) {
                    throw new IllegalStateException("Received negative file " + "index from peer while " + "not doing incremental " + "recursing");
                }
                int directoryIndex = Filelist.OFFSET - index;
                FileInfo directory = this.fileList.getStubDirectoryOrNull(directoryIndex);
                if (directory == null) {
                    throw new IllegalStateException(String.format("there is no stub directory for index %d", directoryIndex));
                }
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("Receiving directory index %d is dir %s", directoryIndex, directory));
                }
                
                segment = this.receiveSegment(directory);
                if (this.listOnly) {
                    this.generator.listSegment(segment);
                } else {
                    this.generator.generateSegment(this.targetPath, segment, this.filterRuleConfiguration);
                }
                numSegmentsInProgress++;
            } else if (index >= 0) {
                if (this.listOnly) {
                    throw new RsyncProtocolException(String.format("Error: received file index %d when listing files only", index));
                }
                
                final char iFlags = this.in.getChar();
                if (!Item.isValidItem(iFlags)) {
                    throw new IllegalStateException(String.format("got flags %s - not supported", Integer.toBinaryString(iFlags)));
                }
                
                if ((iFlags & Item.TRANSFER) == 0) {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine(String.format("index %d is not a transfer", index));
                    }
                    continue;
                }
                
                if (phase != TransferPhase.TRANSFER) {
                    throw new RsyncProtocolException(String.format("Error: wrong phase (%s)", phase));
                }
                
                segment = Util.defaultIfNull(segment, this.fileList.getFirstSegment());
                LocatableFileInfo fileInfo = (LocatableFileInfo) segment.getFileWithIndexOrNull(index);
                if (fileInfo == null) {
                    if (this.fileSelection != FileSelection.RECURSE) {
                        throw new RsyncProtocolException(String.format("Received invalid file index %d from peer", index));
                    }
                    segment = this.fileList.getSegmentWith(index);
                    if (segment == null) {
                        throw new RsyncProtocolException(String.format("Received invalid file %d from peer", index));
                    }
                    fileInfo = (LocatableFileInfo) segment.getFileWithIndexOrNull(index);
                    assert fileInfo != null;
                }
                
                if (LOG.isLoggable(Level.INFO)) {
                    LOG.info(fileInfo.toString());
                }
                
                this.stats.numTransferredFiles++;
                this.stats.totalTransferredSize += fileInfo.getAttributes().getSize();
                
                if (this.isTransferred(index) && LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Re-receiving " + fileInfo);
                }
                ioError |= this.receiveAndMatch(segment, index, fileInfo);
            }
        }
        return ioError;
    }
    
    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     * @throws InterruptedException
     * @throws RsyncSecurityException
     */
    private List<FileInfoStub> receiveFileStubs() throws ChannelException, RsyncProtocolException, InterruptedException, RsyncSecurityException {
        long numBytesRead = this.in.getNumBytesRead() - this.in.getNumBytesPrefetched();
        List<FileInfoStub> stubs = new ArrayList<>();
        
        while (true) {
            char flags = this.readFlags();
            if (flags == 0) {
                break;
            }
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("got flags " + Integer.toBinaryString(flags));
            }
            FileInfoStub stub = this.receiveFileInfoStub(flags);
            stubs.add(stub);
        }
        
        long segmentSize = this.in.getNumBytesRead() - this.in.getNumBytesPrefetched() - numBytesRead;
        this.stats.totalFileListSize += segmentSize;
        return stubs;
    }
    
    /**
     * @throws RsyncProtocolException if failing to decode the filter rules
     */
    private List<String> receiveFilterRules() throws ChannelException, RsyncProtocolException {
        int numBytesToRead;
        List<String> list = new ArrayList<>();
        
        try {
            
            while ((numBytesToRead = this.in.getInt()) > 0) {
                ByteBuffer buf = this.in.get(numBytesToRead);
                list.add(this.characterDecoder.decode(buf));
            }
            
            return list;
            
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }
    
    private Group receiveGroup() throws ChannelException, RsyncProtocolException {
        int gid = this.receiveGroupId();
        String groupName = this.receiveGroupName();
        return new Group(groupName, gid);
    }
    
    private int receiveGroupId() throws ChannelException, RsyncProtocolException {
        int gid = this.receiveAndDecodeInt();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("received group id " + gid);
        }
        if (gid < 0 || gid > Group.ID_MAX) {
            throw new RsyncProtocolException(String.format("received illegal value for group id: %d (valid range [0..%d]", gid, Group.ID_MAX));
        }
        return gid;
    }
    
    /**
     * @throws RsyncProtocolException if group name is the empty string
     */
    private Map<Integer, Group> receiveGroupList() throws ChannelException, RsyncProtocolException {
        Map<Integer, Group> groups = new HashMap<>();
        while (true) {
            int gid = this.receiveGroupId();
            boolean isDone = gid == 0;
            if (isDone) {
                return groups;
            }
            String groupName = this.receiveGroupName();
            Group group = new Group(groupName, gid);
            groups.put(gid, group);
        }
    }
    
    /**
     * @throws RsyncProtocolException if group name cannot be decoded or it is the
     *                                empty string
     */
    private String receiveGroupName() throws ChannelException, RsyncProtocolException {
        try {
            int nameLength = 0xFF & this.in.getByte();
            ByteBuffer buf = this.in.get(nameLength);
            String groupName = this.characterDecoder.decode(buf);
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("received group name " + groupName);
            }
            if (groupName.isEmpty()) {
                throw new RsyncProtocolException("group name is empty");
            }
            return groupName;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }
    
    private Group receiveIncompleteGroup() throws ChannelException, RsyncProtocolException {
        int gid = this.receiveGroupId();
        return new Group("", gid);
    }
    
    private User receiveIncompleteUser() throws ChannelException, RsyncProtocolException {
        int uid = this.receiveUserId();
        return new User("", uid);
    }
    
    private Filelist.Segment receiveInitialSegment() throws InterruptedException, RsyncException {
        // unable to resolve path until we have the initial list of files
        List<FileInfoStub> stubs = this.receiveFileStubs();
        if (!this.listOnly) {
            this.pathResolver = this.getPathResolver(stubs);
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("Path Resolver: " + this.pathResolver);
            }
        }
        
        if (!this.numericIds && this.fileSelection != FileSelection.RECURSE) {
            if (this.preserveUser) {
                Map<Integer, User> uidUserMap = this.receiveUserList();
                uidUserMap.put(User.ROOT.getId(), User.ROOT);
                this.addUserNameToStubs(uidUserMap, stubs);
            }
            if (this.preserveGroup) {
                Map<Integer, Group> gidGroupMap = this.receiveGroupList();
                gidGroupMap.put(Group.ROOT.getId(), Group.ROOT);
                this.addGroupNameToStubs(gidGroupMap, stubs);
            }
        }
        
        Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(null);
        
        for (FileInfoStub stub : stubs) {
            Path pathOrNull = this.resolvePathOrNull(stub.pathNameOrNull);
            FileInfo f = createFileInfo(stub.pathNameOrNull, stub.pathNameBytes, stub.attrs, pathOrNull, stub.symlinkTargetOrNull, stub.major, stub.minor);
            builder.add(f);
        }
        
        Filelist.Segment segment = this.fileList.newSegment(builder);
        return segment;
    }
    
    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     */
    private byte[] receivePathNameBytes(char xflags) throws ChannelException {
        int prefixNumBytes = 0;
        if ((xflags & TransmitFlags.SAME_NAME) != 0) {
            prefixNumBytes = 0xFF & this.in.getByte();
        }
        int suffixNumBytes;
        if ((xflags & TransmitFlags.LONG_NAME) != 0) {
            suffixNumBytes = this.receiveAndDecodeInt();
        } else {
            suffixNumBytes = 0xFF & this.in.getByte();
        }
        
        byte[] prevFileNameBytes = this.fileInfoCache.getPrevFileNameBytes();
        byte[] fileNameBytes = new byte[prefixNumBytes + suffixNumBytes];
        System.arraycopy(prevFileNameBytes /* src */, 0 /* srcPos */, fileNameBytes /* dst */, 0 /* dstPos */, prefixNumBytes /* length */);
        this.in.get(fileNameBytes, prefixNumBytes, suffixNumBytes);
        this.fileInfoCache.setPrevFileNameBytes(fileNameBytes);
        return fileNameBytes;
    }
    
    private RsyncFileAttributes receiveRsyncFileAttributes(char xflags) throws ChannelException, RsyncProtocolException {
        long fileSize = this.receiveAndDecodeLong(3);
        if (fileSize < 0) {
            throw new RsyncProtocolException(String.format("received negative file size %d", fileSize));
        }
        
        long lastModified;
        if ((xflags & TransmitFlags.SAME_TIME) != 0) {
            lastModified = this.fileInfoCache.getPrevLastModified();
        } else {
            lastModified = this.receiveAndDecodeLong(4);
            this.fileInfoCache.setPrevLastModified(lastModified);
        }
        if (lastModified < 0) {
            throw new RsyncProtocolException(String.format("received last modification time %d", lastModified));
        }
        
        int mode;
        if ((xflags & TransmitFlags.SAME_MODE) != 0) {
            mode = this.fileInfoCache.getPrevMode();
        } else {
            mode = this.in.getInt();
            this.fileInfoCache.setPrevMode(mode);
        }
        
        User user;
        boolean reusePrevUserId = (xflags & TransmitFlags.SAME_UID) != 0;
        if (reusePrevUserId) {
            user = this.getPreviousUser();
        } else {
            if (!this.preserveUser) {
                throw new RsyncProtocolException("got new uid when not " + "preserving uid");
            }
            boolean isReceiveUserName = (xflags & TransmitFlags.USER_NAME_FOLLOWS) != 0;
            if (isReceiveUserName && this.fileSelection != FileSelection.RECURSE) {
                throw new RsyncProtocolException("got user name mapping when " + "not doing incremental " + "recursion");
            } else if (isReceiveUserName && this.numericIds) {
                throw new RsyncProtocolException("got user name mapping with " + "--numeric-ids");
            }
            if (this.fileSelection == FileSelection.RECURSE && isReceiveUserName) {
                user = this.receiveUser();
                this.recursiveUidUserMap.put(user.getId(), user);
            } else if (this.fileSelection == FileSelection.RECURSE) {
                // && !isReceiveUsername where isReceiveUserName is only true
                // once for every new mapping, old ones have been sent
                // previously
                int uid = this.receiveUserId();
                // Note: _uidUserMap contains a predefined mapping for root
                user = this.recursiveUidUserMap.get(uid);
                if (user == null) {
                    user = new User("", uid);
                }
            } else { // if (fileSelection != FileSelection.RECURSE) {
                // User with uid but no user name. User name mappings are sent
                // in batch after initial file list
                user = this.receiveIncompleteUser();
            }
            this.fileInfoCache.setPrevUser(user);
        }
        
        Group group;
        boolean reusePrevGroupId = (xflags & TransmitFlags.SAME_GID) != 0;
        if (reusePrevGroupId) {
            group = this.getPreviousGroup();
        } else {
            if (!this.preserveGroup) {
                throw new RsyncProtocolException("got new gid when not " + "preserving gid");
            }
            boolean isReceiveGroupName = (xflags & TransmitFlags.GROUP_NAME_FOLLOWS) != 0;
            if (isReceiveGroupName && this.fileSelection != FileSelection.RECURSE) {
                throw new RsyncProtocolException("got group name mapping " + "when not doing incremental " + "recursion");
            } else if (isReceiveGroupName && this.numericIds) {
                throw new RsyncProtocolException("got group name mapping " + "with --numeric-ids");
            }
            if (this.fileSelection == FileSelection.RECURSE && isReceiveGroupName) {
                group = this.receiveGroup();
                this.recursiveGidGroupMap.put(group.getId(), group);
            } else if (this.fileSelection == FileSelection.RECURSE) {
                int gid = this.receiveGroupId();
                group = this.recursiveGidGroupMap.get(gid);
                if (group == null) {
                    group = new Group("", gid);
                }
            } else { // if (fileSelection != FileSelection.RECURSE) {
                // Group with gid but no group name. Group name mappings are
                // sent in batch after initial file list
                group = this.receiveIncompleteGroup();
            }
            this.fileInfoCache.setPrevGroup(group);
        }
        
        // throws IllegalArgumentException if fileSize or lastModified is
        // negative, but we check for this earlier
        RsyncFileAttributes attrs = new RsyncFileAttributes(mode, fileSize, lastModified, user, group);
        return attrs;
    }
    
    /**
     * @throws RsyncProtocolException if received file is invalid in some way
     * @throws InterruptedException
     * @throws RsyncSecurityException
     */
    private Filelist.Segment receiveSegment(FileInfo dir) throws ChannelException, RsyncProtocolException, InterruptedException, RsyncSecurityException {
        long numBytesRead = this.in.getNumBytesRead() - this.in.getNumBytesPrefetched();
        Filelist.SegmentBuilder builder = new Filelist.SegmentBuilder(dir);
        
        while (true) {
            char flags = this.readFlags();
            if (flags == 0) {
                break;
            }
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("got flags " + Integer.toBinaryString(flags));
            }
            FileInfo fileInfo = this.receiveFileInfo(flags);
            builder.add(fileInfo);
        }
        
        long segmentSize = this.in.getNumBytesRead() - this.in.getNumBytesPrefetched() - numBytesRead;
        this.stats.totalFileListSize += segmentSize;
        Filelist.Segment segment = this.fileList.newSegment(builder);
        return segment;
    }
    
    private void receiveStatistics() throws ChannelException {
        this.stats.totalBytesWritten = this.receiveAndDecodeLong(3);
        this.stats.totalBytesRead = this.receiveAndDecodeLong(3);
        this.stats.totalFileSize = this.receiveAndDecodeLong(3);
        this.stats.fileListBuildTime = this.receiveAndDecodeLong(3);
        this.stats.fileListTransferTime = this.receiveAndDecodeLong(3);
    }
    
    /**
     *
     * @throws TextConversionException
     * @throws ChannelException
     */
    private String receiveSymlinkTarget() throws ChannelException {
        int length = this.receiveAndDecodeInt();
        ByteBuffer buf = this.in.get(length);
        String name = this.characterDecoder.decode(buf);
        return name;
    }
    
    private User receiveUser() throws ChannelException, RsyncProtocolException {
        int uid = this.receiveUserId();
        String userName = this.receiveUserName();
        return new User(userName, uid);
    }
    
    private int receiveUserId() throws ChannelException, RsyncProtocolException {
        int uid = this.receiveAndDecodeInt();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("received user id " + uid);
        }
        if (uid < 0 || uid > User.ID_MAX) {
            throw new RsyncProtocolException(String.format("received illegal value for user id: %d (valid range [0..%d]", uid, User.ID_MAX));
        }
        return uid;
    }
    
    /**
     * @throws RsyncProtocolException if user name is the empty string
     */
    private Map<Integer, User> receiveUserList() throws ChannelException, RsyncProtocolException {
        Map<Integer, User> users = new HashMap<>();
        while (true) {
            int uid = this.receiveUserId();
            boolean isDone = uid == 0;
            if (isDone) {
                return users;
            }
            String userName = this.receiveUserName();
            User user = new User(userName, uid);
            users.put(uid, user);
        }
    }
    
    /**
     * @throws RsyncProtocolException if user name cannot be decoded or it is the
     *                                empty string
     */
    private String receiveUserName() throws ChannelException, RsyncProtocolException {
        try {
            int nameLength = 0xFF & this.in.getByte();
            ByteBuffer buf = this.in.get(nameLength);
            String userName = this.characterDecoder.decode(buf);
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("received user name " + userName);
            }
            if (userName.isEmpty()) {
                throw new RsyncProtocolException("user name is empty");
            }
            return userName;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }
    
    private Path resolvePathOrNull(String pathNameOrNull) throws RsyncSecurityException, InterruptedException {
        if (this.listOnly || pathNameOrNull == null) {
            return null;
        }
        try {
            Path relativePath = this.pathResolver.relativePathOf(pathNameOrNull);
            Path fullPath = this.pathResolver.fullPathOf(relativePath);
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer(String.format("relative path: %s, full path: %s", relativePath, fullPath));
            }
            if (PathOps.isPathPreservable(fullPath)) {
                return fullPath;
            }
            this.generator.sendMessage(MessageCode.ERROR, String.format("Unable to preserve file " + "name for: \"%s\"\n", pathNameOrNull));
        } catch (InvalidPathException e) {
            this.generator.sendMessage(MessageCode.ERROR, e.getMessage());
        }
        this.ioError |= IoError.GENERAL;
        return null;
    }
    
    private void sendFilterRules() throws InterruptedException {
        
        if (this.filterRuleConfiguration != null) {
            for (FilterRuleList.FilterRule rule : this.filterRuleConfiguration.getFilterRuleListForSending()._rules) {
                byte[] encodedRule = this.characterEncoder.encode(rule.toString());
                
                ByteBuffer buf = ByteBuffer.allocate(4 + encodedRule.length).order(ByteOrder.LITTLE_ENDIAN);
                buf.putInt(encodedRule.length);
                buf.put(encodedRule);
                buf.flip();
                this.generator.sendBytes(buf);
            }
        }
        
        // send stop signal
        ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(0);
        buf.flip();
        this.generator.sendBytes(buf);
    }
    
    private void setIsTransferred(int index) {
        this.transferred.set(index);
    }
    
    public Statistics statistics() {
        return this.stats;
    }
    
    @Override
    public String toString() {
        return String.format(
                "%s(" + "isDeferWrite=%b, " + "isExitAfterEOF=%b, " + "exitEarlyIfEmptyList=%b, " + "isInterruptible=%b, " + "isListOnly=%b, " + "isNumericIds=%b, " + "isPreserveDevices=%b, "
                        + "isPreserveLinks=%b, " + "isPreservePermissions=%b, " + "isPreserveSpecials=%b, " + "isPreserveTimes=%b, " + "isPreserveUser=%b, " + "isPreserveGroup=%b, "
                        + "isReceiveStatistics=%b, " + "isSafeFileList=%b, " + "fileSelection=%s, " + "filterMode=%s, " + "targetPath=%s, " + "fileAttributeManager=%s" + ")",
                this.getClass().getSimpleName(), this.deferWrite, this.exitAfterEOF, this.exitEarlyIfEmptyList, this.listOnly, this.interruptible, this.numericIds,
                this.preserveDevices, this.preserveLinks, this.preservePermissions, this.preserveSpecials, this.preserveTimes, this.preserveUser, this.preserveGroup,
                this.receiveStatistics, this.safeFileList, this.fileSelection, this.filterMode, this.targetPath, this.fileAttributeManager);
    }
    
    private void updateAttrsIfDiffer(Path path, RsyncFileAttributes targetAttrs) throws IOException {
        RsyncFileAttributes curAttrs = this.fileAttributeManager.stat(path);
        
        if (this.preservePermissions && curAttrs.getMode() != targetAttrs.getMode()) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("updating file permissions %o -> %o on %s", curAttrs.getMode(), targetAttrs.getMode(), path));
            }
            this.fileAttributeManager.setFileMode(path, targetAttrs.getMode(), LinkOption.NOFOLLOW_LINKS);
        }
        if (this.preserveTimes && curAttrs.lastModifiedTime() != targetAttrs.lastModifiedTime()) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("updating mtime %d -> %d on %s", curAttrs.lastModifiedTime(), targetAttrs.lastModifiedTime(), path));
            }
            this.fileAttributeManager.setLastModifiedTime(path, targetAttrs.lastModifiedTime(), LinkOption.NOFOLLOW_LINKS);
        }
        if (this.preserveUser) {
            if (!this.numericIds && !targetAttrs.getUser().getName().isEmpty() && !curAttrs.getUser().getName().equals(targetAttrs.getUser().getName())) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("updating ownership %s -> %s on %s", curAttrs.getUser(), targetAttrs.getUser(), path));
                }
                // FIXME: side effect of chown in Linux is that set user/group
                // id bit are cleared.
                this.fileAttributeManager.setOwner(path, targetAttrs.getUser(), LinkOption.NOFOLLOW_LINKS);
            } else if ((this.numericIds || targetAttrs.getUser().getName().isEmpty()) && curAttrs.getUser().getId() != targetAttrs.getUser().getId()) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("updating uid %d -> %d on %s", curAttrs.getUser().getId(), targetAttrs.getUser().getId(), path));
                }
                // NOTE: side effect of chown in Linux is that set user/group id
                // bit might be cleared.
                this.fileAttributeManager.setUserId(path, targetAttrs.getUser().getId(), LinkOption.NOFOLLOW_LINKS);
            }
        }
        if (this.preserveGroup) {
            if (!this.numericIds && !targetAttrs.getGroup().getName().isEmpty() && !curAttrs.getGroup().getName().equals(targetAttrs.getGroup().getName())) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("updating group %s -> %s on %s", curAttrs.getGroup(), targetAttrs.getGroup(), path));
                }
                this.fileAttributeManager.setGroup(path, targetAttrs.getGroup(), LinkOption.NOFOLLOW_LINKS);
            } else if ((this.numericIds || targetAttrs.getGroup().getName().isEmpty()) && curAttrs.getGroup().getId() != targetAttrs.getGroup().getId()) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("updating gid %d -> %d on %s", curAttrs.getGroup().getId(), targetAttrs.getGroup().getId(), path));
                }
                this.fileAttributeManager.setGroupId(path, targetAttrs.getGroup().getId(), LinkOption.NOFOLLOW_LINKS);
            }
        }
    }
    
    private void writeToFile(FileChannel out, ByteBuffer src) {
        try {
            // NOTE: might notably fail due to running out of disk space
            out.write(src);
            if (src.hasRemaining()) {
                throw new IllegalStateException(String.format("truncated write to %s, returned %d bytes, " + "expected %d more bytes", out, src.position(), src.remaining()));
            }
        } catch (IOException e) {
            // native exists immediately if this happens, and so do we:
            throw new RuntimeException(e);
        }
    }
}
