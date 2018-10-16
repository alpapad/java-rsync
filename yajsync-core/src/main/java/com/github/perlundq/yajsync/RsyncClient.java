/*
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
package com.github.perlundq.yajsync;

import java.io.Console;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.attr.FileInfo;
import com.github.perlundq.yajsync.internal.session.ClientSessionConfig;
import com.github.perlundq.yajsync.internal.session.FilterMode;
import com.github.perlundq.yajsync.internal.session.FilterRuleConfiguration;
import com.github.perlundq.yajsync.internal.session.Generator;
import com.github.perlundq.yajsync.internal.session.Receiver;
import com.github.perlundq.yajsync.internal.session.RsyncTaskExecutor;
import com.github.perlundq.yajsync.internal.session.Sender;
import com.github.perlundq.yajsync.internal.session.SessionStatistics;
import com.github.perlundq.yajsync.internal.session.SessionStatus;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.util.BitOps;
import com.github.perlundq.yajsync.internal.util.Pair;
import com.github.perlundq.yajsync.internal.util.Util;

public final class RsyncClient {
    public static class Builder {
        private AuthProvider _authProvider = new ConsoleAuthProvider();
        private Charset _charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService _executorService;
        private FileSelection _fileSelection;
        private boolean _isAlwaysItemize;
        private boolean _isDeferWrite;
        private boolean _isDelete;
        private boolean _isIgnoreTimes;
        private boolean _isNumericIds;
        private boolean _isPreserveDevices;
        private boolean _isPreserveGroup;
        private boolean _isPreserveLinks;
        private boolean _isPreservePermissions;
        private boolean _isPreserveSpecials;
        private boolean _isPreserveTimes;
        private boolean _isPreserveUser;
        private PrintStream _stderr = System.err;
        private int _verbosity;
        private FilterRuleConfiguration _filterRuleConfiguration;
        
        public Builder authProvider(AuthProvider authProvider) {
            this._authProvider = authProvider;
            return this;
        }
        
        public Local buildLocal() {
            return new RsyncClient(this).new Local();
        }
        
        public Remote buildRemote(ReadableByteChannel in, WritableByteChannel out, boolean isInterruptible) {
            return new RsyncClient(this).new Remote(in, out, isInterruptible);
        }
        
        /**
         *
         * @throws UnsupportedCharsetException if charset is not supported
         */
        public Builder charset(Charset charset) {
            Util.validateCharset(charset);
            this._charset = charset;
            return this;
        }
        
        public Builder executorService(ExecutorService executorService) {
            this._executorService = executorService;
            return this;
        }
        
        public Builder fileSelection(FileSelection fileSelection) {
            this._fileSelection = fileSelection;
            return this;
        }
        
        public Builder isAlwaysItemize(boolean isAlwaysItemize) {
            this._isAlwaysItemize = isAlwaysItemize;
            return this;
        }
        
        public Builder isDeferWrite(boolean isDeferWrite) {
            this._isDeferWrite = isDeferWrite;
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
        
        public Builder stderr(PrintStream stderr) {
            this._stderr = stderr;
            return this;
        }
        
        public Builder verbosity(int verbosity) {
            this._verbosity = verbosity;
            return this;
        }
        
        public Builder filterRuleConfiguration(FilterRuleConfiguration filterRuleConfiguration) {
            this._filterRuleConfiguration = filterRuleConfiguration;
            return this;
        }
    }
    
    private static class ConsoleAuthProvider implements AuthProvider {
        private final Console console = System.console();
        
        @Override
        public char[] getPassword() throws IOException {
            if (this.console == null) {
                throw new IOException("no console available");
            }
            return this.console.readPassword("Password: ");
        }
        
        @Override
        public String getUser() throws IOException {
            if (this.console == null) {
                throw new IOException("no console available");
            }
            return this.console.readLine("User name: ");
        }
    }
    
    public class FileListing {
        private final Future<Result> _future;
        private final CountDownLatch _isListingAvailable;
        private BlockingQueue<Pair<Boolean, FileInfo>> _listing;
        
        private FileListing(final ClientSessionConfig cfg, final String moduleName, final List<String> serverArgs, final AuthProvider authProvider, final ReadableByteChannel in,
                final WritableByteChannel out, final boolean isInterruptible, final FileSelection fileSelection)
        
        {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws RsyncException, InterruptedException {
                    try {
                        SessionStatus status = cfg.handshake(moduleName, serverArgs, authProvider);
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("handshake status: " + status);
                        }
                        if (status == SessionStatus.ERROR) {
                            return Result.failure();
                        } else if (status == SessionStatus.EXIT) {
                            return Result.success();
                        }
                        Generator generator = new Generator.Builder(out, cfg.checksumSeed()).charset(cfg.charset()).fileSelection(fileSelection).isDelete(RsyncClient.this._isDelete)
                                .isPreserveDevices(RsyncClient.this._isPreserveDevices).isPreserveSpecials(RsyncClient.this._isPreserveSpecials).isPreserveLinks(RsyncClient.this._isPreserveLinks)
                                .isPreservePermissions(RsyncClient.this._isPreservePermissions).isPreserveTimes(RsyncClient.this._isPreserveTimes).isPreserveUser(RsyncClient.this._isPreserveUser)
                                .isPreserveGroup(RsyncClient.this._isPreserveGroup).isNumericIds(RsyncClient.this._isNumericIds).isIgnoreTimes(RsyncClient.this._isIgnoreTimes)
                                .isAlwaysItemize(RsyncClient.this._verbosity > 1).isInterruptible(isInterruptible).build();
                        FileListing.this._listing = generator.files();
                        FileListing.this._isListingAvailable.countDown();
                        Receiver receiver = Receiver.Builder.newListing(generator, in).filterMode(FilterMode.SEND).isDeferWrite(RsyncClient.this._isDeferWrite).isExitAfterEOF(true)
                                .isExitEarlyIfEmptyList(true).isReceiveStatistics(true).isSafeFileList(cfg.isSafeFileList()).build();
                        boolean isOK = RsyncClient.this._rsyncTaskExecutor.exec(generator, receiver);
                        return new Result(isOK, receiver.statistics());
                    } finally {
                        if (FileListing.this._listing == null) {
                            FileListing.this._listing = new LinkedBlockingQueue<>();
                            FileListing.this._listing.put(new Pair<Boolean, FileInfo>(false, null));
                            FileListing.this._isListingAvailable.countDown();
                        }
                        if (RsyncClient.this._isOwnerOfExecutorService) {
                            if (_log.isLoggable(Level.FINE)) {
                                _log.fine("shutting down " + RsyncClient.this._executorService);
                            }
                            RsyncClient.this._executorService.shutdown();
                        }
                    }
                }
            };
            this._future = RsyncClient.this._executorService.submit(callable);
            this._isListingAvailable = new CountDownLatch(1);
        }
        
        private FileListing(final Sender sender, final Generator generator, final Receiver receiver) {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws RsyncException, InterruptedException {
                    try {
                        boolean isOK = RsyncClient.this._rsyncTaskExecutor.exec(sender, generator, receiver);
                        return new Result(isOK, receiver.statistics());
                    } finally {
                        if (RsyncClient.this._isOwnerOfExecutorService) {
                            if (_log.isLoggable(Level.FINE)) {
                                _log.fine("shutting down " + RsyncClient.this._executorService);
                            }
                            RsyncClient.this._executorService.shutdown();
                        }
                    }
                }
            };
            this._future = RsyncClient.this._executorService.submit(callable);
            this._listing = generator.files();
            this._isListingAvailable = new CountDownLatch(0);
        }
        
        public Future<Result> futureResult() {
            return this._future;
        }
        
        public Result get() throws InterruptedException, RsyncException {
            try {
                return this._future.get();
            } catch (Throwable e) {
                RsyncTaskExecutor.throwUnwrappedException(e);
                throw new AssertionError();
            }
        }
        
        /**
         * @return the next available file information or null if there is nothing left
         *         available.
         */
        public FileInfo take() throws InterruptedException {
            this._isListingAvailable.await();
            Pair<Boolean, FileInfo> res = this._listing.take();
            boolean isDone = res.first() == null;
            if (isDone) {
                return null;
            } else {
                return res.second();
            }
        }
    }
    
    public class Local {
        public class Copy {
            private final Iterable<Path> _srcPaths;
            
            private Copy(Iterable<Path> srcPaths) {
                assert srcPaths != null;
                this._srcPaths = srcPaths;
            }
            
            public Result to(Path dstPath) throws RsyncException, InterruptedException {
                assert dstPath != null;
                return Local.this.localTransfer(this._srcPaths, dstPath);
            }
        }
        
        public Copy copy(Iterable<Path> paths) {
            assert paths != null;
            return new Copy(paths);
        }
        
        public Copy copy(Path[] paths) {
            return this.copy(Arrays.asList(paths));
        }
        
        public FileListing list(Iterable<Path> srcPaths) {
            assert srcPaths != null;
            Pipe[] pipePair = pipePair();
            Pipe toSender = pipePair[0];
            Pipe toReceiver = pipePair[1];
            FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this._fileSelectionOrNull, FileSelection.TRANSFER_DIRS);
            byte[] seed = BitOps.toLittleEndianBuf((int) System.currentTimeMillis());
            Sender sender = new Sender.Builder(toSender.source(), toReceiver.sink(), srcPaths, seed).isExitEarlyIfEmptyList(true).charset(RsyncClient.this._charset)
                    .isPreserveDevices(RsyncClient.this._isPreserveDevices).isPreserveSpecials(RsyncClient.this._isPreserveSpecials).isPreserveLinks(RsyncClient.this._isPreserveLinks)
                    .isPreserveUser(RsyncClient.this._isPreserveUser).isPreserveGroup(RsyncClient.this._isPreserveGroup).isNumericIds(RsyncClient.this._isNumericIds).fileSelection(fileSelection)
                    .build();
            Generator generator = new Generator.Builder(toSender.sink(), seed).charset(RsyncClient.this._charset).fileSelection(fileSelection).isDelete(RsyncClient.this._isDelete)
                    .isPreserveDevices(RsyncClient.this._isPreserveDevices).isPreserveSpecials(RsyncClient.this._isPreserveSpecials).isPreserveLinks(RsyncClient.this._isPreserveLinks)
                    .isPreservePermissions(RsyncClient.this._isPreservePermissions).isPreserveTimes(RsyncClient.this._isPreserveTimes).isPreserveUser(RsyncClient.this._isPreserveUser)
                    .isPreserveGroup(RsyncClient.this._isPreserveGroup).isNumericIds(RsyncClient.this._isNumericIds).isIgnoreTimes(RsyncClient.this._isIgnoreTimes)
                    .isAlwaysItemize(RsyncClient.this._isAlwaysItemize).build();
            Receiver receiver = Receiver.Builder.newListing(generator, toReceiver.source()).isExitEarlyIfEmptyList(true).isDeferWrite(RsyncClient.this._isDeferWrite).build();
            
            return new FileListing(sender, generator, receiver);
        }
        
        public FileListing list(Path[] paths) {
            return this.list(Arrays.asList(paths));
        }
        
        private Result localTransfer(Iterable<Path> srcPaths, Path dstPath) throws RsyncException, InterruptedException {
            assert srcPaths != null;
            assert dstPath != null;
            Pipe[] pipePair = pipePair();
            Pipe toSender = pipePair[0];
            Pipe toReceiver = pipePair[1];
            FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this._fileSelectionOrNull, FileSelection.EXACT);
            byte[] seed = BitOps.toLittleEndianBuf((int) System.currentTimeMillis());
            Sender sender = new Sender//
                    .Builder(toSender.source(), toReceiver.sink(), srcPaths, seed)//
                            .isExitEarlyIfEmptyList(true).charset(RsyncClient.this._charset)//
                            .isPreserveDevices(RsyncClient.this._isPreserveDevices)//
                            .isPreserveSpecials(RsyncClient.this._isPreserveSpecials)//
                            .isPreserveLinks(RsyncClient.this._isPreserveLinks)//
                            .isPreserveUser(RsyncClient.this._isPreserveUser)//
                            .isPreserveGroup(RsyncClient.this._isPreserveGroup)//
                            .isNumericIds(RsyncClient.this._isNumericIds)//
                            .fileSelection(fileSelection).build();
            Generator generator = new Generator//
                    .Builder(toSender.sink(), seed)//
                            .charset(RsyncClient.this._charset)//
                            .fileSelection(fileSelection)//
                            .isDelete(RsyncClient.this._isDelete)//
                            .isPreserveDevices(RsyncClient.this._isPreserveDevices)//
                            .isPreserveSpecials(RsyncClient.this._isPreserveSpecials)//
                            .isPreserveLinks(RsyncClient.this._isPreserveLinks)//
                            .isPreservePermissions(RsyncClient.this._isPreservePermissions)//
                            .isPreserveTimes(RsyncClient.this._isPreserveTimes)//
                            .isPreserveUser(RsyncClient.this._isPreserveUser)//
                            .isPreserveGroup(RsyncClient.this._isPreserveGroup)//
                            .isNumericIds(RsyncClient.this._isNumericIds)//
                            .isIgnoreTimes(RsyncClient.this._isIgnoreTimes)//
                            .isAlwaysItemize(RsyncClient.this._isAlwaysItemize).build();
            Receiver receiver = new Receiver//
                    .Builder(generator, toReceiver.source(), dstPath)//
                            .isExitEarlyIfEmptyList(true)//
                            .isDeferWrite(RsyncClient.this._isDeferWrite)//
                            .build();
            try {
                boolean isOK = RsyncClient.this._rsyncTaskExecutor.exec(sender, generator, receiver);
                return new Result(isOK, receiver.statistics());
            } finally {
                if (RsyncClient.this._isOwnerOfExecutorService) {
                    RsyncClient.this._executorService.shutdown();
                }
            }
        }
    }
    
    private enum Mode {
        LOCAL_COPY, LOCAL_LIST, REMOTE_LIST, REMOTE_RECEIVE, REMOTE_SEND
    }
    
    public class ModuleListing {
        private final Future<Result> _future;
        private final BlockingQueue<Pair<Boolean, String>> _moduleNames;
        
        private ModuleListing(final ClientSessionConfig cfg, final List<String> serverArgs) {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    try {
                        SessionStatus status = cfg.handshake("", serverArgs, RsyncClient.this._authProvider);
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("handshake status: " + status);
                        }
                        if (status == SessionStatus.ERROR) {
                            return Result.failure();
                        } else if (status == SessionStatus.EXIT) {
                            return Result.success();
                        }
                        throw new AssertionError();
                    } finally {
                        if (RsyncClient.this._isOwnerOfExecutorService) {
                            if (_log.isLoggable(Level.FINE)) {
                                _log.fine("shutting down " + RsyncClient.this._executorService);
                            }
                            RsyncClient.this._executorService.shutdown();
                        }
                    }
                }
            };
            this._future = RsyncClient.this._executorService.submit(callable);
            this._moduleNames = cfg.modules();
        }
        
        public Future<Result> futureResult() {
            return this._future;
        }
        
        public Result get() throws InterruptedException, RsyncException {
            try {
                return this._future.get();
            } catch (Throwable e) {
                RsyncTaskExecutor.throwUnwrappedException(e);
                throw new AssertionError();
            }
        }
        
        /**
         * @return the next line of the remote module or motd listing or null if there
         *         are no more lines available
         */
        public String take() throws InterruptedException {
            Pair<Boolean, String> res = this._moduleNames.take();
            boolean isDone = res.first() == null;
            if (isDone) {
                return null;
            } else {
                return res.second();
            }
        }
    }
    
    public class Remote {
        public class Receive {
            private final String _moduleName;
            private final Iterable<String> _srcPathNames;
            
            private Receive(String moduleName, Iterable<String> srcPathNames) {
                assert moduleName != null;
                assert srcPathNames != null;
                this._moduleName = moduleName;
                this._srcPathNames = srcPathNames;
            }
            
            public Result to(Path dstPath) throws RsyncException, InterruptedException {
                assert dstPath != null;
                FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this._fileSelectionOrNull, FileSelection.EXACT);
                
                List<String> serverArgs = Remote.this.createServerArgs(Mode.REMOTE_RECEIVE, fileSelection);
                for (String s : this._srcPathNames) {
                    assert s.startsWith(Text.SLASH) : s;
                    serverArgs.add(this._moduleName + s);
                }
                
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("file selection: %s, src: %s, dst: " + "%s, remote args: %s", fileSelection, this._srcPathNames, dstPath, serverArgs));
                }
                
                ClientSessionConfig cfg = new ClientSessionConfig(Remote.this._in, Remote.this._out, RsyncClient.this._charset, fileSelection == FileSelection.RECURSE, RsyncClient.this._stderr);
                SessionStatus status = cfg.handshake(this._moduleName, serverArgs, RsyncClient.this._authProvider);
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("handshake status: " + status);
                }
                
                if (status == SessionStatus.ERROR) {
                    return Result.failure();
                } else if (status == SessionStatus.EXIT) {
                    return Result.success();
                }
                
                try {
                    Generator generator = new Generator//
                            .Builder(Remote.this._out, cfg.checksumSeed())//
                                    .charset(cfg.charset())//
                                    .fileSelection(fileSelection)//
                                    .isDelete(RsyncClient.this._isDelete)//
                                    .isPreserveLinks(RsyncClient.this._isPreserveLinks)//
                                    .isPreservePermissions(RsyncClient.this._isPreservePermissions)//
                                    .isPreserveTimes(RsyncClient.this._isPreserveTimes)//
                                    .isPreserveUser(RsyncClient.this._isPreserveUser)//
                                    .isPreserveGroup(RsyncClient.this._isPreserveGroup)//
                                    .isNumericIds(RsyncClient.this._isNumericIds)//
                                    .isIgnoreTimes(RsyncClient.this._isIgnoreTimes)//
                                    .isAlwaysItemize(RsyncClient.this._verbosity > 1)//
                                    .isInterruptible(Remote.this._isInterruptible)//
                                    .build();
                    Receiver receiver = new Receiver//
                            .Builder(generator, Remote.this._in, dstPath)//
                                    .filterMode(FilterMode.SEND)//
                                    .isDeferWrite(RsyncClient.this._isDeferWrite)//
                                    .isExitAfterEOF(true)//
                                    .isExitEarlyIfEmptyList(true)//
                                    .isReceiveStatistics(true)//
                                    .isSafeFileList(cfg.isSafeFileList())//
                                    .build();
                    boolean isOK = RsyncClient.this._rsyncTaskExecutor.exec(generator, receiver);
                    return new Result(isOK, receiver.statistics());
                } finally {
                    if (RsyncClient.this._isOwnerOfExecutorService) {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("shutting down " + RsyncClient.this._executorService);
                        }
                        RsyncClient.this._executorService.shutdown();
                    }
                }
            }
        }
        
        public class Send {
            private final Iterable<Path> _srcPaths;
            
            private Send(Iterable<Path> srcPaths) {
                assert srcPaths != null;
                this._srcPaths = srcPaths;
            }
            
            public Result to(String moduleName, String dstPathName) throws RsyncException, InterruptedException {
                assert moduleName != null;
                assert dstPathName != null;
                assert dstPathName.startsWith(Text.SLASH);
                
                FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this._fileSelectionOrNull, FileSelection.EXACT);
                List<String> serverArgs = Remote.this.createServerArgs(Mode.REMOTE_SEND, fileSelection);
                serverArgs.add(moduleName + dstPathName);
                
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine(String.format("file selection: %s, src: %s, dst: %s, remote " + "args: %s", fileSelection, this._srcPaths, dstPathName, serverArgs));
                }
                
                ClientSessionConfig cfg = new ClientSessionConfig(Remote.this._in, Remote.this._out, RsyncClient.this._charset, fileSelection == FileSelection.RECURSE, RsyncClient.this._stderr);
                SessionStatus status = cfg.handshake(moduleName, serverArgs, RsyncClient.this._authProvider);
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("handshake status: " + status);
                }
                
                if (status == SessionStatus.ERROR) {
                    return Result.failure();
                } else if (status == SessionStatus.EXIT) {
                    return Result.success();
                }
                
                try {
                    Sender sender = Sender.Builder.newClient(Remote.this._in, Remote.this._out, this._srcPaths, cfg.checksumSeed())
                            .filterMode(RsyncClient.this._isDelete ? FilterMode.SEND : FilterMode.NONE).charset(RsyncClient.this._charset).fileSelection(fileSelection)
                            .filterRuleConfiguration(RsyncClient.this._filterRuleConfiguration)//
                            .isPreserveLinks(RsyncClient.this._isPreserveLinks).isPreserveUser(RsyncClient.this._isPreserveUser).isPreserveGroup(RsyncClient.this._isPreserveGroup)
                            .isNumericIds(RsyncClient.this._isNumericIds).isInterruptible(Remote.this._isInterruptible).isSafeFileList(cfg.isSafeFileList()).build();
                    boolean isOK = RsyncClient.this._rsyncTaskExecutor.exec(sender);
                    return new Result(isOK, sender.statistics());
                } finally {
                    if (RsyncClient.this._isOwnerOfExecutorService) {
                        if (_log.isLoggable(Level.FINE)) {
                            _log.fine("shutting down " + RsyncClient.this._executorService);
                        }
                        RsyncClient.this._executorService.shutdown();
                    }
                }
            }
        }
        
        private final ReadableByteChannel _in;
        
        private final boolean _isInterruptible;
        
        private final WritableByteChannel _out;
        
        public Remote(ReadableByteChannel in, WritableByteChannel out, boolean isInterruptible) {
            assert in != null;
            assert out != null;
            this._in = in;
            this._out = out;
            this._isInterruptible = isInterruptible;
        }
        
        private List<String> createServerArgs(Mode mode, FileSelection fileSelection) {
            assert mode != null;
            assert fileSelection != null;
            List<String> serverArgs = new LinkedList<>();
            serverArgs.add("--server");
            boolean isPeerSender = mode != Mode.REMOTE_SEND;
            if (isPeerSender) {
                serverArgs.add("--sender");
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append("-");
            for (int i = 0; i < RsyncClient.this._verbosity; i++) {
                sb.append("v");
            }
            if (RsyncClient.this._isPreserveLinks) {
                sb.append("l");
            }
            if (fileSelection == FileSelection.TRANSFER_DIRS) {
                sb.append("d");
            }
            if (RsyncClient.this._isPreservePermissions) {
                sb.append("p");
            }
            if (RsyncClient.this._isPreserveTimes) {
                sb.append("t");
            }
            if (RsyncClient.this._isPreserveUser) {
                sb.append("o");
            }
            if (RsyncClient.this._isPreserveGroup) {
                sb.append("g");
            }
            if (RsyncClient.this._isPreserveDevices) {
                sb.append("D");
            }
            if (RsyncClient.this._isIgnoreTimes) {
                sb.append("I");
            }
            if (fileSelection == FileSelection.RECURSE) {
                sb.append("r");
            }
            sb.append("e");
            sb.append(".");
            if (fileSelection == FileSelection.RECURSE) {
                sb.append("i");
            }
            sb.append("s");
            sb.append("f");
            serverArgs.add(sb.toString());
            
            if (RsyncClient.this._isDelete && mode == Mode.REMOTE_SEND) {
                serverArgs.add("--delete");
            }
            if (RsyncClient.this._isNumericIds) {
                serverArgs.add("--numeric-ids");
            }
            if (RsyncClient.this._isDelete && RsyncClient.this._fileSelectionOrNull == FileSelection.TRANSFER_DIRS) {
                // seems like it's only safe to use --delete and --dirs with
                // rsync versions that happens to support --no-r
                serverArgs.add("--no-r");
            }
            if (RsyncClient.this._isPreserveDevices && !RsyncClient.this._isPreserveSpecials) {
                serverArgs.add("--no-specials");
            }
            
            serverArgs.add("."); // arg delimiter
            
            return serverArgs;
        }
        
        public FileListing list(String moduleName, Iterable<String> srcPathNames) {
            assert moduleName != null;
            assert srcPathNames != null;
            FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this._fileSelectionOrNull, FileSelection.TRANSFER_DIRS);
            List<String> serverArgs = this.createServerArgs(Mode.REMOTE_LIST, fileSelection);
            for (String s : srcPathNames) {
                assert s.startsWith(Text.SLASH) : s;
                serverArgs.add(moduleName + s);
            }
            
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("file selection: %s, src: %s, remote args: %s", fileSelection, srcPathNames, serverArgs));
            }
            ClientSessionConfig cfg = new ClientSessionConfig(this._in, this._out, RsyncClient.this._charset, fileSelection == FileSelection.RECURSE, RsyncClient.this._stderr);
            return new FileListing(cfg, moduleName, serverArgs, RsyncClient.this._authProvider, this._in, this._out, this._isInterruptible, fileSelection);
        }
        
        public FileListing list(String moduleName, String[] paths) {
            return this.list(moduleName, Arrays.asList(paths));
        }
        
        public ModuleListing listModules() {
            FileSelection fileSelection = FileSelection.EXACT;
            Iterable<String> srcPathNames = Collections.emptyList();
            
            List<String> serverArgs = this.createServerArgs(Mode.REMOTE_LIST, fileSelection);
            for (String src : srcPathNames) {
                serverArgs.add(src);
            }
            
            if (_log.isLoggable(Level.FINE)) {
                _log.fine(String.format("file selection: %s, src: %s, remote " + "args: %s", fileSelection, srcPathNames, serverArgs));
            }
            ClientSessionConfig cfg = new ClientSessionConfig(this._in, this._out, RsyncClient.this._charset, fileSelection == FileSelection.RECURSE, RsyncClient.this._stderr);
            return new ModuleListing(cfg, serverArgs);
        }
        
        public Receive receive(String moduleName, Iterable<String> pathNames) {
            assert moduleName != null;
            assert pathNames != null;
            return new Receive(moduleName, pathNames);
        }
        
        public Receive receive(String moduleName, String[] pathNames) {
            return this.receive(moduleName, Arrays.asList(pathNames));
        }
        
        public Send send(Iterable<Path> paths) {
            assert paths != null;
            return new Send(paths);
        }
        
        public Send send(Path[] paths) {
            return this.send(Arrays.asList(paths));
        }
        
        List<String> toListOfStrings(Iterable<Path> paths) {
            List<String> srcPathNames = new LinkedList<>();
            for (Path p : paths) {
                srcPathNames.add(p.toString());
            }
            return srcPathNames;
        }
    }
    
    public static class Result {
        public static Result failure() {
            return new Result(false, new SessionStatistics());
        }
        
        public static Result success() {
            return new Result(true, new SessionStatistics());
        }
        
        private final boolean _isOK;
        
        private final Statistics _statistics;
        
        private Result(boolean isOK, Statistics statistics) {
            this._isOK = isOK;
            this._statistics = statistics;
        }
        
        public boolean isOK() {
            return this._isOK;
        }
        
        public Statistics statistics() {
            return this._statistics;
        }
    }
    
    private static final Logger _log = Logger.getLogger(RsyncClient.class.getName());
    
    private static Pipe[] pipePair() {
        try {
            return new Pipe[] { Pipe.open(), Pipe.open() };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private final AuthProvider _authProvider;
    private final Charset _charset;
    private final ExecutorService _executorService;
    private final FileSelection _fileSelectionOrNull;
    private final boolean _isAlwaysItemize;
    private final boolean _isDeferWrite;
    private final boolean _isDelete;
    private final boolean _isIgnoreTimes;
    private final boolean _isNumericIds;
    private final boolean _isOwnerOfExecutorService;
    private final boolean _isPreserveDevices;
    private final boolean _isPreserveGroup;
    private final boolean _isPreserveLinks;
    private final boolean _isPreservePermissions;
    private final boolean _isPreserveSpecials;
    private final boolean _isPreserveTimes;
    private final boolean _isPreserveUser;
    private final RsyncTaskExecutor _rsyncTaskExecutor;
    private final PrintStream _stderr;
    private final int _verbosity;
    private FilterRuleConfiguration _filterRuleConfiguration;
    
    private RsyncClient(Builder builder) {
        assert builder != null;
        this._authProvider = builder._authProvider;
        this._isAlwaysItemize = builder._isAlwaysItemize;
        this._isDeferWrite = builder._isDeferWrite;
        this._isDelete = builder._isDelete;
        this._isIgnoreTimes = builder._isIgnoreTimes;
        this._isPreserveDevices = builder._isPreserveDevices;
        this._isPreserveSpecials = builder._isPreserveSpecials;
        this._isPreserveUser = builder._isPreserveUser;
        this._isPreserveGroup = builder._isPreserveGroup;
        this._isPreserveLinks = builder._isPreserveLinks;
        this._isNumericIds = builder._isNumericIds;
        this._isPreservePermissions = builder._isPreservePermissions;
        this._isPreserveTimes = builder._isPreserveTimes;
        this._charset = builder._charset;
        this._filterRuleConfiguration = builder._filterRuleConfiguration;
        
        if (builder._executorService == null) {
            this._executorService = Executors.newCachedThreadPool();
            this._isOwnerOfExecutorService = true;
        } else {
            this._executorService = builder._executorService;
            this._isOwnerOfExecutorService = false;
        }
        this._rsyncTaskExecutor = new RsyncTaskExecutor(this._executorService);
        this._fileSelectionOrNull = builder._fileSelection;
        this._verbosity = builder._verbosity;
        this._stderr = builder._stderr;
    }
}
