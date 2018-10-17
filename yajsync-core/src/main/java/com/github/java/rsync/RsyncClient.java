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
package com.github.java.rsync;

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

import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.internal.session.ClientSessionConfig;
import com.github.java.rsync.internal.session.FilterMode;
import com.github.java.rsync.internal.session.FilterRuleConfiguration;
import com.github.java.rsync.internal.session.Generator;
import com.github.java.rsync.internal.session.Receiver;
import com.github.java.rsync.internal.session.RsyncTaskExecutor;
import com.github.java.rsync.internal.session.Sender;
import com.github.java.rsync.internal.session.SessionStatistics;
import com.github.java.rsync.internal.session.SessionStatus;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.util.BitOps;
import com.github.java.rsync.internal.util.Pair;
import com.github.java.rsync.internal.util.Util;

public final class RsyncClient {
    public static class Builder {
        private AuthProvider authProvider = new ConsoleAuthProvider();
        private Charset charset = Charset.forName(Text.UTF8_NAME);
        private ExecutorService executorService;
        private FileSelection fileSelection;
        private boolean alwaysItemize;
        private boolean deferWrite;
        private boolean delete;
        private boolean ignoreTimes;
        private boolean numericIds;
        private boolean preserveDevices;
        private boolean preserveGroup;
        private boolean preserveLinks;
        private boolean preservePermissions;
        private boolean preserveSpecials;
        private boolean preserveTimes;
        private boolean preserveUser;
        private PrintStream stderr = System.err;
        private int verbosity;
        private FilterRuleConfiguration filterRuleConfiguration;
        
        public Builder authProvider(AuthProvider authProvider) {
            this.authProvider = authProvider;
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
            this.charset = charset;
            return this;
        }
        
        public Builder executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }
        
        public Builder fileSelection(FileSelection fileSelection) {
            this.fileSelection = fileSelection;
            return this;
        }
        
        public Builder isAlwaysItemize(boolean isAlwaysItemize) {
            this.alwaysItemize = isAlwaysItemize;
            return this;
        }
        
        public Builder isDeferWrite(boolean isDeferWrite) {
            this.deferWrite = isDeferWrite;
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
        
        public Builder stderr(PrintStream stderr) {
            this.stderr = stderr;
            return this;
        }
        
        public Builder verbosity(int verbosity) {
            this.verbosity = verbosity;
            return this;
        }
        
        public Builder filterRuleConfiguration(FilterRuleConfiguration filterRuleConfiguration) {
            this.filterRuleConfiguration = filterRuleConfiguration;
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
        private final Future<Result> future;
        private final CountDownLatch listingAvailable;
        private BlockingQueue<Pair<Boolean, FileInfo>> listing;
        
        private FileListing(final ClientSessionConfig cfg, final String moduleName, final List<String> serverArgs, final AuthProvider authProvider, final ReadableByteChannel in,
                final WritableByteChannel out, final boolean isInterruptible, final FileSelection fileSelection)
        
        {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws RsyncException, InterruptedException {
                    try {
                        SessionStatus status = cfg.handshake(moduleName, serverArgs, authProvider);
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("handshake status: " + status);
                        }
                        if (status == SessionStatus.ERROR) {
                            return Result.failure();
                        } else if (status == SessionStatus.EXIT) {
                            return Result.success();
                        }
                        Generator generator = new Generator.Builder(out, cfg.getChecksumSeed())//
                                .charset(cfg.getCharset())//
                                .fileSelection(fileSelection)//
                                .isDelete(RsyncClient.this.delete) //
                                .isPreserveDevices(RsyncClient.this.preserveDevices)//
                                .isPreserveSpecials(RsyncClient.this.preserveSpecials)//
                                .isPreserveLinks(RsyncClient.this.preserveLinks) //
                                .isPreservePermissions(RsyncClient.this.preservePermissions)//
                                .isPreserveTimes(RsyncClient.this.preserveTimes)//
                                .isPreserveUser(RsyncClient.this.preserveUser) //
                                .isPreserveGroup(RsyncClient.this.preserveGroup)//
                                .isNumericIds(RsyncClient.this.numericIds)//
                                .isIgnoreTimes(RsyncClient.this.ignoreTimes) //
                                .isAlwaysItemize(RsyncClient.this.verbosity > 1)//
                                .isInterruptible(isInterruptible)//
                                .build();
                        
                        FileListing.this.listing = generator.getFiles();
                        FileListing.this.listingAvailable.countDown();
                        Receiver receiver = Receiver.Builder.newListing(generator, in).filterMode(FilterMode.SEND).isDeferWrite(RsyncClient.this.deferWrite).isExitAfterEOF(true)
                                .isExitEarlyIfEmptyList(true).isReceiveStatistics(true).isSafeFileList(cfg.isSafeFileList()).build();
                        boolean isOK = RsyncClient.this.rsyncTaskExecutor.exec(generator, receiver);
                        return new Result(isOK, receiver.statistics());
                    } finally {
                        if (FileListing.this.listing == null) {
                            FileListing.this.listing = new LinkedBlockingQueue<>();
                            FileListing.this.listing.put(new Pair<Boolean, FileInfo>(false, null));
                            FileListing.this.listingAvailable.countDown();
                        }
                        if (RsyncClient.this.ownerOfExecutorService) {
                            if (LOG.isLoggable(Level.FINE)) {
                                LOG.fine("shutting down " + RsyncClient.this.executorService);
                            }
                            RsyncClient.this.executorService.shutdown();
                        }
                    }
                }
            };
            this.future = RsyncClient.this.executorService.submit(callable);
            this.listingAvailable = new CountDownLatch(1);
        }
        
        private FileListing(final Sender sender, final Generator generator, final Receiver receiver) {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws RsyncException, InterruptedException {
                    try {
                        boolean isOK = RsyncClient.this.rsyncTaskExecutor.exec(sender, generator, receiver);
                        return new Result(isOK, receiver.statistics());
                    } finally {
                        if (RsyncClient.this.ownerOfExecutorService) {
                            if (LOG.isLoggable(Level.FINE)) {
                                LOG.fine("shutting down " + RsyncClient.this.executorService);
                            }
                            RsyncClient.this.executorService.shutdown();
                        }
                    }
                }
            };
            this.future = RsyncClient.this.executorService.submit(callable);
            this.listing = generator.getFiles();
            this.listingAvailable = new CountDownLatch(0);
        }
        
        public Future<Result> futureResult() {
            return this.future;
        }
        
        public Result get() throws InterruptedException, RsyncException {
            try {
                return this.future.get();
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
            this.listingAvailable.await();
            Pair<Boolean, FileInfo> res = this.listing.take();
            boolean isDone = res.getFirst() == null;
            if (isDone) {
                return null;
            } else {
                return res.getSecond();
            }
        }
    }
    
    public class Local {
        public class Copy {
            private final Iterable<Path> srcPaths;
            
            private Copy(Iterable<Path> srcPaths) {
                assert srcPaths != null;
                this.srcPaths = srcPaths;
            }
            
            public Result to(Path dstPath) throws RsyncException, InterruptedException {
                assert dstPath != null;
                return Local.this.localTransfer(this.srcPaths, dstPath);
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
            FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this.fileSelection, FileSelection.TRANSFER_DIRS);
            byte[] seed = BitOps.toLittleEndianBuf((int) System.currentTimeMillis());
            
            Sender sender = new Sender.Builder(toSender.source(), toReceiver.sink(), srcPaths, seed)//
                    .isExitEarlyIfEmptyList(true)//
                    .charset(RsyncClient.this.charset) //
                    .isPreserveDevices(RsyncClient.this.preserveDevices) //
                    .isPreserveSpecials(RsyncClient.this.preserveSpecials) //
                    .isPreserveLinks(RsyncClient.this.preserveLinks) //
                    .isPreserveUser(RsyncClient.this.preserveUser)//
                    .isPreserveGroup(RsyncClient.this.preserveGroup)//
                    .isNumericIds(RsyncClient.this.numericIds)//
                    .fileSelection(fileSelection).build();
            
            Generator generator = new Generator.Builder(toSender.sink(), seed)//
                    .charset(RsyncClient.this.charset)//
                    .fileSelection(fileSelection)//
                    .isDelete(RsyncClient.this.delete).isPreserveDevices(RsyncClient.this.preserveDevices)//
                    .isPreserveSpecials(RsyncClient.this.preserveSpecials)//
                    .isPreserveLinks(RsyncClient.this.preserveLinks) //
                    .isPreservePermissions(RsyncClient.this.preservePermissions)//
                    .isPreserveTimes(RsyncClient.this.preserveTimes)//
                    .isPreserveUser(RsyncClient.this.preserveUser) //
                    .isPreserveGroup(RsyncClient.this.preserveGroup)//
                    .isNumericIds(RsyncClient.this.numericIds)//
                    .isIgnoreTimes(RsyncClient.this.ignoreTimes) //
                    .isAlwaysItemize(RsyncClient.this.alwaysItemize)//
                    .build();
            
            Receiver receiver = Receiver.Builder.newListing(generator, toReceiver.source()).isExitEarlyIfEmptyList(true).isDeferWrite(RsyncClient.this.deferWrite).build();
            
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
            FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this.fileSelection, FileSelection.EXACT);
            byte[] seed = BitOps.toLittleEndianBuf((int) System.currentTimeMillis());
            Sender sender = new Sender//
                    .Builder(toSender.source(), toReceiver.sink(), srcPaths, seed)//
                            .isExitEarlyIfEmptyList(true).charset(RsyncClient.this.charset)//
                            .isPreserveDevices(RsyncClient.this.preserveDevices)//
                            .isPreserveSpecials(RsyncClient.this.preserveSpecials)//
                            .isPreserveLinks(RsyncClient.this.preserveLinks)//
                            .isPreserveUser(RsyncClient.this.preserveUser)//
                            .isPreserveGroup(RsyncClient.this.preserveGroup)//
                            .isNumericIds(RsyncClient.this.numericIds)//
                            .fileSelection(fileSelection).build();

            Generator generator = new Generator//
                    .Builder(toSender.sink(), seed)//
                            .charset(RsyncClient.this.charset)//
                            .fileSelection(fileSelection)//
                            .isDelete(RsyncClient.this.delete)//
                            .isPreserveDevices(RsyncClient.this.preserveDevices)//
                            .isPreserveSpecials(RsyncClient.this.preserveSpecials)//
                            .isPreserveLinks(RsyncClient.this.preserveLinks)//
                            .isPreservePermissions(RsyncClient.this.preservePermissions)//
                            .isPreserveTimes(RsyncClient.this.preserveTimes)//
                            .isPreserveUser(RsyncClient.this.preserveUser)//
                            .isPreserveGroup(RsyncClient.this.preserveGroup)//
                            .isNumericIds(RsyncClient.this.numericIds)//
                            .isIgnoreTimes(RsyncClient.this.ignoreTimes)//
                            .isAlwaysItemize(RsyncClient.this.alwaysItemize).build();

            Receiver receiver = new Receiver//
                    .Builder(generator, toReceiver.source(), dstPath)//
                            .isExitEarlyIfEmptyList(true)//
                            .isDeferWrite(RsyncClient.this.deferWrite)//
                            .build();
            try {
                boolean isOK = RsyncClient.this.rsyncTaskExecutor.exec(sender, generator, receiver);
                return new Result(isOK, receiver.statistics());
            } finally {
                if (RsyncClient.this.ownerOfExecutorService) {
                    RsyncClient.this.executorService.shutdown();
                }
            }
        }
    }
    
    private enum Mode {
        LOCAL_COPY, LOCAL_LIST, REMOTE_LIST, REMOTE_RECEIVE, REMOTE_SEND
    }
    
    public class ModuleListing {
        private final Future<Result> future;
        private final BlockingQueue<Pair<Boolean, String>> moduleNames;
        
        private ModuleListing(final ClientSessionConfig cfg, final List<String> serverArgs) {
            Callable<Result> callable = new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    try {
                        SessionStatus status = cfg.handshake("", serverArgs, RsyncClient.this.authProvider);
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("handshake status: " + status);
                        }
                        if (status == SessionStatus.ERROR) {
                            return Result.failure();
                        } else if (status == SessionStatus.EXIT) {
                            return Result.success();
                        }
                        throw new AssertionError();
                    } finally {
                        if (RsyncClient.this.ownerOfExecutorService) {
                            if (LOG.isLoggable(Level.FINE)) {
                                LOG.fine("shutting down " + RsyncClient.this.executorService);
                            }
                            RsyncClient.this.executorService.shutdown();
                        }
                    }
                }
            };
            this.future = RsyncClient.this.executorService.submit(callable);
            this.moduleNames = cfg.modules();
        }
        
        public Future<Result> futureResult() {
            return this.future;
        }
        
        public Result get() throws InterruptedException, RsyncException {
            try {
                return this.future.get();
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
            Pair<Boolean, String> res = this.moduleNames.take();
            boolean isDone = res.getFirst() == null;
            if (isDone) {
                return null;
            } else {
                return res.getSecond();
            }
        }
    }
    
    public class Remote {
        public class Receive {
            private final String moduleName;
            private final Iterable<String> srcPathNames;
            
            private Receive(String moduleName, Iterable<String> srcPathNames) {
                assert moduleName != null;
                assert srcPathNames != null;
                this.moduleName = moduleName;
                this.srcPathNames = srcPathNames;
            }
            
            public Result to(Path dstPath) throws RsyncException, InterruptedException {
                assert dstPath != null;
                FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this.fileSelection, FileSelection.EXACT);
                
                List<String> serverArgs = Remote.this.createServerArgs(Mode.REMOTE_RECEIVE, fileSelection);
                for (String s : this.srcPathNames) {
                    assert s.startsWith(Text.SLASH) : s;
                    serverArgs.add(this.moduleName + s);
                }
                
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("file selection: %s, src: %s, dst: " + "%s, remote args: %s", fileSelection, this.srcPathNames, dstPath, serverArgs));
                }
                
                ClientSessionConfig cfg = new ClientSessionConfig(Remote.this.in, Remote.this.out, RsyncClient.this.charset, fileSelection == FileSelection.RECURSE, RsyncClient.this.stderr);
                SessionStatus status = cfg.handshake(this.moduleName, serverArgs, RsyncClient.this.authProvider);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("handshake status: " + status);
                }
                
                if (status == SessionStatus.ERROR) {
                    return Result.failure();
                } else if (status == SessionStatus.EXIT) {
                    return Result.success();
                }
                
                try {
                    Generator generator = new Generator//
                            .Builder(Remote.this.out, cfg.getChecksumSeed())//
                                    .charset(cfg.getCharset())//
                                    .fileSelection(fileSelection)//
                                    .isDelete(RsyncClient.this.delete)//
                                    .isPreserveLinks(RsyncClient.this.preserveLinks)//
                                    .isPreservePermissions(RsyncClient.this.preservePermissions)//
                                    .isPreserveTimes(RsyncClient.this.preserveTimes)//
                                    .isPreserveUser(RsyncClient.this.preserveUser)//
                                    .isPreserveGroup(RsyncClient.this.preserveGroup)//
                                    .isNumericIds(RsyncClient.this.numericIds)//
                                    .isIgnoreTimes(RsyncClient.this.ignoreTimes)//
                                    .isAlwaysItemize(RsyncClient.this.verbosity > 1)//
                                    .isInterruptible(Remote.this.interruptible)//
                                    .build();
                    Receiver receiver = new Receiver//
                            .Builder(generator, Remote.this.in, dstPath)//
                                    .filterMode(FilterMode.SEND)//
                                    .isDeferWrite(RsyncClient.this.deferWrite)//
                                    .isExitAfterEOF(true)//
                                    .isExitEarlyIfEmptyList(true)//
                                    .isReceiveStatistics(true)//
                                    .isSafeFileList(cfg.isSafeFileList())//
                                    .build();
                    boolean isOK = RsyncClient.this.rsyncTaskExecutor.exec(generator, receiver);
                    return new Result(isOK, receiver.statistics());
                } finally {
                    if (RsyncClient.this.ownerOfExecutorService) {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("shutting down " + RsyncClient.this.executorService);
                        }
                        RsyncClient.this.executorService.shutdown();
                    }
                }
            }
        }
        
        public class Send {
            private final Iterable<Path> srcPaths;
            
            private Send(Iterable<Path> srcPaths) {
                assert srcPaths != null;
                this.srcPaths = srcPaths;
            }
            
            public Result to(String moduleName, String dstPathName) throws RsyncException, InterruptedException {
                assert moduleName != null;
                assert dstPathName != null;
                assert dstPathName.startsWith(Text.SLASH);
                
                FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this.fileSelection, FileSelection.EXACT);
                List<String> serverArgs = Remote.this.createServerArgs(Mode.REMOTE_SEND, fileSelection);
                serverArgs.add(moduleName + dstPathName);
                
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("file selection: %s, src: %s, dst: %s, remote " + "args: %s", fileSelection, this.srcPaths, dstPathName, serverArgs));
                }
                
                ClientSessionConfig cfg = new ClientSessionConfig(Remote.this.in, Remote.this.out, RsyncClient.this.charset, fileSelection == FileSelection.RECURSE, RsyncClient.this.stderr);
                SessionStatus status = cfg.handshake(moduleName, serverArgs, RsyncClient.this.authProvider);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("handshake status: " + status);
                }
                
                if (status == SessionStatus.ERROR) {
                    return Result.failure();
                } else if (status == SessionStatus.EXIT) {
                    return Result.success();
                }
                
                try {
                    Sender sender = Sender.Builder.newClient(Remote.this.in, Remote.this.out, this.srcPaths, cfg.getChecksumSeed())
                            .filterMode(RsyncClient.this.delete ? FilterMode.SEND : FilterMode.NONE)//
                            .charset(RsyncClient.this.charset)//
                            .fileSelection(fileSelection)//
                            .filterRuleConfiguration(RsyncClient.this.filterRuleConfiguration)//
                            .isPreserveLinks(RsyncClient.this.preserveLinks)//
                            .isPreserveUser(RsyncClient.this.preserveUser)//
                            .isPreserveGroup(RsyncClient.this.preserveGroup)//
                            .isNumericIds(RsyncClient.this.numericIds)//
                            .isInterruptible(Remote.this.interruptible)//
                            .isSafeFileList(cfg.isSafeFileList())//
                            .build();
                    boolean isOK = RsyncClient.this.rsyncTaskExecutor.exec(sender);
                    return new Result(isOK, sender.statistics());
                } finally {
                    if (RsyncClient.this.ownerOfExecutorService) {
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("shutting down " + RsyncClient.this.executorService);
                        }
                        RsyncClient.this.executorService.shutdown();
                    }
                }
            }
        }
        
        private final ReadableByteChannel in;
        
        private final boolean interruptible;
        
        private final WritableByteChannel out;
        
        public Remote(ReadableByteChannel in, WritableByteChannel out, boolean isInterruptible) {
            assert in != null;
            assert out != null;
            this.in = in;
            this.out = out;
            this.interruptible = isInterruptible;
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
            for (int i = 0; i < RsyncClient.this.verbosity; i++) {
                sb.append("v");
            }
            if (RsyncClient.this.preserveLinks) {
                sb.append("l");
            }
            if (fileSelection == FileSelection.TRANSFER_DIRS) {
                sb.append("d");
            }
            if (RsyncClient.this.preservePermissions) {
                sb.append("p");
            }
            if (RsyncClient.this.preserveTimes) {
                sb.append("t");
            }
            if (RsyncClient.this.preserveUser) {
                sb.append("o");
            }
            if (RsyncClient.this.preserveGroup) {
                sb.append("g");
            }
            if (RsyncClient.this.preserveDevices) {
                sb.append("D");
            }
            if (RsyncClient.this.ignoreTimes) {
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
            
            if (RsyncClient.this.delete && mode == Mode.REMOTE_SEND) {
                serverArgs.add("--delete");
            }
            if (RsyncClient.this.numericIds) {
                serverArgs.add("--numeric-ids");
            }
            if (RsyncClient.this.delete && RsyncClient.this.fileSelection == FileSelection.TRANSFER_DIRS) {
                // seems like it's only safe to use --delete and --dirs with
                // rsync versions that happens to support --no-r
                serverArgs.add("--no-r");
            }
            if (RsyncClient.this.preserveDevices && !RsyncClient.this.preserveSpecials) {
                serverArgs.add("--no-specials");
            }
            
            serverArgs.add("."); // arg delimiter
            
            return serverArgs;
        }
        
        public FileListing list(String moduleName, Iterable<String> srcPathNames) {
            assert moduleName != null;
            assert srcPathNames != null;
            FileSelection fileSelection = Util.defaultIfNull(RsyncClient.this.fileSelection, FileSelection.TRANSFER_DIRS);
            List<String> serverArgs = this.createServerArgs(Mode.REMOTE_LIST, fileSelection);
            for (String s : srcPathNames) {
                assert s.startsWith(Text.SLASH) : s;
                serverArgs.add(moduleName + s);
            }
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("file selection: %s, src: %s, remote args: %s", fileSelection, srcPathNames, serverArgs));
            }
            ClientSessionConfig cfg = new ClientSessionConfig(this.in, this.out, RsyncClient.this.charset, fileSelection == FileSelection.RECURSE, RsyncClient.this.stderr);
            return new FileListing(cfg, moduleName, serverArgs, RsyncClient.this.authProvider, this.in, this.out, this.interruptible, fileSelection);
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
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("file selection: %s, src: %s, remote " + "args: %s", fileSelection, srcPathNames, serverArgs));
            }
            ClientSessionConfig cfg = new ClientSessionConfig(this.in, this.out, RsyncClient.this.charset, fileSelection == FileSelection.RECURSE, RsyncClient.this.stderr);
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
        
        private final boolean isOK;
        
        private final Statistics statistics;
        
        private Result(boolean isOK, Statistics statistics) {
            this.isOK = isOK;
            this.statistics = statistics;
        }
        
        public boolean isOK() {
            return this.isOK;
        }
        
        public Statistics statistics() {
            return this.statistics;
        }
    }
    
    private static final Logger LOG = Logger.getLogger(RsyncClient.class.getName());
    
    private static Pipe[] pipePair() {
        try {
            return new Pipe[] { Pipe.open(), Pipe.open() };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private final AuthProvider authProvider;
    private final Charset charset;
    private final ExecutorService executorService;
    private final FileSelection fileSelection;
    private final boolean alwaysItemize;
    private final boolean deferWrite;
    private final boolean delete;
    private final boolean ignoreTimes;
    private final boolean numericIds;
    private final boolean ownerOfExecutorService;
    private final boolean preserveDevices;
    private final boolean preserveGroup;
    private final boolean preserveLinks;
    private final boolean preservePermissions;
    private final boolean preserveSpecials;
    private final boolean preserveTimes;
    private final boolean preserveUser;
    private final RsyncTaskExecutor rsyncTaskExecutor;
    private final PrintStream stderr;
    private final int verbosity;
    private final FilterRuleConfiguration filterRuleConfiguration;
    
    private RsyncClient(Builder builder) {
        assert builder != null;
        this.authProvider = builder.authProvider;
        this.alwaysItemize = builder.alwaysItemize;
        this.deferWrite = builder.deferWrite;
        this.delete = builder.delete;
        this.ignoreTimes = builder.ignoreTimes;
        this.preserveDevices = builder.preserveDevices;
        this.preserveSpecials = builder.preserveSpecials;
        this.preserveUser = builder.preserveUser;
        this.preserveGroup = builder.preserveGroup;
        this.preserveLinks = builder.preserveLinks;
        this.numericIds = builder.numericIds;
        this.preservePermissions = builder.preservePermissions;
        this.preserveTimes = builder.preserveTimes;
        this.charset = builder.charset;
        this.filterRuleConfiguration = builder.filterRuleConfiguration;
        
        if (builder.executorService == null) {
            this.executorService = Executors.newCachedThreadPool();
            this.ownerOfExecutorService = true;
        } else {
            this.executorService = builder.executorService;
            this.ownerOfExecutorService = false;
        }
        this.rsyncTaskExecutor = new RsyncTaskExecutor(this.executorService);
        this.fileSelection = builder.fileSelection;
        this.verbosity = builder.verbosity;
        this.stderr = builder.stderr;
    }
}
