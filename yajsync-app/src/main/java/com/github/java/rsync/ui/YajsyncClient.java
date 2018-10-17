/*
 * A simple rsync command line client implementation
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
package com.github.java.rsync.ui;

import java.io.BufferedReader;
import java.io.Console;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.java.rsync.AuthProvider;
import com.github.java.rsync.FileSelection;
import com.github.java.rsync.RsyncClient;
import com.github.java.rsync.RsyncException;
import com.github.java.rsync.RsyncServer;
import com.github.java.rsync.Statistics;
import com.github.java.rsync.attr.DeviceInfo;
import com.github.java.rsync.attr.FileInfo;
import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.SymlinkInfo;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.session.FileAttributeManager;
import com.github.java.rsync.internal.session.FileAttributeManagerFactory;
import com.github.java.rsync.internal.session.FilterRuleConfiguration;
import com.github.java.rsync.internal.session.SessionStatistics;
import com.github.java.rsync.internal.util.ArgumentParser;
import com.github.java.rsync.internal.util.ArgumentParsingError;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.FileOps;
import com.github.java.rsync.internal.util.Option;
import com.github.java.rsync.internal.util.Pair;
import com.github.java.rsync.internal.util.PathOps;
import com.github.java.rsync.internal.util.Triple;
import com.github.java.rsync.internal.util.Util;
import com.github.java.rsync.net.ChannelFactory;
import com.github.java.rsync.net.DuplexByteChannel;
import com.github.java.rsync.net.SSLChannelFactory;
import com.github.java.rsync.net.StandardChannelFactory;

public class YajsyncClient {
    private enum Mode {
        LOCAL_COPY, LOCAL_LIST, REMOTE_LIST, REMOTE_RECEIVE, REMOTE_SEND;

        public boolean isRemote() {
            return this == REMOTE_SEND || this == REMOTE_RECEIVE || this == REMOTE_LIST;
        }
    }

    private static final Logger LOG = Logger.getLogger(YajsyncClient.class.getName());

    private static final int PORT_UNDEFINED = -1;

    private static List<String> readLinesFromStdin() throws IOException {
        List<String> lines = new LinkedList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    return lines;
                }
                lines.add(line);
            }
        }
    }

    private static Pair<RsyncUrls, RsyncUrl> updateRemotePort(Path cwd, int newPortNumber, RsyncUrls srcArgs, RsyncUrl dstArgOrNull) throws ArgumentParsingError {
        try {
            ConnectionInfo connInfo = srcArgs.isRemote() ? srcArgs.getConnectionInfo() : dstArgOrNull.getConnectionInfo();
            // Note: won't detect ambiguous ports if explicitly specifying 873
            // in rsync:// url + something else in --port=
            if (connInfo.getPortNumber() != RsyncServer.DEFAULT_LISTEN_PORT && newPortNumber != connInfo.getPortNumber()) {
                throw new ArgumentParsingError(String.format("ambiguous remote ports: %d != %d", newPortNumber, connInfo.getPortNumber()));
            }
            ConnectionInfo newConnInfo = new ConnectionInfo.Builder(connInfo.getAddress()).portNumber(newPortNumber).userName(connInfo.getUserName()).build();
            if (srcArgs.isRemote()) {
                return new Pair<>(new RsyncUrls(newConnInfo, srcArgs.getModuleName(), srcArgs.getPathNames()), dstArgOrNull);
            } // else if (dstArg.isRemote()) {
            return new Pair<>(srcArgs, new RsyncUrl(cwd, newConnInfo, dstArgOrNull.getModuleName(), dstArgOrNull.getPathName()));
        } catch (IllegalUrlException e) {
            throw new RuntimeException(e);
        }
    }

    private final AuthProvider authProvider = new AuthProvider() {
        @Override
        public char[] getPassword() throws IOException {
            if (YajsyncClient.this.passwordFile != null) {
                if (!YajsyncClient.this.passwordFile.equals("-")) {
                    Path p = Paths.get(YajsyncClient.this.passwordFile);
                    FileAttributeManager fileManager = FileAttributeManagerFactory.newMostAble(p.getFileSystem(), User.NOBODY, Group.NOBODY, Environment.DEFAULT_FILE_PERMS,
                            Environment.DEFAULT_DIR_PERMS);
                    RsyncFileAttributes attrs = fileManager.stat(p);
                    if ((attrs.getMode() & (FileOps.S_IROTH | FileOps.S_IWOTH)) != 0) {
                        throw new IOException(String.format("insecure permissions on %s: %s", YajsyncClient.this.passwordFile, attrs));
                    }
                }
                try (BufferedReader br = new BufferedReader(YajsyncClient.this.passwordFile.equals("-") ? new InputStreamReader(System.in) : new FileReader(YajsyncClient.this.passwordFile))) {
                    return br.readLine().toCharArray();
                }
            }

            String passwordStr = Environment.getRsyncPasswordOrNull();
            if (passwordStr != null) {
                return passwordStr.toCharArray();
            }

            Console console = System.console();
            if (console == null) {
                throw new IOException("no console available");
            }
            return console.readPassword("Password: ");
        }

        @Override
        public String getUser() {
            return YajsyncClient.this.userName;
        }
    };
    private final RsyncClient.Builder clientBuilder = new RsyncClient.Builder().authProvider(this.authProvider);
    private int contimeout = 0;
    private Path cwd;
    private String cwdName = Environment.getWorkingDirectoryName();
    private FileSelection fileSelection;
    private FileSystem fs = FileSystems.getDefault();
    private final List<String> inputFilterRules = new LinkedList<>();
    private String passwordFile;
    private boolean readStdin = false;
    private int remotePort = PORT_UNDEFINED;
    private boolean showStatistics;
    private Statistics statistics = new SessionStatistics();
    private PrintStream stderr = System.out;
    private PrintStream stdout = System.out;
    private final SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private int timeout = 0;
    private String userName;

    private boolean useTLS;

    private int verbosity;

    private String fileInfoToListingString(FileInfo f) {
        RsyncFileAttributes attrs = f.getAttributes();
        Date t = new Date(FileTime.from(attrs.lastModifiedTime(), TimeUnit.SECONDS).toMillis());
        if (f instanceof SymlinkInfo) {
            return String.format("%s %11d %s %s -> %s", FileOps.modeToString(attrs.getMode()), attrs.getSize(), this.timeFormatter.format(t), f.getPathName(), ((SymlinkInfo) f).getTargetPathName());
        } else if (f instanceof DeviceInfo) {
            DeviceInfo d = (DeviceInfo) f;
            return String.format("%s %11d %d,%d %s %s", FileOps.modeToString(attrs.getMode()), attrs.getSize(), d.getMajor(), d.getMinor(), this.timeFormatter.format(t), d.getPathName());
        }
        return String.format("%s %11d %s %s", FileOps.modeToString(attrs.getMode()), attrs.getSize(), this.timeFormatter.format(t), f.getPathName());
    }

    private Iterable<Path> getPaths(Iterable<String> pathNames) {
        List<Path> paths = new LinkedList<>();
        for (String pathName : pathNames) {
            Path p = PathOps.get(this.cwd.getFileSystem(), pathName);
            paths.add(p);
        }
        return paths;
    }

    private List<Option> options() {
        List<Option> options = new LinkedList<>();

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "charset", "", "which charset to use (default UTF-8)", option -> {
            String charsetName = (String) option.getValue();
            try {
                Charset charset = Charset.forName(charsetName);
                this.clientBuilder.charset(charset);
                return ArgumentParser.Status.CONTINUE;
            } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
                throw new ArgumentParsingError(String.format("failed to set character set to %s: %s", charsetName, e));
            }
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "dirs", "d", "transfer directories without recursing (default " + "false unless listing files)", option -> {
            if (this.fileSelection == FileSelection.RECURSE) {
                throw new ArgumentParsingError("--recursive and --dirs are " + "incompatible options");
            }
            this.fileSelection = FileSelection.TRANSFER_DIRS;
            this.clientBuilder.fileSelection(this.fileSelection);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "recursive", "r", "recurse into directories (default false)", option -> {
            if (this.fileSelection == FileSelection.TRANSFER_DIRS) {
                throw new ArgumentParsingError("--recursive and --dirs are " + "incompatible options");
            }
            this.fileSelection = FileSelection.RECURSE;
            this.clientBuilder.fileSelection(this.fileSelection);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "verbose", "v", "increase output verbosity (default quiet)", option -> {
            this.clientBuilder.verbosity(this.verbosity++);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "devices", "", "_simulate_ preserve character device files and " + "block device files (default false)", option -> {
            this.clientBuilder.isPreserveDevices(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "specials", "", "_simulate_ preserve special device files - named " + "sockets and named pipes (default false)", option -> {
            this.clientBuilder.isPreserveSpecials(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "", "D", "same as --devices and --specials (default false)", option -> {
            this.clientBuilder.isPreserveDevices(true);
            this.clientBuilder.isPreserveSpecials(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "links", "l", "preserve symlinks (default false)", option -> {
            this.clientBuilder.isPreserveLinks(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "perms", "p", "preserve file permissions (default false)", option -> {
            this.clientBuilder.isPreservePermissions(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "times", "t", "preserve last modification time (default false)", option -> {
            this.clientBuilder.isPreserveTimes(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "owner", "o", "preserve owner (default false)", option -> {
            this.clientBuilder.isPreserveUser(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "group", "g", "preserve group (default false)", option -> {
            this.clientBuilder.isPreserveGroup(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "archive", "a", "archive mode - same as -rlptgoD (default false)", option -> {
            this.clientBuilder.fileSelection(FileSelection.RECURSE).isPreserveLinks(true).isPreservePermissions(true).isPreserveTimes(true).isPreserveGroup(true).isPreserveUser(true)
                    .isPreserveDevices(true).isPreserveSpecials(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "delete", "", "delete extraneous files (default false)", option -> {
            this.clientBuilder.isDelete(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "numeric-ids", "", "don't map uid/gid values by user/group name " + "(default false)", option -> {
            this.clientBuilder.isNumericIds(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "ignore-times", "I", "transfer files that match both size and time " + "(default false)", option -> {
            this.clientBuilder.isIgnoreTimes(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "stats", "", "show file transfer statistics", option -> {
            this.showStatistics = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "password-file", "", "read daemon-access password from specified file " + "(where `-' is stdin)", option -> {
            this.passwordFile = (String) option.getValue();
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "port", "", String.format("server port number (default %d)", RsyncServer.DEFAULT_LISTEN_PORT), option -> {
            int port = (int) option.getValue();
            if (ConnectionInfo.isValidPortNumber(port)) {
                this.remotePort = port;
                return ArgumentParser.Status.CONTINUE;
            } else {
                throw new ArgumentParsingError(String.format("illegal port %d - must be within " + "the range [%d, %d]", port, ConnectionInfo.PORT_MIN, ConnectionInfo.PORT_MAX));
            }
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "stdin", "", "read list of source files from stdin", option -> {
            this.readStdin = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        String deferredWriteHelp = "(receiver only) receiver defers writing into target tempfile as " + "long as possible to possibly eliminate all I/O writes for "
                + "identical files. This comes at the cost of a highly increased " + "risk of the file being modified by a process already having it " + "opened (default false)";

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "defer-write", "", deferredWriteHelp, option -> {
            this.clientBuilder.isDeferWrite(true);
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "timeout", "", "set I/O read timeout in seconds (default 0 - " + "disabled)", option -> {
            int timeout = (int) option.getValue();
            if (timeout < 0) {
                throw new ArgumentParsingError(String.format("invalid timeout %d - mut be greater " + "than or equal to 0", timeout));
            }
            this.timeout = timeout * 1000;
            // Timeout socket operations depend on
            // ByteBuffer.array and ByteBuffer.arrayOffset.
            // Disable direct allocation if the resulting
            // ByteBuffer won't have an array.
            if (timeout > 0 && !Environment.hasAllocateDirectArray()) {
                Environment.setAllocateDirect(false);
            }
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newIntegerOption(Option.Policy.OPTIONAL, "contimeout", "", "set daemon connection timeout in seconds (default " + "0 - disabled)", option -> {
            int contimeout = (int) option.getValue();
            if (contimeout >= 0) {
                this.contimeout = contimeout * 1000;
                return ArgumentParser.Status.CONTINUE;
            } else {
                throw new ArgumentParsingError(String.format("invalid connection timeout %d - " + "must be greater than or equal to 0", contimeout));
            }
        }));

        options.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "tls", "", String.format("tunnel all data over TLS/SSL " + "(default %s)", this.useTLS), option -> {
            this.useTLS = true;
            // SSLChannel.read and SSLChannel.write depends on
            // ByteBuffer.array and ByteBuffer.arrayOffset.
            // Disable direct allocation if the resulting
            // ByteBuffer won't have an array.
            if (!Environment.hasAllocateDirectArray()) {
                Environment.setAllocateDirect(false);
            }
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "cwd", "", "change current working directory (usable in " + "combination with --fs)", option -> {
            this.cwdName = (String) option.getValue();
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "fs", "", "use a non-default Java nio FileSystem implementation " + "(see also --cwd)", option -> {
            try {
                String fsName = (String) option.getValue();
                this.fs = PathOps.fileSystemOf(fsName);
                this.cwdName = Util.firstOf(this.fs.getRootDirectories()).toString();
                return ArgumentParser.Status.CONTINUE;
            } catch (IOException | URISyntaxException e) {
                throw new ArgumentParsingError(e);
            }
        }));
        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "filter", "f", "add a file-filtering RULE", option -> {
            this.inputFilterRules.add((String) option.getValue());
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "exclude", "", "exclude files matching PATTERN", option -> {

            this.inputFilterRules.add("- " + (String) option.getValue());
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "exclude-from", "", "read exclude patterns from FILE", option -> {
            this.inputFilterRules.add("merge,- " + (String) option.getValue());
            return ArgumentParser.Status.CONTINUE;
        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "include", "", "don't exclude files matching PATTERN", option -> {

            this.inputFilterRules.add("+ " + (String) option.getValue());
            return ArgumentParser.Status.CONTINUE;

        }));

        options.add(Option.newStringOption(Option.Policy.OPTIONAL, "include-from", "", "read list of source-file names from FILE", option -> {
            this.inputFilterRules.add("merge,+ " + (String) option.getValue());
            return ArgumentParser.Status.CONTINUE;

        }));
        return options;
    }

    private Triple<Mode, RsyncUrls, RsyncUrl> parseUnnamedArgs(List<String> unnamed) throws ArgumentParsingError {
        try {
            int len = unnamed.size();
            if (len == 0) {
                throw new ArgumentParsingError(
                        "Please specify at least one " + "non-option argument for (one " + "or more) source files and " + "optionally one destination " + "(defaults to current " + "directory)");
            }
            int numSrcArgs = len == 1 ? 1 : len - 1;
            List<String> srcFileNames = unnamed.subList(0, numSrcArgs);
            RsyncUrls srcUrls = new RsyncUrls(this.cwd, srcFileNames);
            if (len == 1) {
                if (srcUrls.isRemote()) {
                    return new Triple<>(Mode.REMOTE_LIST, srcUrls, null);
                }
                return new Triple<>(Mode.LOCAL_LIST, srcUrls, null);
            }

            int indexOfLast = len - 1;
            String dstFileName = unnamed.get(indexOfLast);
            RsyncUrl dstUrl = RsyncUrl.parse(this.cwd, dstFileName);
            if (srcUrls.isRemote() && dstUrl.isRemote()) {
                throw new ArgumentParsingError(String.format("source arguments %s and destination argument %s must" + " not both be remote", srcUrls, dstUrl));
            } else if (srcUrls.isRemote()) {
                return new Triple<>(Mode.REMOTE_RECEIVE, srcUrls, dstUrl);
            } else if (dstUrl.isRemote()) {
                return new Triple<>(Mode.REMOTE_SEND, srcUrls, dstUrl);
            } else {
                return new Triple<>(Mode.LOCAL_COPY, srcUrls, dstUrl);
            }
        } catch (IllegalUrlException e) {
            throw new ArgumentParsingError(e);
        }
    }

    private RsyncClient.Result remoteTransfer(Mode mode, RsyncUrls srcArgs, RsyncUrl dstArgOrNull) throws RsyncException, InterruptedException {
        ConnectionInfo connInfo = srcArgs.isRemote() ? srcArgs.getConnectionInfo() : dstArgOrNull.getConnectionInfo();
        ChannelFactory socketFactory = this.useTLS ? new SSLChannelFactory() : new StandardChannelFactory();

        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("connecting to %s (TLS=%b)", connInfo, this.useTLS));
        }

        try (DuplexByteChannel sock = socketFactory.open(connInfo.getAddress(), connInfo.getPortNumber(), this.contimeout, this.timeout)) { // throws IOException
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("connected to " + sock);
            }
            this.userName = connInfo.getUserName();

            boolean isInterruptible = !this.useTLS;
            RsyncClient.Remote client = this.clientBuilder.buildRemote(sock /* in */, sock /* out */, isInterruptible);
            switch (mode) {
                case REMOTE_SEND:
                    return client.send(this.getPaths(srcArgs.getPathNames())).to(dstArgOrNull.getModuleName(), dstArgOrNull.getPathName());
                case REMOTE_RECEIVE:
                    return client.receive(srcArgs.getModuleName(), srcArgs.getPathNames()).to(PathOps.get(this.cwd.getFileSystem(), dstArgOrNull.getPathName()));
                case REMOTE_LIST:
                    if (srcArgs.getModuleName().isEmpty()) {
                        RsyncClient.ModuleListing listing = client.listModules();
                        while (true) {
                            String line = listing.take();
                            boolean isDone = line == null;
                            if (isDone) {
                                return listing.get();
                            }
                            System.out.println(line);
                        }
                    } else {
                        RsyncClient.FileListing listing = client.list(srcArgs.getModuleName(), srcArgs.getPathNames());
                        while (true) {
                            FileInfo f = listing.take();
                            boolean isDone = f == null;
                            if (isDone) {
                                return listing.get();
                            }
                            String ls = this.fileInfoToListingString(f);
                            System.out.println(ls);
                        }
                    }
                default:
                    throw new AssertionError(mode);
            }
        } catch (UnknownHostException | UnresolvedAddressException e) {
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.severe(String.format("Error: failed to resolve %s (%s)", connInfo.getAddress(), e.getMessage()));
            }
        } catch (IOException e) { // SocketChannel.{open,close}()
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.severe("Error: socket open/close error: " + e.getMessage());
            }
        } catch (ChannelException e) {
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.log(Level.SEVERE, "Error: communication closed with peer: ", e);
            }
        }
        return RsyncClient.Result.failure();
    }

    public YajsyncClient setStandardErr(PrintStream err) {
        this.stderr = err;
        this.clientBuilder.stderr(this.stderr);
        return this;
    }

    public YajsyncClient setStandardOut(PrintStream out) {
        this.stdout = out;
        return this;
    }

    private void showStatistics(Statistics stats) {
        this.stdout.format(
                "Number of files: %d%n" + "Number of files transferred: %d%n" + "Total file size: %d bytes%n" + "Total transferred file size: %d bytes%n" + "Literal data: %d bytes%n"
                        + "Matched data: %d bytes%n" + "File list size: %d%n" + "File list generation time: %.3f seconds%n" + "File list transfer time: %.3f seconds%n" + "Total bytes sent: %d%n"
                        + "Total bytes received: %d%n",
                stats.getNumFiles(), stats.getNumTransferredFiles(), stats.getTotalFileSize(), stats.getTotalTransferredSize(), stats.getTotalLiteralSize(), stats.getTotalMatchedSize(),
                stats.getTotalFileListSize(), stats.getFileListBuildTime() / 1000.0, stats.getFileListTransferTime() / 1000.0, stats.getTotalBytesWritten(), stats.getTotalBytesRead());
    }

    public int start(String[] args) {
        ArgumentParser argsParser = ArgumentParser.newWithUnnamed(this.getClass().getSimpleName(), "files...");
        argsParser.addHelpTextDestination(this.stdout);

        try {
            for (Option o : this.options()) {
                argsParser.add(o);
            }
            ArgumentParser.Status rc = argsParser.parse(Arrays.asList(args));
            if (rc != ArgumentParser.Status.CONTINUE) {
                return rc == ArgumentParser.Status.EXIT_OK ? 0 : 1;
            }
            this.cwd = this.fs.getPath(this.cwdName);

            List<String> unnamed = new LinkedList<>();
            if (this.readStdin) {
                unnamed.addAll(readLinesFromStdin());
            }
            unnamed.addAll(argsParser.getUnnamedArguments());

            Triple<Mode, RsyncUrls, RsyncUrl> res = this.parseUnnamedArgs(unnamed);
            Mode mode = res.getFirst();
            RsyncUrls srcArgs = res.getSecond();
            RsyncUrl dstArgOrNull = res.getThird();

            if (this.remotePort != PORT_UNDEFINED && mode.isRemote()) {
                Pair<RsyncUrls, RsyncUrl> res2 = updateRemotePort(this.cwd, this.remotePort, srcArgs, dstArgOrNull);
                srcArgs = res2.getFirst();
                dstArgOrNull = res2.getSecond();
            }

            Level logLevel = Util.getLogLevelForNumber(Util.WARNING_LOG_LEVEL_NUM + this.verbosity);
            Util.setRootLogLevel(logLevel);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine(String.format("%s src: %s, dst: %s", mode, srcArgs, dstArgOrNull));
            }
            this.clientBuilder.filterRuleConfiguration(new FilterRuleConfiguration(this.inputFilterRules));
            
            RsyncClient.Result result;
            if (mode.isRemote()) {
                result = this.remoteTransfer(mode, srcArgs, dstArgOrNull);
            } else if (mode == Mode.LOCAL_COPY) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("starting local transfer (using rsync's delta " + "transfer algorithm - i.e. will not run with a " + "--whole-file option, so performance is most "
                            + "probably lower than rsync)");
                }
                result = this.clientBuilder.buildLocal().copy(this.getPaths(srcArgs.getPathNames())).to(PathOps.get(this.cwd.getFileSystem(), dstArgOrNull.getPathName()));
            } else if (mode == Mode.LOCAL_LIST) {
                RsyncClient.FileListing listing = this.clientBuilder.buildLocal().list(this.getPaths(srcArgs.getPathNames()));
                while (true) {
                    FileInfo f = listing.take();
                    boolean isDone = f == null;
                    if (isDone) {
                        result = listing.get();
                        break;
                    }
                    System.out.println(this.fileInfoToListingString(f));
                }
            } else {
                throw new AssertionError();
            }
            this.statistics = result.statistics();
            if (this.showStatistics) {
                this.showStatistics(result.statistics());
            }
            if (LOG.isLoggable(Level.INFO)) {
                LOG.info("exit status: " + (result.isOK() ? "OK" : "ERROR"));
            }
            return result.isOK() ? 0 : -1;

        } catch (ArgumentParsingError e) {
            this.stderr.println(e.getMessage());
            this.stderr.println(argsParser.toUsageString());
        } catch (IOException e) { // reading from stdinp
            this.stderr.println(e.getMessage());
        } catch (RsyncException e) {
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.severe(e.getMessage());
            }
        } catch (InterruptedException e) {
            if (LOG.isLoggable(Level.SEVERE)) {
                LOG.log(Level.SEVERE, "", e);
            }
        }
        return -1;
    }

    public Statistics statistics() {
        return this.statistics;
    }
}
