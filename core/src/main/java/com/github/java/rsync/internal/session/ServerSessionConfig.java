/*
 * Rsync server -> client handshaking protocol
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

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.github.java.rsync.FileSelection;
import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.RsyncSecurityException;
import com.github.java.rsync.internal.channels.ChannelEOFException;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.text.TextConversionException;
import com.github.java.rsync.internal.util.ArgumentParser;
import com.github.java.rsync.internal.util.ArgumentParsingError;
import com.github.java.rsync.internal.util.BitOps;
import com.github.java.rsync.internal.util.Consts;
import com.github.java.rsync.internal.util.MemoryPolicy;
import com.github.java.rsync.internal.util.Option;
import com.github.java.rsync.internal.util.OverflowException;
import com.github.java.rsync.internal.util.Util;
import com.github.java.rsync.server.module.Module;
import com.github.java.rsync.server.module.ModuleException;
import com.github.java.rsync.server.module.ModuleSecurityException;
import com.github.java.rsync.server.module.Modules;
import com.github.java.rsync.server.module.RestrictedModule;
import com.github.java.rsync.server.module.RsyncAuthContext;

public class ServerSessionConfig extends SessionConfig {
    private static final Logger LOG = Logger.getLogger(ServerSessionConfig.class.getName());

    /**
     * @throws RsyncSecurityException
     * @throws IllegalArgumentException if charset is not supported
     * @throws RsyncProtocolException   if failing to encode/decode characters
     *                                  correctly
     * @throws RsyncProtocolException   if failed to parse arguments sent by peer
     *                                  correctly
     */
    public static ServerSessionConfig handshake(Charset charset, ReadableByteChannel in, WritableByteChannel out, Modules modules)
            throws ChannelException, RsyncProtocolException, RsyncSecurityException {
        assert charset != null;
        assert in != null;
        assert out != null;
        assert modules != null;

        ServerSessionConfig instance = new ServerSessionConfig(in, out, charset);
        try {
            instance.getExchangeProtocolVersion();
            String moduleName = instance.receiveModule();

            if (moduleName.isEmpty()) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("sending module listing and exiting");
                }
                instance.sendModuleListing(modules.all());
                instance.sendStatus(SessionStatus.EXIT);
                instance.status = SessionStatus.EXIT; // FIXME: create separate status type instead
                return instance;
            }

            Module module = modules.get(moduleName); // throws ModuleException
            if (module instanceof RestrictedModule) {
                RestrictedModule restrictedModule = (RestrictedModule) module;
                module = instance.unlockModule(restrictedModule); // throws ModuleSecurityException
            }
            instance.setModule(module);
            instance.sendStatus(SessionStatus.OK);
            instance.status = SessionStatus.OK;

            Collection<String> args = instance.receiveArguments();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("parsing arguments: " + args);
            }
            instance.parseArguments(args);
            instance.sendCompatibilities();
            instance.sendChecksumSeed();
            return instance;
        } catch (ArgumentParsingError | TextConversionException e) {
            throw new RsyncProtocolException(e);
        } catch (ModuleException e) {
            if (LOG.isLoggable(Level.WARNING)) {
                LOG.warning(e.getMessage());
            }
            instance.sendErrorStatus(e.getMessage());
            instance.status = SessionStatus.ERROR;
            return instance;
        } finally {
            instance.flush();
        }
    }

    private boolean delete = false;
    private FileSelection fileSelection = FileSelection.EXACT;
    private boolean ignoreTimes = false;
    private boolean incrementalRecurse = false;
    private Module module;
    private boolean numericIds = false;
    private boolean preserveDevices = false;
    private boolean preserveGroup = false;
    private boolean preserveLinks = false;
    private boolean preservePermissions = false;
    private boolean preserveSpecials = false;
    private boolean preserveTimes = false;
    private boolean preserveUser = false;
    private Path receiverDestination;
    private boolean safeFileList;
    private boolean sender = false;
    private final List<Path> sourceFiles = new LinkedList<>();

    private int verbosity = 0;

    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    private ServerSessionConfig(ReadableByteChannel in, WritableByteChannel out, Charset charset) {
        super(in, out, charset);
        int seedValue = (int) System.currentTimeMillis();
        checksumSeed = BitOps.toLittleEndianBuf(seedValue);
    }

    public FileSelection fileSelection() {
        return fileSelection;
    }

    private void flush() throws ChannelException {
        peerConnection.flush();
    }

    public Path getReceiverDestination() {
        assert receiverDestination != null;
        return receiverDestination;
    }

    public List<Path> getSourceFiles() {
        return sourceFiles;
    }

    public int getVerbosity() {
        return verbosity;
    }

    public boolean isDelete() {
        return delete;
    }

    public boolean isIgnoreTimes() {
        return ignoreTimes;
    }

    public boolean isNumericIds() {
        return numericIds;
    }

    public boolean isPreserveDevices() {
        return preserveDevices;
    }

    public boolean isPreserveGroup() {
        return preserveGroup;
    }

    public boolean isPreserveLinks() {
        return preserveLinks;
    }

    public boolean isPreservePermissions() {
        return preservePermissions;
    }

    public boolean isPreserveSpecials() {
        return preserveSpecials;
    }

    public boolean isPreserveTimes() {
        return preserveTimes;
    }

    public boolean isPreserveUser() {
        return preserveUser;
    }

    public boolean isSafeFileList() {
        return safeFileList;
    }

    public boolean isSender() {
        return sender;
    }

    private void parseArguments(Collection<String> receivedArguments) throws ArgumentParsingError, RsyncProtocolException, RsyncSecurityException {
        ArgumentParser argsParser = ArgumentParser.newWithUnnamed("", "files...");
        // NOTE: has no argument handler
        argsParser.add(Option.newWithoutArgument(Option.Policy.REQUIRED, "server", "", "", null));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "sender", "", "", option -> {
            sender = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "recursive", "r", "", option -> {
            fileSelection = FileSelection.RECURSE;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "no-r", "", "", option -> {
            // is sent when transfer dirs and delete
            if (fileSelection == FileSelection.RECURSE) {
                fileSelection = FileSelection.EXACT;
            }
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newStringOption(Option.Policy.REQUIRED, "rsh", "e", "", option -> {
            try {
                String val = (String) option.getValue();
                parsePeerCompatibilites(val);
                return ArgumentParser.Status.CONTINUE;
            } catch (RsyncProtocolException e) {
                throw new ArgumentParsingError(e);
            }
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "ignore-times", "I", "", option -> {
            ignoreTimes = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "verbose", "v", "", option -> {
            verbosity++;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "delete", "", "", option -> {
            delete = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "", "D", "", option -> {
            preserveDevices = true;
            preserveSpecials = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "specials", "", "", option -> {
            preserveSpecials = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "no-specials", "", "", option -> {
            preserveSpecials = false;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "links", "l", "", option -> {
            preserveLinks = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "owner", "o", "", option -> {
            preserveUser = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "group", "g", "", option -> {
            preserveGroup = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "numeric-ids", "", "", option -> {
            numericIds = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "perms", "p", "", option -> {
            preservePermissions = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "times", "t", "", option -> {
            preserveTimes = true;
            return ArgumentParser.Status.CONTINUE;
        }));

        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "dirs", "d", "", option -> {
            fileSelection = FileSelection.TRANSFER_DIRS;
            return ArgumentParser.Status.CONTINUE;
        }));

        // FIXME: let ModuleProvider mutate this argsParser instance before
        // calling parse (e.g. adding specific options or removing options)

        ArgumentParser.Status rc = argsParser.parse(receivedArguments);
        assert rc == ArgumentParser.Status.CONTINUE;
        assert fileSelection != FileSelection.RECURSE || incrementalRecurse : "We support only incremental recursive transfers for now";

        if (!isSender() && !module.isWritable()) {
            throw new RsyncProtocolException(String.format("Error: module %s is not writable", module));
        }

        List<String> unnamed = argsParser.getUnnamedArguments();
        if (unnamed.size() < 2) {
            throw new RsyncProtocolException(String.format("Got too few unnamed arguments from peer " + "(%d), expected \".\" and more", unnamed.size()));
        }
        String dotSeparator = unnamed.remove(0);
        if (!dotSeparator.equals(Text.DOT)) {
            throw new RsyncProtocolException(String.format("Expected first non option-argument to be " + "\".\", received \"%s\"", dotSeparator));
        }

        if (isSender()) {
            Pattern wildcardsPattern = Pattern.compile(".*[\\[*?].*"); // matches literal [, * or ?
            for (String fileName : unnamed) {
                if (wildcardsPattern.matcher(fileName).matches()) {
                    // FIXME: Do we need this?
                    // public static DirectoryStream<Path>
                    // java.nio.file.Files.newDirectoryStream(Path dir, String glob) throws
                    // IOException
                    throw new RsyncProtocolException(String.format("wildcards are not supported (%s)", fileName));
                }
                Path safePath = module.getRestrictedPath().resolve(fileName);
                sourceFiles.add(safePath);
            }
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("sender source files: " + sourceFiles);
            }
        } else {
            if (unnamed.size() != 1) {
                throw new RsyncProtocolException(String.format("Error: expected exactly one file argument: %s contains %d", unnamed, unnamed.size()));
            }
            String fileName = unnamed.get(0);
            Path safePath = module.getRestrictedPath().resolve(fileName);
            receiverDestination = safePath.normalize();

            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("receiver destination: " + receiverDestination);
            }
        }
    }

    // @throws RsyncProtocolException
    private void parsePeerCompatibilites(String str) throws RsyncProtocolException {
        if (str.startsWith(Text.DOT)) {
            if (str.contains("i")) { // CF_INC_RECURSE
                assert fileSelection == FileSelection.RECURSE;
                incrementalRecurse = true; // only set by client on --recursive or -r, but can also be disabled, we require
                                           // it however (as a start)
            }
            if (str.contains("L")) { // CF_SYMLINK_TIMES
            }
            if (str.contains("s")) { // CF_SYMLINK_ICONV
            }
            safeFileList = str.contains("f");
        } else {
            throw new RsyncProtocolException(String.format("Protocol not supported - got %s from peer", str));
        }
    }

    private String readStringUntilNullOrEof() throws ChannelException, RsyncProtocolException {
        ByteBuffer buf = ByteBuffer.allocate(64);
        try {
            while (true) {
                byte b = peerConnection.getByte();
                if (b == Text.ASCII_NULL) {
                    break;
                } else if (!buf.hasRemaining()) {
                    buf = Util.enlargeByteBuffer(buf, MemoryPolicy.IGNORE, Consts.MAX_BUF_SIZE);
                }
                buf.put(b);
            }
        } catch (OverflowException e) {
            throw new RsyncProtocolException(e);
        } catch (ChannelEOFException e) {
            // EOF is OK
        }
        buf.flip();
        try {
            return characterDecoder.decode(buf);
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        }
    }

    /**
     *
     * @ @throws ChannelException
     * @throws RsyncProtocolException
     */
    private Collection<String> receiveArguments() throws ChannelException, RsyncProtocolException {
        Collection<String> list = new LinkedList<>();
        while (true) {
            String arg = readStringUntilNullOrEof();
            if (arg.isEmpty()) {
                break;
            }
            list.add(arg);
        }
        return list;
    }

    /**
     * @throws RsyncProtocolException if failing to decode input characters using
     *                                current character set
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of characters
     */
    private String receiveModule() throws ChannelException, RsyncProtocolException {
        return readLine();
    }

    private void sendChecksumSeed() throws ChannelException {
        assert checksumSeed != null;
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("> (checksum seed) " + BitOps.toBigEndianInt(checksumSeed));
        }
        peerConnection.putInt(BitOps.toBigEndianInt(checksumSeed));
    }

    private void sendCompatibilities() throws ChannelException {
        byte flags = 0;
        if (safeFileList) {
            flags |= RsyncCompatibilities.CF_SAFE_FLIST;
        }
        if (incrementalRecurse) {
            flags |= RsyncCompatibilities.CF_INC_RECURSE;
        }
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("> (we support) " + flags);
        }
        peerConnection.putByte(flags);
    }

    private void sendErrorStatus(String msg) throws ChannelException {
        writeString(String.format("%s: %s\n", SessionStatus.ERROR.toString(), msg));
    }

    /**
     * @throws TextConversionException
     */
    private void sendModuleListing(Iterable<Module> modules) throws ChannelException {
        for (Module module : modules) {
            assert !module.getName().isEmpty();
            if (module.getComment().isEmpty()) {
                writeString(String.format("%-15s\n", module.getName()));
            } else {
                writeString(String.format("%-15s\t%s\n", module.getName(), module.getComment()));
            }
        }
    }

    private void sendStatus(SessionStatus status) throws ChannelException {
        writeString(status.toString() + "\n");
    }

    private void setModule(Module module) {
        this.module = module;
    }

    /**
     * @throws RsyncProtocolException if failing to decode input characters using
     *                                current character set
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of characters
     */
    private Module unlockModule(RestrictedModule restrictedModule) throws ModuleSecurityException, ChannelException, RsyncProtocolException {
        RsyncAuthContext authContext = new RsyncAuthContext(characterEncoder);
        writeString(SessionStatus.AUTHREQ + authContext.getChallenge() + '\n');

        String userResponse = readLine();
        String[] userResponseTuple = userResponse.split(" ", 2);
        if (userResponseTuple.length != 2) {
            throw new RsyncProtocolException("invalid challenge " + "response " + userResponse);
        }

        String userName = userResponseTuple[0];
        String correctResponse = restrictedModule.authenticate(authContext, userName);
        String response = userResponseTuple[1];
        if (response.equals(correctResponse)) {
            return restrictedModule.toModule();
        } else {
            throw new ModuleSecurityException("failed to authenticate " + userName);
        }
    }
}
