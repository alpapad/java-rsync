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
package com.github.perlundq.yajsync.internal.session;

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

import com.github.perlundq.yajsync.FileSelection;
import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.RsyncSecurityException;
import com.github.perlundq.yajsync.internal.channels.ChannelEOFException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.util.ArgumentParser;
import com.github.perlundq.yajsync.internal.util.ArgumentParsingError;
import com.github.perlundq.yajsync.internal.util.BitOps;
import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.MemoryPolicy;
import com.github.perlundq.yajsync.internal.util.Option;
import com.github.perlundq.yajsync.internal.util.OverflowException;
import com.github.perlundq.yajsync.internal.util.Util;
import com.github.perlundq.yajsync.server.module.Module;
import com.github.perlundq.yajsync.server.module.ModuleException;
import com.github.perlundq.yajsync.server.module.ModuleSecurityException;
import com.github.perlundq.yajsync.server.module.Modules;
import com.github.perlundq.yajsync.server.module.RestrictedModule;
import com.github.perlundq.yajsync.server.module.RsyncAuthContext;

public class ServerSessionConfig extends SessionConfig {
    private static final Logger _log = Logger.getLogger(ServerSessionConfig.class.getName());
    
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
            instance.exchangeProtocolVersion();
            String moduleName = instance.receiveModule();
            
            if (moduleName.isEmpty()) {
                if (_log.isLoggable(Level.FINE)) {
                    _log.fine("sending module listing and exiting");
                }
                instance.sendModuleListing(modules.all());
                instance.sendStatus(SessionStatus.EXIT);
                instance._status = SessionStatus.EXIT; // FIXME: create separate status type instead
                return instance;
            }
            
            Module module = modules.get(moduleName); // throws ModuleException
            if (module instanceof RestrictedModule) {
                RestrictedModule restrictedModule = (RestrictedModule) module;
                module = instance.unlockModule(restrictedModule); // throws ModuleSecurityException
            }
            instance.setModule(module);
            instance.sendStatus(SessionStatus.OK);
            instance._status = SessionStatus.OK;
            
            Collection<String> args = instance.receiveArguments();
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("parsing arguments: " + args);
            }
            instance.parseArguments(args);
            instance.sendCompatibilities();
            instance.sendChecksumSeed();
            return instance;
        } catch (ArgumentParsingError | TextConversionException e) {
            throw new RsyncProtocolException(e);
        } catch (ModuleException e) {
            if (_log.isLoggable(Level.WARNING)) {
                _log.warning(e.getMessage());
            }
            instance.sendErrorStatus(e.getMessage());
            instance._status = SessionStatus.ERROR;
            return instance;
        } finally {
            instance.flush();
        }
    }
    
    private FileSelection _fileSelection = FileSelection.EXACT;
    private boolean _isDelete = false;
    private boolean _isIgnoreTimes = false;
    private boolean _isIncrementalRecurse = false;
    private boolean _isNumericIds = false;
    private boolean _isPreserveDevices = false;
    private boolean _isPreserveGroup = false;
    private boolean _isPreserveLinks = false;
    private boolean _isPreservePermissions = false;
    private boolean _isPreserveSpecials = false;
    private boolean _isPreserveTimes = false;
    private boolean _isPreserveUser = false;
    private boolean _isSafeFileList;
    private boolean _isSender = false;
    private Module _module;
    private Path _receiverDestination;
    private final List<Path> _sourceFiles = new LinkedList<>();
    
    private int _verbosity = 0;
    
    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    private ServerSessionConfig(ReadableByteChannel in, WritableByteChannel out, Charset charset) {
        super(in, out, charset);
        int seedValue = (int) System.currentTimeMillis();
        this._checksumSeed = BitOps.toLittleEndianBuf(seedValue);
    }
    
    public FileSelection fileSelection() {
        return this._fileSelection;
    }
    
    private void flush() throws ChannelException {
        this._peerConnection.flush();
    }
    
    public Path getReceiverDestination() {
        assert this._receiverDestination != null;
        return this._receiverDestination;
    }
    
    public boolean isDelete() {
        return this._isDelete;
    }
    
    public boolean isIgnoreTimes() {
        return this._isIgnoreTimes;
    }
    
    public boolean isNumericIds() {
        return this._isNumericIds;
    }
    
    public boolean isPreserveDevices() {
        return this._isPreserveDevices;
    }
    
    public boolean isPreserveGroup() {
        return this._isPreserveGroup;
    }
    
    public boolean isPreserveLinks() {
        return this._isPreserveLinks;
    }
    
    public boolean isPreservePermissions() {
        return this._isPreservePermissions;
    }
    
    public boolean isPreserveSpecials() {
        return this._isPreserveSpecials;
    }
    
    public boolean isPreserveTimes() {
        return this._isPreserveTimes;
    }
    
    public boolean isPreserveUser() {
        return this._isPreserveUser;
    }
    
    public boolean isSafeFileList() {
        return this._isSafeFileList;
    }
    
    public boolean isSender() {
        return this._isSender;
    }
    
    private void parseArguments(Collection<String> receivedArguments) throws ArgumentParsingError, RsyncProtocolException, RsyncSecurityException {
        ArgumentParser argsParser = ArgumentParser.newWithUnnamed("", "files...");
        // NOTE: has no argument handler
        argsParser.add(Option.newWithoutArgument(Option.Policy.REQUIRED, "server", "", "", null));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "sender", "", "", option -> {
            this._isSender = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "recursive", "r", "", option -> {
            this._fileSelection = FileSelection.RECURSE;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "no-r", "", "", option -> {
            // is sent when transfer dirs and delete
            if (this._fileSelection == FileSelection.RECURSE) {
                this._fileSelection = FileSelection.EXACT;
            }
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newStringOption(Option.Policy.REQUIRED, "rsh", "e", "", option -> {
            try {
                String val = (String) option.getValue();
                this.parsePeerCompatibilites(val);
                return ArgumentParser.Status.CONTINUE;
            } catch (RsyncProtocolException e) {
                throw new ArgumentParsingError(e);
            }
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "ignore-times", "I", "", option -> {
            this._isIgnoreTimes = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "verbose", "v", "", option -> {
            this._verbosity++;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "delete", "", "", option -> {
            this._isDelete = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "", "D", "", option -> {
            this._isPreserveDevices = true;
            this._isPreserveSpecials = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "specials", "", "", option -> {
            this._isPreserveSpecials = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "no-specials", "", "", option -> {
            this._isPreserveSpecials = false;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "links", "l", "", option -> {
            this._isPreserveLinks = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "owner", "o", "", option -> {
            this._isPreserveUser = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "group", "g", "", option -> {
            this._isPreserveGroup = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "numeric-ids", "", "", option -> {
            this._isNumericIds = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "perms", "p", "", option -> {
            this._isPreservePermissions = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "times", "t", "", option -> {
            this._isPreserveTimes = true;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        argsParser.add(Option.newWithoutArgument(Option.Policy.OPTIONAL, "dirs", "d", "", option -> {
            this._fileSelection = FileSelection.TRANSFER_DIRS;
            return ArgumentParser.Status.CONTINUE;
        }));
        
        // FIXME: let ModuleProvider mutate this argsParser instance before
        // calling parse (e.g. adding specific options or removing options)
        
        ArgumentParser.Status rc = argsParser.parse(receivedArguments);
        assert rc == ArgumentParser.Status.CONTINUE;
        assert this._fileSelection != FileSelection.RECURSE || this._isIncrementalRecurse : "We support only incremental recursive transfers for now";
        
        if (!this.isSender() && !this._module.isWritable()) {
            throw new RsyncProtocolException(String.format("Error: module %s is not writable", this._module));
        }
        
        List<String> unnamed = argsParser.getUnnamedArguments();
        if (unnamed.size() < 2) {
            throw new RsyncProtocolException(String.format("Got too few unnamed arguments from peer " + "(%d), expected \".\" and more", unnamed.size()));
        }
        String dotSeparator = unnamed.remove(0);
        if (!dotSeparator.equals(Text.DOT)) {
            throw new RsyncProtocolException(String.format("Expected first non option-argument to be " + "\".\", received \"%s\"", dotSeparator));
        }
        
        if (this.isSender()) {
            Pattern wildcardsPattern = Pattern.compile(".*[\\[*?].*"); // matches literal [, * or ?
            for (String fileName : unnamed) {
                if (wildcardsPattern.matcher(fileName).matches()) {
                    throw new RsyncProtocolException(String.format("wildcards are not supported (%s)", fileName));
                }
                Path safePath = this._module.restrictedPath().resolve(fileName);
                this._sourceFiles.add(safePath);
            }
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("sender source files: " + this._sourceFiles);
            }
        } else {
            if (unnamed.size() != 1) {
                throw new RsyncProtocolException(String.format("Error: expected exactly one file argument: %s contains %d", unnamed, unnamed.size()));
            }
            String fileName = unnamed.get(0);
            Path safePath = this._module.restrictedPath().resolve(fileName);
            this._receiverDestination = safePath.normalize();
            
            if (_log.isLoggable(Level.FINE)) {
                _log.fine("receiver destination: " + this._receiverDestination);
            }
        }
    }
    
    // @throws RsyncProtocolException
    private void parsePeerCompatibilites(String str) throws RsyncProtocolException {
        if (str.startsWith(Text.DOT)) {
            if (str.contains("i")) { // CF_INC_RECURSE
                assert this._fileSelection == FileSelection.RECURSE;
                this._isIncrementalRecurse = true; // only set by client on --recursive or -r, but can also be disabled, we require
                                                   // it however (as a start)
            }
            if (str.contains("L")) { // CF_SYMLINK_TIMES
            }
            if (str.contains("s")) { // CF_SYMLINK_ICONV
            }
            this._isSafeFileList = str.contains("f");
        } else {
            throw new RsyncProtocolException(String.format("Protocol not supported - got %s from peer", str));
        }
    }
    
    private String readStringUntilNullOrEof() throws ChannelException, RsyncProtocolException {
        ByteBuffer buf = ByteBuffer.allocate(64);
        try {
            while (true) {
                byte b = this._peerConnection.getByte();
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
            return this._characterDecoder.decode(buf);
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
            String arg = this.readStringUntilNullOrEof();
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
        return this.readLine();
    }
    
    private void sendChecksumSeed() throws ChannelException {
        assert this._checksumSeed != null;
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("> (checksum seed) " + BitOps.toBigEndianInt(this._checksumSeed));
        }
        this._peerConnection.putInt(BitOps.toBigEndianInt(this._checksumSeed));
    }
    
    private void sendCompatibilities() throws ChannelException {
        byte flags = 0;
        if (this._isSafeFileList) {
            flags |= RsyncCompatibilities.CF_SAFE_FLIST;
        }
        if (this._isIncrementalRecurse) {
            flags |= RsyncCompatibilities.CF_INC_RECURSE;
        }
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("> (we support) " + flags);
        }
        this._peerConnection.putByte(flags);
    }
    
    private void sendErrorStatus(String msg) throws ChannelException {
        this.writeString(String.format("%s: %s\n", SessionStatus.ERROR.toString(), msg));
    }
    
    /**
     * @throws TextConversionException
     */
    private void sendModuleListing(Iterable<Module> modules) throws ChannelException {
        for (Module module : modules) {
            assert !module.name().isEmpty();
            if (module.comment().isEmpty()) {
                this.writeString(String.format("%-15s\n", module.name()));
            } else {
                this.writeString(String.format("%-15s\t%s\n", module.name(), module.comment()));
            }
        }
    }
    
    private void sendStatus(SessionStatus status) throws ChannelException {
        this.writeString(status.toString() + "\n");
    }
    
    private void setModule(Module module) {
        this._module = module;
    }
    
    public List<Path> sourceFiles() {
        return this._sourceFiles;
    }
    
    /**
     * @throws RsyncProtocolException if failing to decode input characters using
     *                                current character set
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of characters
     */
    private Module unlockModule(RestrictedModule restrictedModule) throws ModuleSecurityException, ChannelException, RsyncProtocolException {
        RsyncAuthContext authContext = new RsyncAuthContext(this._characterEncoder);
        this.writeString(SessionStatus.AUTHREQ + authContext.challenge() + '\n');
        
        String userResponse = this.readLine();
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
    
    public int verbosity() {
        return this._verbosity;
    }
}
