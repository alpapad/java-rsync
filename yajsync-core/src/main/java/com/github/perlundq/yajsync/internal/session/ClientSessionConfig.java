/*
 * Rsync client -> server handshaking protocol
 *
 * Copyright (C) 1996-2011 by Andrew Tridgell, Wayne Davison, and others
 * Copyright (C) 2013, 2014 Per Lundqvist
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
import java.io.PrintStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.github.perlundq.yajsync.AuthProvider;
import com.github.perlundq.yajsync.RsyncException;
import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.util.BitOps;
import com.github.perlundq.yajsync.internal.util.Pair;
import com.github.perlundq.yajsync.server.module.RsyncAuthContext;

public class ClientSessionConfig extends SessionConfig {
    private static final Logger _log = Logger.getLogger(ClientSessionConfig.class.getName());
    private final PrintStream _err;
    private final boolean _isRecursive;
    private boolean _isSafeFileList;
    private final BlockingQueue<Pair<Boolean, String>> _listing = new LinkedBlockingQueue<>();
    
    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    public ClientSessionConfig(ReadableByteChannel in, WritableByteChannel out, Charset charset, boolean isRecursive, PrintStream stderr) {
        super(in, out, charset);
        this._isRecursive = isRecursive;
        this._err = stderr;
    }
    
    /**
     * @throws RsyncProtocolException   if peer fails to adhere to the rsync
     *                                  handshake protocol
     * @throws ChannelException         if there is a communication failure with
     *                                  peer
     * @throws IllegalStateException    if failing to encode output characters using
     *                                  current character set
     * @throws IllegalArgumentException if charset is not supported
     */
    public SessionStatus handshake(String moduleName, Iterable<String> args, AuthProvider authProvider) throws RsyncException {
        try {
            this.exchangeProtocolVersion();
            this.sendModule(moduleName);
            this.printLinesAndGetReplyStatus(authProvider);
            if (this._status != SessionStatus.OK) {
                return this._status;
            }
            
            assert !moduleName.isEmpty();
            this.sendArguments(args);
            this.receiveCompatibilities();
            this.receiveChecksumSeed();
            return this._status;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        } finally {
            Pair<Boolean, String> poisonPill = new Pair<>(false, null);
            this._listing.add(poisonPill);
        }
    }
    
    public boolean isSafeFileList() {
        return this._isSafeFileList;
    }
    
    public BlockingQueue<Pair<Boolean, String>> modules() {
        return this._listing;
    }
    
    /**
     * @throws RsyncException         if failing to provide a username and/or
     *                                password
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of characters
     * @throws ChannelException       if there is a communication failure with peer
     */
    private void printLinesAndGetReplyStatus(AuthProvider authProvider) throws RsyncException {
        while (true) {
            String line = this.readLine();
            if (line.equals(SessionStatus.OK.toString())) {
                this._status = SessionStatus.OK;
                return;
            } else if (line.equals(SessionStatus.EXIT.toString())) {
                this._status = SessionStatus.EXIT;
                return;
            } else if (line.startsWith(SessionStatus.ERROR.toString())) {
                this._err.println(line);
                this._status = SessionStatus.ERROR;
                return;
            } else if (line.startsWith(SessionStatus.AUTHREQ.toString())) {
                String challenge = line.substring(SessionStatus.AUTHREQ.toString().length());
                this.sendAuthResponse(authProvider, challenge);
            } else {
                this._listing.add(new Pair<>(true, line));
            }
        }
    }
    
    private void receiveChecksumSeed() throws ChannelException {
        int seedValue = this._peerConnection.getInt();
        this._checksumSeed = BitOps.toLittleEndianBuf(seedValue);
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("< (checksum seed) " + seedValue);
        }
    }
    
    /**
     * @throws ChannelException       if there is a communication failure with peer
     * @throws RsyncProtocolException if peer protocol is incompatible with ours
     */
    private void receiveCompatibilities() throws ChannelException, RsyncProtocolException {
        byte flags = this._peerConnection.getByte();
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("< (peer supports) " + flags);
        }
        if (this._isRecursive && (flags & RsyncCompatibilities.CF_INC_RECURSE) == 0) {
            throw new RsyncProtocolException("peer does not support " + "incremental recurse");
        }
        this._isSafeFileList = (flags & RsyncCompatibilities.CF_SAFE_FLIST) != 0;
    }
    
    /**
     * @throws IllegalStateException if failing to encode output characters using
     *                               current character set
     */
    private void sendArguments(Iterable<String> serverArgs) throws ChannelException {
        for (String arg : serverArgs) {
            this.writeString(arg);
            this._peerConnection.putByte((byte) 0);
        }
        this._peerConnection.putByte((byte) 0);
    }
    
    /**
     * @throws RsyncException   if failing to provide a username and/or password
     * @throws ChannelException if there is a communication failure with peer
     */
    private void sendAuthResponse(AuthProvider authProvider, String challenge) throws RsyncException {
        try {
            String user = authProvider.getUser();
            char[] password = authProvider.getPassword();
            try {
                RsyncAuthContext authContext = RsyncAuthContext.fromChallenge(this._characterEncoder, challenge);
                String response = authContext.response(password);
                this.writeString(String.format("%s %s\n", user, response));
            } finally {
                Arrays.fill(password, (char) 0);
            }
        } catch (IOException | TextConversionException e) {
            throw new RsyncException(e);
        }
    }
    
    /**
     * @throws ChannelException      if there is a communication failure with peer
     * @throws IllegalStateException if failing to encode output characters using
     *                               current character set
     */
    private void sendModule(String moduleName) throws ChannelException {
        this.writeString(moduleName + '\n');
    }
}
