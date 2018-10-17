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
package com.github.java.rsync.internal.session;

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

import com.github.java.rsync.AuthProvider;
import com.github.java.rsync.RsyncException;
import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.text.TextConversionException;
import com.github.java.rsync.internal.util.BitOps;
import com.github.java.rsync.internal.util.Pair;
import com.github.java.rsync.server.module.RsyncAuthContext;

public class ClientSessionConfig extends SessionConfig {
    private static final Logger LOG = Logger.getLogger(ClientSessionConfig.class.getName());
    private final PrintStream err;
    private final BlockingQueue<Pair<Boolean, String>> listing = new LinkedBlockingQueue<>();
    private final boolean recursive;
    private boolean safeFileList;

    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    public ClientSessionConfig(ReadableByteChannel in, WritableByteChannel out, Charset charset, boolean isRecursive, PrintStream stderr) {
        super(in, out, charset);
        recursive = isRecursive;
        err = stderr;
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
            getExchangeProtocolVersion();
            sendModule(moduleName);
            printLinesAndGetReplyStatus(authProvider);
            if (status != SessionStatus.OK) {
                return status;
            }

            assert !moduleName.isEmpty();
            sendArguments(args);
            receiveCompatibilities();
            receiveChecksumSeed();
            return status;
        } catch (TextConversionException e) {
            throw new RsyncProtocolException(e);
        } finally {
            Pair<Boolean, String> poisonPill = new Pair<>(false, null);
            listing.add(poisonPill);
        }
    }

    public boolean isSafeFileList() {
        return safeFileList;
    }

    public BlockingQueue<Pair<Boolean, String>> modules() {
        return listing;
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
            String line = readLine();
            if (line.equals(SessionStatus.OK.toString())) {
                status = SessionStatus.OK;
                return;
            } else if (line.equals(SessionStatus.EXIT.toString())) {
                status = SessionStatus.EXIT;
                return;
            } else if (line.startsWith(SessionStatus.ERROR.toString())) {
                err.println(line);
                status = SessionStatus.ERROR;
                return;
            } else if (line.startsWith(SessionStatus.AUTHREQ.toString())) {
                String challenge = line.substring(SessionStatus.AUTHREQ.toString().length());
                sendAuthResponse(authProvider, challenge);
            } else {
                listing.add(new Pair<>(true, line));
            }
        }
    }

    private void receiveChecksumSeed() throws ChannelException {
        int seedValue = peerConnection.getInt();
        checksumSeed = BitOps.toLittleEndianBuf(seedValue);
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("< (checksum seed) " + seedValue);
        }
    }

    /**
     * @throws ChannelException       if there is a communication failure with peer
     * @throws RsyncProtocolException if peer protocol is incompatible with ours
     */
    private void receiveCompatibilities() throws ChannelException, RsyncProtocolException {
        byte flags = peerConnection.getByte();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("< (peer supports) " + flags);
        }
        if (recursive && (flags & RsyncCompatibilities.CF_INC_RECURSE) == 0) {
            throw new RsyncProtocolException("peer does not support " + "incremental recurse");
        }
        safeFileList = (flags & RsyncCompatibilities.CF_SAFE_FLIST) != 0;
    }

    /**
     * @throws IllegalStateException if failing to encode output characters using
     *                               current character set
     */
    private void sendArguments(Iterable<String> serverArgs) throws ChannelException {
        for (String arg : serverArgs) {
            writeString(arg);
            peerConnection.putByte((byte) 0);
        }
        peerConnection.putByte((byte) 0);
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
                RsyncAuthContext authContext = RsyncAuthContext.fromChallenge(characterEncoder, challenge);
                String response = authContext.response(password);
                writeString(String.format("%s %s\n", user, response));
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
        writeString(moduleName + '\n');
    }
}
