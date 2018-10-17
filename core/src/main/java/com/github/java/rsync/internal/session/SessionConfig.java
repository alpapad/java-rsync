/*
 * General rsync client <-> server protocol routines
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

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.java.rsync.RsyncProtocolException;
import com.github.java.rsync.internal.channels.AutoFlushableDuplexChannel;
import com.github.java.rsync.internal.channels.BufferedOutputChannel;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.channels.SimpleInputChannel;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.text.TextConversionException;
import com.github.java.rsync.internal.text.TextDecoder;
import com.github.java.rsync.internal.text.TextEncoder;
import com.github.java.rsync.internal.util.Consts;
import com.github.java.rsync.internal.util.MemoryPolicy;
import com.github.java.rsync.internal.util.OverflowException;
import com.github.java.rsync.internal.util.Util;

public abstract class SessionConfig {
    private static final Logger LOG = Logger.getLogger(SessionConfig.class.getName());
    private static final Pattern PROTOCOL_VERSION_REGEX = Pattern.compile("@RSYNCD: (\\d+)\\.(\\d+)$");
    private static final ProtocolVersion VERSION = new ProtocolVersion(30, 0);

    protected TextDecoder characterDecoder;
    protected TextEncoder characterEncoder;
    private Charset charset;
    protected byte[] checksumSeed; // always stored in little endian
    protected final AutoFlushableDuplexChannel peerConnection;

    protected SessionStatus status;

    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    protected SessionConfig(ReadableByteChannel in, WritableByteChannel out, Charset charset) {
        peerConnection = new AutoFlushableDuplexChannel(new SimpleInputChannel(in), new BufferedOutputChannel(out));
        setCharset(charset);
    }

    public Charset getCharset() {
        assert charset != null;
        return charset;
    }

    public byte[] getChecksumSeed() {
        assert checksumSeed != null;
        return checksumSeed;
    }

    /**
     * @throws RsyncProtocolException if peer sent a version less than ours
     * @throws RsyncProtocolException if protocol version is invalid
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of characters
     * @throws ChannelException       if there is a communication failure with peer
     * @throws IllegalStateException  if failing to encode output characters using
     *                                current character set
     */
    protected void getExchangeProtocolVersion() throws ChannelException, RsyncProtocolException {
        sendVersion(VERSION);
        ProtocolVersion peerVersion = receivePeerVersion();
        if (peerVersion.compareTo(VERSION) < 0) {
            throw new RsyncProtocolException(String.format("Error: peer version is less than our version (%s < %s)", peerVersion, VERSION));
        }
    }

    public SessionStatus getStatus() {
        assert status != null;
        return status;
    }

    /**
     * @throws RsyncProtocolException if failing to decode input characters from
     *                                peer using current character set
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if peer sent too large amount of characters
     * @throws ChannelException       if there is a communication failure with peer
     */
    protected String readLine() throws ChannelException, RsyncProtocolException {
        ByteBuffer buf = ByteBuffer.allocate(64);
        String result = null;
        while (result == null) {
            byte lastByte = peerConnection.getByte();
            switch (lastByte) {
                case Text.ASCII_CR:
                    break;
                case Text.ASCII_NEWLINE:
                    buf.flip();
                    try {
                        result = characterDecoder.decode(buf);
                    } catch (TextConversionException e) {
                        throw new RsyncProtocolException(e);
                    }
                    break;
                case Text.ASCII_NULL:
                    throw new RsyncProtocolException(String.format("got a null-terminated input string without a newline: " + "\"%s\"", characterDecoder.decode(buf)));
                default:
                    if (!buf.hasRemaining()) {
                        try {
                            buf = Util.enlargeByteBuffer(buf, MemoryPolicy.IGNORE, Consts.MAX_BUF_SIZE);
                        } catch (OverflowException e) {
                            throw new RsyncProtocolException(e);
                        }
                    }
                    buf.put(lastByte);
            }
        }

        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("< " + result);
        }
        return result;
    }

    /**
     * @throws RsyncProtocolException if protocol version is invalid
     * @throws RsyncProtocolException if failing to decode input characters using
     *                                current character set
     * @throws RsyncProtocolException if peer sent premature null character
     * @throws RsyncProtocolException if received too large amount of characters
     * @throws ChannelException       if there is a communication failure with peer
     */
    private ProtocolVersion receivePeerVersion() throws ChannelException, RsyncProtocolException {
        String versionResponse = readLine();
        Matcher m = PROTOCOL_VERSION_REGEX.matcher(versionResponse);
        if (m.matches()) {
            return new ProtocolVersion(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
        } else {
            throw new RsyncProtocolException(String.format("Invalid protocol version: %s", versionResponse));
        }
    }

    /**
     * @throws IllegalStateException if failing to encode output characters using
     *                               current character set
     * @throws ChannelException      if there is a communication failure with peer
     */
    private void sendVersion(ProtocolVersion version) throws ChannelException {
        writeString(String.format("@RSYNCD: %d.%d\n", version.getMajor(), version.getMinor()));
    }

    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    private void setCharset(Charset charset) {
        assert charset != null;
        if (!Util.isValidCharset(charset)) {
            throw new IllegalArgumentException(String.format(
                    "character set %s is not supported - cannot encode SLASH (/)," + " DOT (.), NEWLINE (\n), CARRIAGE RETURN (\r) and NULL (\0) " + "to their ASCII counterparts and vice versa",
                    charset));
        }
        this.charset = charset;
        characterEncoder = TextEncoder.newStrict(this.charset);
        characterDecoder = TextDecoder.newStrict(this.charset);
    }

    /**
     * @throws IllegalStateException if failing to encode output characters using
     *                               current character set
     */
    protected void writeString(String text) throws ChannelException {
        try {
            if (LOG.isLoggable(Level.FINER)) {
                LOG.finer("> " + text);
            }
            byte[] textEncoded = characterEncoder.encode(text);
            peerConnection.put(textEncoded, 0, textEncoded.length);
        } catch (TextConversionException e) {
            throw new IllegalStateException(e);
        }
    }
}
