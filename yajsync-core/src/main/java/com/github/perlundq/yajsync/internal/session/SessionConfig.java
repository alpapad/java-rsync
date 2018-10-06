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
package com.github.perlundq.yajsync.internal.session;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.perlundq.yajsync.RsyncProtocolException;
import com.github.perlundq.yajsync.internal.channels.AutoFlushableDuplexChannel;
import com.github.perlundq.yajsync.internal.channels.BufferedOutputChannel;
import com.github.perlundq.yajsync.internal.channels.ChannelException;
import com.github.perlundq.yajsync.internal.channels.SimpleInputChannel;
import com.github.perlundq.yajsync.internal.text.Text;
import com.github.perlundq.yajsync.internal.text.TextConversionException;
import com.github.perlundq.yajsync.internal.text.TextDecoder;
import com.github.perlundq.yajsync.internal.text.TextEncoder;
import com.github.perlundq.yajsync.internal.util.Consts;
import com.github.perlundq.yajsync.internal.util.MemoryPolicy;
import com.github.perlundq.yajsync.internal.util.OverflowException;
import com.github.perlundq.yajsync.internal.util.Util;

public abstract class SessionConfig {
    private static final Logger _log = Logger.getLogger(SessionConfig.class.getName());
    private static final Pattern PROTOCOL_VERSION_REGEX = Pattern.compile("@RSYNCD: (\\d+)\\.(\\d+)$");
    private static final ProtocolVersion VERSION = new ProtocolVersion(30, 0);
    
    protected TextDecoder _characterDecoder;
    protected TextEncoder _characterEncoder;
    private Charset _charset;
    protected byte[] _checksumSeed; // always stored in little endian
    protected final AutoFlushableDuplexChannel _peerConnection;
    
    protected SessionStatus _status;
    
    /**
     * @throws IllegalArgumentException if charset is not supported
     */
    protected SessionConfig(ReadableByteChannel in, WritableByteChannel out, Charset charset) {
        this._peerConnection = new AutoFlushableDuplexChannel(new SimpleInputChannel(in), new BufferedOutputChannel(out));
        this.setCharset(charset);
    }
    
    public Charset charset() {
        assert this._charset != null;
        return this._charset;
    }
    
    public byte[] checksumSeed() {
        assert this._checksumSeed != null;
        return this._checksumSeed;
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
    protected void exchangeProtocolVersion() throws ChannelException, RsyncProtocolException {
        this.sendVersion(VERSION);
        ProtocolVersion peerVersion = this.receivePeerVersion();
        if (peerVersion.compareTo(VERSION) < 0) {
            throw new RsyncProtocolException(String.format("Error: peer version is less than our version (%s < %s)", peerVersion, VERSION));
        }
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
            byte lastByte = this._peerConnection.getByte();
            switch (lastByte) {
                case Text.ASCII_CR:
                    break;
                case Text.ASCII_NEWLINE:
                    buf.flip();
                    try {
                        result = this._characterDecoder.decode(buf);
                    } catch (TextConversionException e) {
                        throw new RsyncProtocolException(e);
                    }
                    break;
                case Text.ASCII_NULL:
                    throw new RsyncProtocolException(String.format("got a null-terminated input string without a newline: " + "\"%s\"", this._characterDecoder.decode(buf)));
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
        
        if (_log.isLoggable(Level.FINER)) {
            _log.finer("< " + result);
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
        String versionResponse = this.readLine();
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
        this.writeString(String.format("@RSYNCD: %d.%d\n", version.major(), version.minor()));
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
        this._charset = charset;
        this._characterEncoder = TextEncoder.newStrict(this._charset);
        this._characterDecoder = TextDecoder.newStrict(this._charset);
    }
    
    public SessionStatus status() {
        assert this._status != null;
        return this._status;
    }
    
    /**
     * @throws IllegalStateException if failing to encode output characters using
     *                               current character set
     */
    protected void writeString(String text) throws ChannelException {
        try {
            if (_log.isLoggable(Level.FINER)) {
                _log.finer("> " + text);
            }
            byte[] textEncoded = this._characterEncoder.encode(text);
            this._peerConnection.put(textEncoded, 0, textEncoded.length);
        } catch (TextConversionException e) {
            throw new IllegalStateException(e);
        }
    }
}
