/*
 * Rsync Channel with support for sending tagged rsync messages to
 * peer
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
package com.github.perlundq.yajsync.internal.channels;

import java.nio.channels.WritableByteChannel;

import com.github.perlundq.yajsync.internal.util.Consts;

public class TaggedOutputChannel extends BufferedOutputChannel implements Taggable {
    private static final int DEFAULT_TAG_OFFSET = 0;
    private static final int TAG_SIZE = Consts.SIZE_INT;
    private int _tag_offset;
    
    public TaggedOutputChannel(WritableByteChannel sock) {
        super(sock);
        this.updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
    }
    
    public TaggedOutputChannel(WritableByteChannel sock, int bufferSize) {
        super(sock, bufferSize);
        this.updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
    }
    
    @Override
    public void flush() throws ChannelException {
        if (this.numBytesBuffered() > 0) {
            if (this.numBytesUntagged() > 0) {
                this.tagCurrentData();
            } else {
                // reset buffer position
                assert this._buffer.position() == this._tag_offset + TAG_SIZE;
                this._buffer.position(this._buffer.position() - TAG_SIZE);
            }
            super.flush();
            this.updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
        }
    }
    
    @Override
    public int numBytesBuffered() {
        return this._buffer.position() - TAG_SIZE;
    }
    
    private int numBytesUntagged() {
        int dataStartOffset = this._tag_offset + TAG_SIZE;
        int numBytesUntagged = this._buffer.position() - dataStartOffset;
        assert numBytesUntagged >= 0;
        return numBytesUntagged;
    }
    
    @Override
    public void putMessage(Message message) throws ChannelException {
        assert message.header().length() == message.payload().remaining();
        
        int numBytesRequired = message.header().length() + TAG_SIZE;
        int minMessageSize = TAG_SIZE + 1;
        
        if (numBytesRequired + minMessageSize > this._buffer.remaining()) {
            this.flush();
        } else if (this.numBytesUntagged() > 0) {
            this.tagCurrentData();
            this.updateTagOffsetAndBufPos(this._buffer.position());
        }
        
        this.putMessageHeader(this._tag_offset, message.header());
        assert this._buffer.remaining() >= message.payload().remaining();
        this.put(message.payload());
        this.updateTagOffsetAndBufPos(this._buffer.position());
    }
    
    private void putMessageHeader(int offset, MessageHeader header) {
        this._buffer.putInt(offset, header.toTag());
    }
    
    private void tagCurrentData() {
        this.putMessageHeader(this._tag_offset, new MessageHeader(MessageCode.DATA, this.numBytesUntagged()));
    }
    
    private void updateTagOffsetAndBufPos(int position) {
        assert position >= 0 && position < this._buffer.limit() - TAG_SIZE;
        this._tag_offset = position;
        this._buffer.position(this._tag_offset + TAG_SIZE);
    }
}
