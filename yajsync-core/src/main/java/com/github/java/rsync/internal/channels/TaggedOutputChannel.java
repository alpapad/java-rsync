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
package com.github.java.rsync.internal.channels;

import java.nio.channels.WritableByteChannel;

import com.github.java.rsync.internal.util.Consts;

public class TaggedOutputChannel extends BufferedOutputChannel implements Taggable {
    private static final int DEFAULT_TAG_OFFSET = 0;
    private static final int TAG_SIZE = Consts.SIZE_INT;
    private int tagOffset;
    
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
        if (this.getNumBytesBuffered() > 0) {
            if (this.getNumBytesUntagged() > 0) {
                this.tagCurrentData();
            } else {
                // reset buffer position
                assert this.buffer.position() == this.tagOffset + TAG_SIZE;
                this.buffer.position(this.buffer.position() - TAG_SIZE);
            }
            super.flush();
            this.updateTagOffsetAndBufPos(DEFAULT_TAG_OFFSET);
        }
    }
    
    @Override
    public int getNumBytesBuffered() {
        return this.buffer.position() - TAG_SIZE;
    }
    
    private int getNumBytesUntagged() {
        int dataStartOffset = this.tagOffset + TAG_SIZE;
        int numBytesUntagged = this.buffer.position() - dataStartOffset;
        assert numBytesUntagged >= 0;
        return numBytesUntagged;
    }
    
    @Override
    public void putMessage(Message message) throws ChannelException {
        assert message.getHeader().getLength() == message.getPayload().remaining();
        
        int numBytesRequired = message.getHeader().getLength() + TAG_SIZE;
        int minMessageSize = TAG_SIZE + 1;
        
        if (numBytesRequired + minMessageSize > this.buffer.remaining()) {
            this.flush();
        } else if (this.getNumBytesUntagged() > 0) {
            this.tagCurrentData();
            this.updateTagOffsetAndBufPos(this.buffer.position());
        }
        
        this.putMessageHeader(this.tagOffset, message.getHeader());
        assert this.buffer.remaining() >= message.getPayload().remaining();
        this.put(message.getPayload());
        this.updateTagOffsetAndBufPos(this.buffer.position());
    }
    
    private void putMessageHeader(int offset, MessageHeader header) {
        this.buffer.putInt(offset, header.toTag());
    }
    
    private void tagCurrentData() {
        this.putMessageHeader(this.tagOffset, new MessageHeader(MessageCode.DATA, this.getNumBytesUntagged()));
    }
    
    private void updateTagOffsetAndBufPos(int position) {
        assert position >= 0 && position < this.buffer.limit() - TAG_SIZE;
        this.tagOffset = position;
        this.buffer.position(this.tagOffset + TAG_SIZE);
    }
}
