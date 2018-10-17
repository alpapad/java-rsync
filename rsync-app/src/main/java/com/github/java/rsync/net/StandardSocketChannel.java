/*
 * Copyright (C) 2014 Per Lundqvist
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
package com.github.java.rsync.net;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.Principal;
import java.util.Optional;

import com.github.java.rsync.internal.util.Environment;

public class StandardSocketChannel implements DuplexByteChannel {
    public static StandardSocketChannel open(String address, int port, int contimeout, int timeout) throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress(address, port);
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.socket().connect(socketAddress, contimeout);
        return new StandardSocketChannel(socketChannel, timeout);
    }
    
    private final InputStream is;
    private final SocketChannel socketChannel;
    
    private final int timeout;
    
    public StandardSocketChannel(SocketChannel socketChannel, int timeout) throws IOException {
        if (timeout > 0) {
            assert Environment.hasAllocateDirectArray() || !Environment.isAllocateDirect();
        }
        this.socketChannel = socketChannel;
        this.timeout = timeout;
        this.socketChannel.socket().setSoTimeout(timeout);
        is = this.socketChannel.socket().getInputStream();
    }
    
    @Override
    public void close() throws IOException {
        socketChannel.close();
    }
    
    @Override
    public InetAddress getPeerAddress() {
        try {
            InetSocketAddress socketAddress = (InetSocketAddress) socketChannel.getRemoteAddress();
            if (socketAddress == null) {
                throw new IllegalStateException(String.format("unable to determine remote address of %s - not connected", socketChannel));
            }
            InetAddress addrOrNull = socketAddress.getAddress();
            if (addrOrNull == null) {
                throw new IllegalStateException(String.format("unable to determine address of %s - unresolved", socketAddress));
            }
            return addrOrNull;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    
    @Override
    public Optional<Principal> getPeerPrincipal() {
        return Optional.empty();
    }
    
    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }
    
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (timeout == 0) {
            return socketChannel.read(dst);
        }
        
        byte[] buf = dst.array();
        int offset = dst.arrayOffset() + dst.position();
        int len = dst.remaining();
        int n = is.read(buf, offset, len);
        if (n != -1) {
            dst.position(dst.position() + n);
        }
        return n;
    }
    
    @Override
    public String toString() {
        return socketChannel.toString();
    }
    
    @Override
    public int write(ByteBuffer src) throws IOException {
        return socketChannel.write(src);
    }
}
