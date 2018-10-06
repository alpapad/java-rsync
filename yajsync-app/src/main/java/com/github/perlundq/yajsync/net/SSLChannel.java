/*
 * SSL/TLS implementation of ReadableByteChannel and WritableByteChannel
 *
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
package com.github.perlundq.yajsync.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.Principal;
import java.util.Optional;

import javax.net.SocketFactory;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.github.perlundq.yajsync.internal.util.Environment;

public class SSLChannel implements DuplexByteChannel {
    public static SSLChannel open(String address, int port, int contimeout, int timeout) throws IOException {
        SocketFactory factory = SSLSocketFactory.getDefault();
        InetSocketAddress socketAddress = new InetSocketAddress(address, port);
        Socket sock = factory.createSocket();
        sock.connect(socketAddress, contimeout);
        return new SSLChannel((SSLSocket) sock, timeout);
    }
    
    private final InputStream _is;
    private final OutputStream _os;
    
    private final SSLSocket _sslSocket;
    
    public SSLChannel(SSLSocket sslSocket, int timeout) throws IOException {
        assert Environment.hasAllocateDirectArray() || !Environment.isAllocateDirect();
        this._sslSocket = sslSocket;
        this._sslSocket.setSoTimeout(timeout);
        this._is = this._sslSocket.getInputStream();
        this._os = this._sslSocket.getOutputStream();
    }
    
    @Override
    public void close() throws IOException {
        this._sslSocket.close(); // will implicitly close _is and _os also
    }
    
    @Override
    public boolean isOpen() {
        return !this._sslSocket.isClosed();
    }
    
    @Override
    public InetAddress peerAddress() {
        InetAddress address = this._sslSocket.getInetAddress();
        if (address == null) {
            throw new IllegalStateException(String.format("unable to determine remote address of %s - not connected", this._sslSocket));
        }
        return address;
    }
    
    @Override
    public Optional<Principal> peerPrincipal() {
        try {
            return Optional.of(this._sslSocket.getSession().getPeerPrincipal());
        } catch (SSLPeerUnverifiedException e) {
            return Optional.empty();
        }
    }
    
    @Override
    public int read(ByteBuffer dst) throws IOException {
        byte[] buf = dst.array();
        int offset = dst.arrayOffset() + dst.position();
        int len = dst.remaining();
        int n = this._is.read(buf, offset, len);
        if (n != -1) {
            dst.position(dst.position() + n);
        }
        return n;
    }
    
    @Override
    public String toString() {
        return this._sslSocket.toString();
    }
    
    @Override
    public int write(ByteBuffer src) throws IOException {
        byte[] buf = src.array();
        int offset = src.arrayOffset() + src.position();
        int len = src.remaining();
        this._os.write(buf, offset, len);
        src.position(src.position() + len);
        return len;
    }
}
