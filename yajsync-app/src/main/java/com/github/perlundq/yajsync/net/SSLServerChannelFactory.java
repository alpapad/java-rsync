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
package com.github.perlundq.yajsync.net;

import java.io.IOException;
import java.net.InetAddress;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

public class SSLServerChannelFactory implements ServerChannelFactory {
    private int _backlog = 128;
    
    private final SSLServerSocketFactory _factory;
    private boolean _isReuseAddress;
    private boolean _isWantClientAuth;
    
    public SSLServerChannelFactory() {
        this._factory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
    }
    
    @Override
    public ServerChannel open(InetAddress address, int port, int timeout) throws IOException {
        SSLServerSocket sock = (SSLServerSocket) this._factory.createServerSocket(port, this._backlog, address);
        try {
            sock.setReuseAddress(this._isReuseAddress);
            sock.setWantClientAuth(this._isWantClientAuth);
            return new SSLServerChannel(sock, timeout);
        } catch (Throwable t) {
            if (!sock.isClosed()) {
                try {
                    sock.close();
                } catch (Throwable tt) {
                    t.addSuppressed(tt);
                }
            }
            throw t;
        }
    }
    
    public ServerChannelFactory setBacklog(int backlog) {
        this._backlog = backlog;
        return this;
    }
    
    @Override
    public ServerChannelFactory setReuseAddress(boolean isReuseAddress) {
        this._isReuseAddress = isReuseAddress;
        return this;
    }
    
    public ServerChannelFactory setWantClientAuth(boolean isWantClientAuth) {
        this._isWantClientAuth = isWantClientAuth;
        return this;
    }
}
