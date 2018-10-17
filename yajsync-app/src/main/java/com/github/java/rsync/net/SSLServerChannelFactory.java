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
import java.net.InetAddress;

import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;

public class SSLServerChannelFactory implements ServerChannelFactory {
    private int backlog = 128;
    
    private final SSLServerSocketFactory factory;
    private boolean reuseAddress;
    private boolean wantClientAuth;
    
    public SSLServerChannelFactory() {
        factory = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
    }
    
    @Override
    public ServerChannel open(InetAddress address, int port, int timeout) throws IOException {
        SSLServerSocket sock = (SSLServerSocket) factory.createServerSocket(port, backlog, address);
        try {
            sock.setReuseAddress(reuseAddress);
            sock.setWantClientAuth(wantClientAuth);
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
        this.backlog = backlog;
        return this;
    }
    
    @Override
    public ServerChannelFactory setReuseAddress(boolean isReuseAddress) {
        reuseAddress = isReuseAddress;
        return this;
    }
    
    public ServerChannelFactory setWantClientAuth(boolean isWantClientAuth) {
        wantClientAuth = isWantClientAuth;
        return this;
    }
}
