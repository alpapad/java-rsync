/*
 * Copyright (C) 2013-2015 Per Lundqvist
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
package com.github.java.rsync.ui;

import java.util.Objects;

import com.github.java.rsync.RsyncServer;
import com.github.java.rsync.internal.util.Environment;

final class ConnectionInfo {
    public static class Builder {
        private final String address;
        private int portNumber = RsyncServer.DEFAULT_LISTEN_PORT;
        private String userName = Environment.getUserName();
        
        public Builder(String address) throws IllegalUrlException {
            assert address != null;
            if (address.isEmpty()) {
                throw new IllegalUrlException("address is empty");
            }
            this.address = address;
        }
        
        public ConnectionInfo build() {
            return new ConnectionInfo(this);
        }
        
        public Builder portNumber(int portNumber) throws IllegalUrlException {
            if (!isValidPortNumber(portNumber)) {
                throw new IllegalUrlException(String.format("illegal port %d - must be within the range [%d, %d]", portNumber, PORT_MIN, PORT_MAX));
            }
            this.portNumber = portNumber;
            return this;
        }
        
        public Builder userName(String userName) {
            assert userName != null;
            this.userName = userName;
            return this;
        }
    }
    
    public static final int PORT_MAX = 65535;
    public static final int PORT_MIN = 1;
    
    public static boolean isValidPortNumber(int portNumber) {
        return portNumber >= PORT_MIN && portNumber <= PORT_MAX;
    }
    
    private final String address;
    
    private final int portNumber;
    
    private final String userName;
    
    private ConnectionInfo(Builder builder) {
        userName = builder.userName;
        address = builder.address;
        portNumber = builder.portNumber;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj != null && this.getClass() == obj.getClass()) {
            ConnectionInfo other = (ConnectionInfo) obj;
            return userName.equals(other.userName) && address.equals(other.address) && portNumber == other.portNumber;
        }
        return false;
    }
    
    public String getAddress() {
        return address;
    }
    
    public int getPortNumber() {
        return portNumber;
    }
    
    public String getUserName() {
        return userName;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(address, userName, portNumber);
    }
    
    @Override
    public String toString() {
        return String.format("rsync://%s%s:%d", userName.isEmpty() ? "" : userName + "@", address, portNumber);
    }
}
