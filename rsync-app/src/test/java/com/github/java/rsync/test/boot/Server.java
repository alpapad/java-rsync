/*
 * A rsync client/server command line implementation
 *
 * Copyright (C) 2016 Per Lundqvist
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
package com.github.java.rsync.test.boot;

import java.io.IOException;
import java.net.URISyntaxException;

import com.github.java.rsync.ui.YajsyncServer;

public class Server {
    public static void main(String args[]) throws IOException, InterruptedException, URISyntaxException {
        YajsyncServer server = new YajsyncServer();
        server.setModuleProvider(new DummyModuleProvider("/tmp/haha/a.jar"));
        int rc = server.start(new String[] { /*"-v", "-v",*/ "--defer-write" });
        System.exit(rc);
    }
}
