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
package com.github.java.rsync.ui;

import java.io.IOException;

public class Client {
    public static void main(String args[]) throws IOException, InterruptedException {
        {
            new YajsyncClient().start(args);
        }
        int rc = new YajsyncClient().start(new String[] { //
                "--archive", //
                "--delete", //
                "--stats", //
                "--filter=+ *", //
                "/home/alpapad/git/rsync/yajsync-orig/yajsync-app/src/main/java/com/github/perlundq/", //
                "rsync://localhost/test/src/main/java/com/github/perlundq/" //
        });
        System.exit(rc);
        
    }
}