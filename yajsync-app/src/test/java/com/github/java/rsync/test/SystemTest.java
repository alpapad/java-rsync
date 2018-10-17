/*
 * Copyright (C) 2014-2016 Per Lundqvist
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
package com.github.java.rsync.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.github.java.rsync.RsyncClient;
import com.github.java.rsync.Statistics;
import com.github.java.rsync.attr.Group;
import com.github.java.rsync.attr.RsyncFileAttributes;
import com.github.java.rsync.attr.User;
import com.github.java.rsync.internal.channels.ChannelException;
import com.github.java.rsync.internal.session.FileAttributeManager;
import com.github.java.rsync.internal.session.UnixFileAttributeManager;
import com.github.java.rsync.internal.text.Text;
import com.github.java.rsync.internal.util.Environment;
import com.github.java.rsync.internal.util.FileOps;
import com.github.java.rsync.internal.util.Option;
import com.github.java.rsync.net.DuplexByteChannel;
import com.github.java.rsync.net.SSLChannelFactory;
import com.github.java.rsync.net.StandardChannelFactory;
import com.github.java.rsync.server.module.Module;
import com.github.java.rsync.server.module.ModuleException;
import com.github.java.rsync.server.module.ModuleProvider;
import com.github.java.rsync.server.module.Modules;
import com.github.java.rsync.server.module.RestrictedModule;
import com.github.java.rsync.server.module.RestrictedPath;
import com.github.java.rsync.server.module.RsyncAuthContext;
import com.github.java.rsync.ui.YajsyncClient;
import com.github.java.rsync.ui.YajsyncServer;

class FileUtil {
    public static final FileAttributeManager fileManager;
    
    static {
        try {
            fileManager = new UnixFileAttributeManager(User.JVM_USER, Group.JVM_GROUP, true, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static long du(Path... srcFiles) throws IOException {
        long size = 0;
        for (Path p : srcFiles) {
            size += Files.size(p);
        }
        return size;
    }
    
    public static boolean exists(Path path) {
        return Files.exists(path, LinkOption.NOFOLLOW_LINKS);
    }
    
    public static byte[] generateBytes(int content, int num) {
        byte[] res = new byte[num];
        for (int i = 0; i < num; i++) {
            res[i] = (byte) content;
        }
        return res;
    }
    
    public static boolean isContentIdentical(Path leftPath, Path rightPath) throws IOException {
        try (InputStream left_is = Files.newInputStream(leftPath); InputStream right_is = Files.newInputStream(rightPath)) {
            while (true) {
                int left_byte = left_is.read();
                int right_byte = right_is.read();
                if (left_byte != right_byte) {
                    return false;
                }
                boolean isEOF = left_byte == -1; // && right_byte == -1;
                if (isEOF) {
                    return true;
                }
            }
        }
    }
    
    public static boolean isDirectoriesIdentical(Path leftDir, Path rightDir) throws IOException {
        SortedMap<Path, Path> leftFiles = FileUtil.listDir(leftDir);
        SortedMap<Path, Path> rightFiles = FileUtil.listDir(rightDir);
        
        if (!leftFiles.keySet().equals(rightFiles.keySet())) {
            return false;
        }
        
        for (Map.Entry<Path, Path> entrySet : leftFiles.entrySet()) {
            Path name = entrySet.getKey();
            Path leftPath = entrySet.getValue();
            Path rightPath = rightFiles.get(name);
            
            RsyncFileAttributes leftAttrs = fileManager.stat(leftPath);
            RsyncFileAttributes rightAttrs = fileManager.stat(rightPath);
            if (!FileUtil.isFileSameTypeAndSize(leftAttrs, rightAttrs)) {
                return false;
            } else if (leftAttrs.isRegularFile()) {
                boolean isIdentical = FileUtil.isContentIdentical(leftPath, rightPath);
                if (!isIdentical) {
                    return false;
                }
            } else if (leftAttrs.isDirectory()) {
                boolean isIdentical = FileUtil.isDirectoriesIdentical(leftPath, rightPath);
                if (!isIdentical) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public static boolean isDirectory(Path path) {
        return Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS);
    }
    
    public static boolean isFile(Path path) {
        return Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
    }
    
    public static boolean isFileSameGroup(RsyncFileAttributes leftAttrs, RsyncFileAttributes rightAttrs) {
        return leftAttrs.getGroup().equals(rightAttrs.getGroup());
    }
    
    public static boolean isFileSameOwner(RsyncFileAttributes leftAttrs, RsyncFileAttributes rightAttrs) {
        return leftAttrs.getUser().equals(rightAttrs.getUser());
    }
    
    private static boolean isFileSameTypeAndSize(RsyncFileAttributes leftAttrs, RsyncFileAttributes rightAttrs) {
        int leftType = FileOps.fileType(leftAttrs.getMode());
        int rightType = FileOps.fileType(rightAttrs.getMode());
        return leftType == rightType && (!FileOps.isRegularFile(leftType) || leftAttrs.getSize() == rightAttrs.getSize());
    }
    
    private static SortedMap<Path, Path> listDir(Path path) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
            SortedMap<Path, Path> files = new TreeMap<>();
            for (Path p : stream) {
                files.put(p.getFileName(), p);
            }
            return files;
        }
    }
    
    public static void writeToFiles(byte[] content, Path... path) throws IOException {
        for (Path p : path) {
            try (FileOutputStream out = new FileOutputStream(p.toFile())) {
                out.write(content);
            }
        }
    }
    
    public static void writeToFiles(int content, Path... path) throws IOException {
        for (Path p : path) {
            try (FileOutputStream out = new FileOutputStream(p.toFile())) {
                out.write(content);
            }
        }
    }
}

class SimpleModule implements Module {
    private final String comment;
    private final boolean readable;
    private final boolean writable;
    private final String name;
    private final RestrictedPath path;
    
    SimpleModule(String name, Path root, String comment, boolean isReadable, boolean isWritable) {
        this.name = name.toString();
        this.path = new RestrictedPath(name, root);
        this.comment = comment;
        this.readable = isReadable;
        this.writable = isWritable;
    }
    
    @Override
    public String getComment() {
        return this.comment;
    }
    
    @Override
    public String getName() {
        return this.name;
    }
    
    @Override
    public RestrictedPath getRestrictedPath() {
        return this.path;
    }
    
    @Override
    public boolean isReadable() {
        return this.readable;
    }
    
    @Override
    public boolean isWritable() {
        return this.writable;
    }
}

class SimpleRestrictedModule extends RestrictedModule {
    private final String authToken;
    private final String comment;
    private final Module module;
    private final String name;
    
    public SimpleRestrictedModule(String authToken, Module module, String name, String comment) {
        this.authToken = authToken;
        this.module = module;
        this.name = name;
        this.comment = comment;
    }
    
    @Override
    public String authenticate(RsyncAuthContext authContext, String userName) {
        return authContext.response(this.authToken.toCharArray());
    }
    
    @Override
    public String getComment() {
        return this.comment;
    }
    
    @Override
    public String getName() {
        return this.name;
    }
    
    @Override
    public Module toModule() {
        return this.module;
    }
}

public class SystemTest {
    private class ReadTimeoutTestServer extends Thread {
        
        private final CountDownLatch listeningLatch;
        private final int port;
        
        public ReadTimeoutTestServer(CountDownLatch isListeningLatch, int port) {
            this.listeningLatch = isListeningLatch;
            this.port = port;
        }
        
        @Override
        public void run() {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(this.port);
                
                this.listeningLatch.countDown();
                
                while (true) {
                    serverSocket.accept();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
    }
    
    private static class ReturnStatus {
        final int rc;
        final Statistics stats;
        
        ReturnStatus(int rc_, Statistics stats_) {
            this.rc = rc_;
            this.stats = stats_;
        }
    }
    
    private final PrintStream nullOut = new PrintStream(new OutputStream() {
        @Override
        public void write(int b) {
            /* nop */}
    });
    
    private ExecutorService service;
    
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();
    
    private ReturnStatus fileCopy(Path src, Path dst, String... args) {
        YajsyncClient client = this.newClient();
        String[] nargs = new String[args.length + 2];
        int i = 0;
        for (String arg : args) {
            nargs[i++] = arg;
        }
        nargs[i++] = src.toString();
        nargs[i++] = dst.toString();
        int rc = client.start(nargs);
        return new ReturnStatus(rc, client.statistics());
    }
    
    private ReturnStatus listFiles(Path src, String... args) {
        YajsyncClient client = this.newClient();
        String[] nargs = new String[args.length + 1];
        int i = 0;
        for (String arg : args) {
            nargs[i++] = arg;
        }
        nargs[i++] = src.toString();
        int rc = client.start(nargs);
        return new ReturnStatus(rc, client.statistics());
    }
    
    private YajsyncClient newClient() {
        return new YajsyncClient().setStandardOut(System.err).setStandardErr(System.err);
    }
    
    private YajsyncServer newServer(Modules modules) {
        YajsyncServer server = new YajsyncServer().setStandardOut(System.err).setStandardErr(System.err);
        server.setModuleProvider(new TestModuleProvider(modules));
        return server;
    }
    
    private ReturnStatus recursiveCopyTrailingSlash(Path src, Path dst) {
        YajsyncClient client = this.newClient();
        int rc = client.start(new String[] { "--recursive", src.toString() + "/", dst.toString() });
        return new ReturnStatus(rc, client.statistics());
    }
    
    @Before
    public void setup() {
        this.service = Executors.newCachedThreadPool();
    }
    
    @After
    public void teardown() {
        this.service.shutdownNow();
    }
    
    @Test
    public void testClientCopyPreserveGid() throws IOException {
        if (!User.JVM_USER.equals(User.ROOT)) {
            return;
        }
        
        Path src = this.tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");
        
        Path srcDir = src.resolve("dir");
        Path srcFile = srcDir.resolve("file");
        Files.createDirectory(srcDir);
        FileUtil.writeToFiles(1, srcFile);
        FileUtil.fileManager.setGroupId(srcFile, User.NOBODY.getId());
        
        Files.createDirectory(dst);
        Path copyOfSrc = dst.resolve(src.getFileName());
        Files.createDirectory(copyOfSrc);
        Path dstDir = copyOfSrc.resolve("dir");
        Path dstFile = dstDir.resolve("file");
        Files.createDirectory(dstDir);
        FileUtil.writeToFiles(1, dstFile);
        
        ReturnStatus status = this.fileCopy(src, dst, "--recursive", "--group", "--numeric-ids");
        
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, copyOfSrc));
        assertTrue(FileUtil.isFileSameGroup(FileUtil.fileManager.stat(srcFile), FileUtil.fileManager.stat(dstFile)));
    }
    
    @Test
    public void testClientCopyPreserveUid() throws IOException {
        if (!User.JVM_USER.equals(User.ROOT)) {
            return;
        }
        
        Path src = this.tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");
        
        Path srcDir = src.resolve("dir");
        Path srcFile = srcDir.resolve("file");
        Files.createDirectory(srcDir);
        FileUtil.writeToFiles(1, srcFile);
        FileUtil.fileManager.setUserId(srcFile, User.NOBODY.getId());
        
        Files.createDirectory(dst);
        Path copyOfSrc = dst.resolve(src.getFileName());
        Files.createDirectory(copyOfSrc);
        Path dstDir = copyOfSrc.resolve("dir");
        Path dstFile = dstDir.resolve("file");
        Files.createDirectory(dstDir);
        FileUtil.writeToFiles(1, dstFile);
        
        ReturnStatus status = this.fileCopy(src, dst, "--recursive", "--owner", "--numeric-ids");
        
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, copyOfSrc));
        assertTrue(FileUtil.isFileSameOwner(FileUtil.fileManager.stat(srcFile), FileUtil.fileManager.stat(dstFile)));
    }
    
    @Test
    public void testClientDotDotSrcArg() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path srcDotDot = this.tempDir.newFolder().toPath().resolve(Paths.get(Text.DOT_DOT)).resolve(src.getFileName());
        Path dst = this.tempDir.newFile().toPath();
        FileUtil.writeToFiles(0, src);
        int numFiles = 1;
        long fileSize = Files.size(src);
        ReturnStatus status = this.fileCopy(srcDotDot, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testClientEmptyDirCopy() throws IOException {
        Path src = this.tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");
        Path copyOfSrc = dst.resolve(src.getFileName());
        int numDirs = 1;
        int numFiles = 0;
        long fileSize = 0;
        ReturnStatus status = this.fileCopy(src, dst, "--recursive");
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, copyOfSrc));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testClientEmptyDirTrailingSlashCopy() throws IOException {
        Path src = this.tempDir.newFolder().toPath();
        Path dst = Paths.get(src.toString() + ".dst");
        int numDirs = 1;
        int numFiles = 0;
        long fileSize = 0;
        ReturnStatus status = this.recursiveCopyTrailingSlash(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectory(dst));
        assertTrue(FileUtil.isDirectoriesIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testClientHelp() {
        int rc = this.newClient().start(new String[] { "--help" });
        assertTrue(rc == 0);
    }
    
    @Test
    public void testClientNoArgs() {
        int rc = this.newClient().start(new String[] {});
        assertTrue(rc == -1);
    }
    
    @Test
    public void testClientSingleFileCopy() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = this.tempDir.newFile().toPath();
        FileUtil.writeToFiles(0, src);
        int numFiles = 1;
        long fileSize = Files.size(src);
        ReturnStatus status = this.fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testCopyFileLessThanBlockSize() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 257;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0xbc, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = this.fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testCopyFileMultipleBlockSize() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 2048;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0xF0, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = this.fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testCopyFileNotMultipleBlockSize() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 651;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0x19, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = this.fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testCopyFileSameBlockSize() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 512;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0xcd, fileSize);
        FileUtil.writeToFiles(content, src);
        ReturnStatus status = this.fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testCopyFileTwiceNotMultipleBlockSize() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 557;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0x18, fileSize);
        FileUtil.writeToFiles(content, src);
        Files.setLastModifiedTime(src, FileTime.fromMillis(0));
        ReturnStatus status = this.fileCopy(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
        ReturnStatus status2 = this.fileCopy(src, dst);
        assertTrue(status2.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status2.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status2.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status2.stats.getTotalLiteralSize() == 0);
        assertTrue(status2.stats.getTotalMatchedSize() == fileSize);
    }
    
    @Test
    public void testCopyFileTwiceNotMultipleBlockSizeTimes() throws IOException {
        Path src = this.tempDir.newFile().toPath();
        Path dst = Paths.get(src.toString() + ".copy");
        int fileSize = 557;
        int numDirs = 0;
        int numFiles = 1;
        byte[] content = FileUtil.generateBytes(0x18, fileSize);
        FileUtil.writeToFiles(content, src);
        Files.setLastModifiedTime(src, FileTime.fromMillis(0));
        ReturnStatus status = this.fileCopy(src, dst, "--times");
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
        ReturnStatus status2 = this.fileCopy(src, dst);
        assertTrue(status2.rc == 0);
        assertTrue(FileUtil.isContentIdentical(src, dst));
        assertTrue(status2.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status2.stats.getNumTransferredFiles() == 0);
        assertTrue(status2.stats.getTotalLiteralSize() == 0);
        assertTrue(status2.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testFileUtilIdenticalDirsWithSymlinks() throws IOException {
        Path left = this.tempDir.newFolder("left_dir").toPath();
        Path right = this.tempDir.newFolder("right_dir").toPath();
        Path left_file = Files.createFile(left.resolve("file1"));
        Path right_file = Files.createFile(right.resolve("file1"));
        Files.createSymbolicLink(left.resolve("link1"), left_file);
        Files.createSymbolicLink(right.resolve("link1"), right_file);
        assertTrue(FileUtil.isDirectoriesIdentical(left, right));
    }
    
    @Test
    public void testFileUtilIdenticalEmptyDirs() throws IOException {
        Path left = this.tempDir.newFolder("left_dir").toPath();
        Path right = this.tempDir.newFolder("right_dir").toPath();
        assertTrue(FileUtil.isDirectoriesIdentical(left, right));
    }
    
    @Test
    public void testFileUtilIdenticalEmptyFiles() throws IOException {
        Path left = this.tempDir.newFile("left_file").toPath();
        Path right = this.tempDir.newFile("right_file").toPath();
        assertTrue(FileUtil.isContentIdentical(left, right));
    }
    
    @Test
    public void testFileUtilIdenticalFiles() throws IOException {
        Path left = this.tempDir.newFile("left_file").toPath();
        Path right = this.tempDir.newFile("right_file").toPath();
        FileUtil.writeToFiles(127, left, right);
        assertTrue(FileUtil.isContentIdentical(left, right));
    }
    
    @Test
    public void testFileUtilNotIdenticalDirs() throws IOException {
        Path left = this.tempDir.newFolder("left_dir").toPath();
        Path right = this.tempDir.newFolder("right_dir").toPath();
        Files.createFile(left.resolve("file1"));
        assertFalse(FileUtil.isDirectoriesIdentical(left, right));
    }
    
    @Test
    public void testFileUtilNotIdenticalDirs2() throws IOException {
        Path left = this.tempDir.newFolder("left_dir").toPath();
        Path right = this.tempDir.newFolder("right_dir").toPath();
        Path left_file = Files.createFile(left.resolve("file1"));
        Files.createFile(right.resolve("file1"));
        FileUtil.writeToFiles(0, left_file);
        assertFalse(FileUtil.isDirectoriesIdentical(left, right));
    }
    
    @Test
    public void testFileUtilNotIdenticalFiles() throws IOException {
        Path left = this.tempDir.newFile("left_file").toPath();
        Path right = this.tempDir.newFile("right_file").toPath();
        FileUtil.writeToFiles(127, left);
        FileUtil.writeToFiles(128, right);
        assertFalse(FileUtil.isContentIdentical(left, right));
    }
    
    @Test(timeout = 1000)
    public void testInvalidPassword() throws InterruptedException {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        final String restrictedModuleName = "Restricted";
        final String authToken = "testAuthToken";
        
        Callable<Integer> serverTask = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Path modulePath = SystemTest.this.tempDir.newFolder().toPath();
                Module m = new SimpleModule(restrictedModuleName, modulePath, "a test module", true, false);
                RestrictedModule rm = new SimpleRestrictedModule(authToken, m, restrictedModuleName, "a restricted module");
                int rc = SystemTest.this.newServer(new TestModules(rm)).setIsListeningLatch(isListeningLatch).start(new String[] { "--port=14415" });
                return rc;
            }
        };
        this.service.submit(serverTask);
        isListeningLatch.await();
        YajsyncClient client = this.newClient().setStandardOut(this.nullOut);
        System.setIn(new ByteArrayInputStream((authToken + "fail").getBytes()));
        int rc = client.start(new String[] { "--port=14415", "--password-file=-", "localhost::" + restrictedModuleName });
        assertTrue(rc != 0);
    }
    
    @Test
    public void testLocalListDotDir() throws IOException {
        Path dir = this.tempDir.newFolder("dir").toPath();
        Files.createFile(dir.resolve("file"));
        ReturnStatus status = this.listFiles(Paths.get("."), "--cwd=" + dir);
        int numFiles = 2;
        long fileSize = 0;
        assertTrue(status.rc == 0);
        assertTrue(status.stats != null);
        assertTrue(status.stats.getNumFiles() == numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == 0);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test
    public void testLocalListDotDirEmpty() throws IOException {
        Path src = this.tempDir.newFolder().toPath();
        ReturnStatus status = this.listFiles(Paths.get("."), "--cwd=" + src);
        int numFiles = 1;
        long fileSize = 0;
        assertTrue(status.rc == 0);
        assertTrue(status.stats != null);
        assertTrue(status.stats.getNumFiles() == numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == 0);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    @Test(timeout = 1000)
    public void testProtectedServerConnection() throws InterruptedException {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        final String restrictedModuleName = "Restricted";
        final String authToken = "ëẗÿåäöüﭏ사غ";
        Callable<Integer> serverTask = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Path modulePath = SystemTest.this.tempDir.newFolder().toPath();
                Module m = new SimpleModule(restrictedModuleName, modulePath, "a test module", true, false);
                RestrictedModule rm = new SimpleRestrictedModule(authToken, m, restrictedModuleName, "a restricted module");
                int rc = SystemTest.this.newServer(new TestModules(rm)).setIsListeningLatch(isListeningLatch).start(new String[] { "--port=14415" });
                return rc;
            }
        };
        this.service.submit(serverTask);
        isListeningLatch.await();
        YajsyncClient client = this.newClient().setStandardOut(this.nullOut);
        System.setIn(new ByteArrayInputStream(authToken.getBytes()));
        int rc = client.start(new String[] { "--port=14415", "--password-file=-", "localhost::" + restrictedModuleName });
        assertTrue(rc == 0);
    }
    
    @Test(expected = SocketTimeoutException.class, timeout = 2000)
    public void testReadTimeout() throws Throwable {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        final int port = 14416;
        
        Thread serverThread = new ReadTimeoutTestServer(isListeningLatch, port);
        serverThread.start();
        isListeningLatch.await();
        
        int _contimeout = 0;
        int _timeout = 1;
        
        if (!Environment.hasAllocateDirectArray()) {
            Environment.setAllocateDirect(false);
        }
        
        DuplexByteChannel sock = new StandardChannelFactory().open("localhost", port, _contimeout, _timeout);
        
        this.testTimeoutHelper(sock);
    }
    
    // FIXME: latch might not get decreased if exception occurs
    // FIXME: port might be unavailable, open it here and inject it
    @Test(timeout = 100)
    public void testServerConnection() throws InterruptedException {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        
        Callable<Integer> serverTask = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                Path modulePath = SystemTest.this.tempDir.newFolder().toPath();
                Module m = new SimpleModule("test", modulePath, "a test module", true, false);
                int rc = SystemTest.this.newServer(new TestModules(m)).setIsListeningLatch(isListeningLatch).start(new String[] { "--port=14415" });
                return rc;
            }
        };
        this.service.submit(serverTask);
        isListeningLatch.await();
        YajsyncClient client = this.newClient().setStandardOut(this.nullOut);
        int rc = client.start(new String[] { "--port=14415", "localhost::" });
        assertTrue(rc == 0);
    }
    
    @Test(timeout = 100)
    public void testServerHelp() throws InterruptedException, IOException {
        int rc = this.newServer(new TestModules()).setStandardOut(this.nullOut).start(new String[] { "--help" });
        assertTrue(rc == 0);
    }
    
    @Test
    public void testSiblingSubdirsSubstring() throws IOException {
        Path src = this.tempDir.newFolder().toPath();
        Path dst = this.tempDir.newFolder().toPath();
        Path srcDir1 = src.resolve("dir");
        Path srcDir2 = src.resolve("dir.sub");
        Path srcFile1 = srcDir1.resolve("file1");
        Path srcFile2 = srcDir2.resolve("file2");
        Files.createDirectory(srcDir1);
        Files.createDirectory(srcDir2);
        FileUtil.writeToFiles(7, srcFile1);
        FileUtil.writeToFiles(8, srcFile2);
        int numDirs = 3;
        int numFiles = 2;
        long fileSize = FileUtil.du(srcFile1, srcFile2);
        ReturnStatus status = this.recursiveCopyTrailingSlash(src, dst);
        assertTrue(status.rc == 0);
        assertTrue(FileUtil.isDirectoriesIdentical(src, dst));
        assertTrue(status.stats.getNumFiles() == numDirs + numFiles);
        assertTrue(status.stats.getNumTransferredFiles() == numFiles);
        assertTrue(status.stats.getTotalLiteralSize() == fileSize);
        assertTrue(status.stats.getTotalMatchedSize() == 0);
    }
    
    private void testTimeoutHelper(DuplexByteChannel sock) throws Throwable {
        try {
            new RsyncClient.Builder().buildRemote(sock /* in */, sock /* out */, true).receive("", new String[] { "/" }).to(Paths.get("/"));
        } catch (ChannelException e) {
            throw e.getCause();
        }
    }
    
    @Test(expected = SocketTimeoutException.class, timeout = 2000)
    public void testTlsConnectionTimeout() throws Throwable {
        int _contimeout = 1;
        int _timeout = 0;
        
        // connect to a non routable ip to provoke the connection timeout
        DuplexByteChannel sock = new SSLChannelFactory().open("10.0.0.0", 14415, _contimeout, _timeout);
        
        this.testTimeoutHelper(sock);
    }
    
    @Test(expected = SocketTimeoutException.class, timeout = 2000)
    public void testTlsReadTimeout() throws Throwable {
        final CountDownLatch isListeningLatch = new CountDownLatch(1);
        final int port = 14417;
        
        Thread serverThread = new ReadTimeoutTestServer(isListeningLatch, port);
        serverThread.start();
        isListeningLatch.await();
        
        int _contimeout = 0;
        int _timeout = 1;
        
        if (!Environment.hasAllocateDirectArray()) {
            Environment.setAllocateDirect(false);
        }
        
        DuplexByteChannel sock = new SSLChannelFactory().open("localhost", port, _contimeout, _timeout);
        
        this.testTimeoutHelper(sock);
    }
}

class TestModuleProvider extends ModuleProvider {
    private final Modules modules;
    
    TestModuleProvider(Modules modules) {
        this.modules = modules;
    }
    
    @Override
    public void close() {
        /* nop */
    }
    
    @Override
    public Modules newAnonymous(InetAddress address) {
        return this.modules;
    }
    
    @Override
    public Modules newAuthenticated(InetAddress address, Principal principal) {
        return this.modules;
    }
    
    @Override
    public Collection<Option> options() {
        return Collections.emptyList();
    }
}

class TestModules implements Modules {
    private final Map<String, Module> modules;
    
    TestModules(Module... modules) {
        this.modules = new HashMap<>();
        for (Module module : modules) {
            this.modules.put(module.getName(), module);
        }
    }
    
    @Override
    public Iterable<Module> all() {
        return this.modules.values();
    }
    
    @Override
    public Module get(String moduleName) throws ModuleException {
        Module module = this.modules.get(moduleName);
        if (module == null) {
            throw new ModuleException("no such module: " + moduleName);
        }
        return module;
    }
}
