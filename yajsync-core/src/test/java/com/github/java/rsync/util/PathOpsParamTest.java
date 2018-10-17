package com.github.java.rsync.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.github.java.rsync.internal.util.PathOps;

@RunWith(Parameterized.class)
public class PathOpsParamTest {
    @Parameters
    public static Iterable<Object[]> fileNames() {
        return Arrays.asList(new Object[][] { { "" }, { "." }, { "./" }, { "./." }, { "..." }, { "1" }, { "1/" }, { "1/." }, { "1/.." }, { "1/../" }, { "1/../." }, { "1/.././" }, { "1/.././." },
                { "1/2/.." }, { "1/2/../.." }, { "1/2/3../../." }, { "1/2/../3/../4/5/../6/7/.." }, { "1/../././2/././//3/../4/5/.././.." }, });
    }
    
    private String pathName;
    
    @Rule
    public final TemporaryFolder tempDir = new TemporaryFolder();
    
    public PathOpsParamTest(String pathName) {
        this.pathName = pathName;
    }
    
    @Test
    public void testNormalizeIdentical() throws IOException {
        Path root = this.tempDir.getRoot().toPath();
        Path p = root.resolve(this.pathName);
        Path normalized = p.normalize();
        // make sure the test only touches files below tempDir
        assertTrue(normalized.startsWith(root));
        Path created = Files.createDirectories(normalized);
        Path q = PathOps.normalizeStrict(p);
        assertTrue(Files.isSameFile(created, q));
    }
}
