package dk.statsbiblioteket.scape.arcunpacker;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 11/30/11
 * Time: 1:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class ArchiveTest {
    @org.junit.Test
    public void testMain() throws Exception {
        Archive archive = new Archive(new File("src/test/resources/test.arc"));
        File unpack = new File("build/testunpack");

        FileUtils.deleteDirectory(unpack);
        archive.unpack(unpack);
        FileUtils.deleteDirectory(unpack);
    }
}
