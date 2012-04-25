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
public class WebArchiveFileTest {
    @org.junit.Test
    public void testUsage(){
        CLI.printUsage();
    }

    @org.junit.Test
    public void testMain() throws Exception {
        WebArchiveFile webArchiveFile = new WebArchiveFile(new File("src/test/resources/IAH-20080430204825-00000-blackbook.arc.gz"));
        File unpack = new File("build/testunpack");

        FileUtils.deleteDirectory(unpack);
        UnpackConfig config = new UnpackConfig(0, 1000, UnpackConfig.Naming.OFFSET);
        webArchiveFile.unpack(unpack,config);
        FileUtils.deleteDirectory(unpack);
    }

    @org.junit.Test
    public void testMain2() throws Exception {
        WebArchiveFile webArchiveFile = new WebArchiveFile(new File("src/test/resources/IAH-20080430204825-00000-blackbook.warc.gz"));
        File unpack = new File("build/testunpack");

        FileUtils.deleteDirectory(unpack);
        UnpackConfig config = new UnpackConfig(200, 299, UnpackConfig.Naming.OFFSET);
        webArchiveFile.unpack(unpack,config);
        FileUtils.deleteDirectory(unpack);
    }

}
