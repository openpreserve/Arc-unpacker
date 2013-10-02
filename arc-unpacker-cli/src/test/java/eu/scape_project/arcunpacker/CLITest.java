package eu.scape_project.arcunpacker;

import org.apache.commons.io.FileUtils;

import eu.scape_project.arcunpacker.CLI;

import java.io.File;
import java.net.URL;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 6/26/12
 * Time: 2:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class CLITest {
    @org.junit.Test
    public void testPrintUsage() throws Exception {
        CLI.printUsage();
    }

    @org.junit.Test
    public void testMain() throws Exception {

        URL file = CLITest.class.getResource("/IAH-20080430204825-00000-blackbook.arc.gz");

        String path = new File(file.toURI()).getAbsolutePath();

        File unpack = new File("build/testunpack");
        FileUtils.deleteDirectory(unpack);
        CLI.main(new String[]{"-f", path, "-naming", "MD5", "-o", unpack.getAbsolutePath(),"-reports",unpack.getAbsolutePath()+"/reports"});
        assertThat(unpack.list().length, is(262));
        FileUtils.deleteDirectory(unpack);


    }

    @org.junit.Test
       public void testMain2() throws Exception {

           URL file = CLITest.class.getResource("/IAH-20080430204825-00000-blackbook.warc.gz");

           String path = new File(file.toURI()).getAbsolutePath();

           File unpack = new File("build/testunpack");
           FileUtils.deleteDirectory(unpack);
           CLI.main(new String[]{"-f", path, "-naming", "OFFSET", "-o", unpack.getAbsolutePath(),"-minResp","200","-maxResp","299"});
           assertThat(unpack.list().length, is(214));
           FileUtils.deleteDirectory(unpack);


       }



}
