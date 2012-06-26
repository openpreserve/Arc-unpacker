package dk.statsbiblioteket.scape.arcunpacker;


import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.net.URL;


/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 11/30/11
 * Time: 1:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class WebArchiveFileTest {


    @org.junit.Test
    public void testNext() throws Exception {
        File file = new File(WebArchiveFileTest.class.getResource("/IAH-20080430204825-00000-blackbook.arc.gz").toURI());

        UnpackConfig config = new UnpackConfig(0, 1000, UnpackConfig.Naming.OFFSET);
        WebArchiveFile webArchiveFile = new WebArchiveFile(file.getName(),file,file.length(),config);
        long filelength = file.length();
        int count = 0;
        ArcRecord resource = webArchiveFile.next();
        long offset=0,length = 0;
        while (resource != null){
            offset = resource.getOffsetInArc();
            length = resource.getLength();
            resource = webArchiveFile.next();
            count++;
        }
        assertThat(count, is(261));
        assertTrue(offset < filelength);
        System.out.println(filelength);
        System.out.println(offset);
        System.out.println(length);
    }

    @org.junit.Test
    public void testNext2() throws Exception {
        File file = new File(WebArchiveFileTest.class.getResource("/IAH-20080430204825-00000-blackbook.warc.gz").toURI());
        UnpackConfig config = new UnpackConfig(200, 299, UnpackConfig.Naming.OFFSET);
        WebArchiveFile webArchiveFile = new WebArchiveFile(file.getName(),file,file.length(),config);
        long filelength = file.length();
        int count = 0;
        ArcRecord resource = webArchiveFile.next();
        long offset=0,length = 0;

        while (resource != null){
            offset = resource.getOffsetInArc();
            length = resource.getLength();

            resource = webArchiveFile.next();
            count++;
        }
        assertThat(count, is(214));
        assertTrue(offset < filelength);
        System.out.println(filelength);
        System.out.println(offset);
        System.out.println(length);

    }

}
