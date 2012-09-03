/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.statsbiblioteket.scape.arcunpacker.mapreduce;

import dk.statsbiblioteket.scape.arcunpacker.HadoopArcRecord;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author onbram
 */

public class ArcRecordReaderTest {

    private ArcInputFormat myArcF;
    private FileSplit split;
    private TaskAttemptContext tac;


    @Before
    public void setUp() throws Exception {

        InputStream is = ArcRecordReaderTest.class.getResourceAsStream("/ONBdevSample.arc.gz");
        File file = FileUtils.getTmpFile("archd", "arc.gz");
        OutputStream out = new FileOutputStream(file);
        int read = 0;
        byte[] bytes = new byte[1024];

        while ((read = is.read(bytes)) != -1) {
            out.write(bytes, 0, read);
        }

        is.close();
        out.flush();
        out.close();


        Configuration conf = new Configuration();
        Job job = new Job(conf);


        split = new FileSplit(new Path(file.getAbsolutePath()), 0, file.length(), null);

        myArcF = new ArcInputFormat();
        tac = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    }



    /**
     * Test of nextKeyValue method, of class ArcRecordReader.
     */
    @Test
    public void testNextKeyValue() throws Exception {
        RecordReader<Text, HadoopArcRecord> recordReader = myArcF.createRecordReader(split, tac);
        recordReader.initialize(split, tac);
        int start = 1;
        while (recordReader.nextKeyValue()) {
            Text currKey = recordReader.getCurrentKey();
            HadoopArcRecord currValue = recordReader.getCurrentValue();

            String currMIMEType = currValue.getMimeType();
            String currType = currValue.getType();
            String currURL = currValue.getUrl();
            InputStream currStream = currValue.getContents();
            String currContent;
            String myContentString;
            int myContentStringIndex;
            Date currDate = currValue.getDate();
            int currHTTPrc = currValue.getHttpReturnCode();
            int currLength = currValue.getLength();

            System.out.println("KEY " + start + ": " + currKey + " MIME Type: " + currMIMEType + " Type: " + currType + " URL: " + currURL + " Date: " + currDate.toString() + " HTTPrc: " + currHTTPrc + " Length: " + currLength);

            // check example record 1 (first one and the header of the ARC file)
            if (start == 1) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.                
                currContent = content2String(currStream);
                myContentString = "<dcterms:isPartOf>";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("ID not equal","20120619072058/filedesc://8839-51-20120619072058-00000-testcrawler02.onb.ac.at.arc", currKey.toString());
                assertEquals("MIME Type not equal","text/plain", currMIMEType);
                assertEquals("Response type not equal","response", currType);
                assertEquals("URL not equal","filedesc://8839-51-20120619072058-00000-testcrawler02.onb.ac.at.arc", currURL);
                assertEquals("Date not equal","Tue Jun 19 07:20:58 CEST 2012", currDate.toString());
                assertEquals("HTTPrc not equal",-1, currHTTPrc);
                assertEquals("Record length not equal",1209, currLength);
                assertEquals("Content seems not to be correct",530, myContentStringIndex);
            }
            // check example record 105 (in the middle)
            if (start == 105) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.   
                currContent = content2String(currStream);
                myContentString = "background-position:495px";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("ID not equal","20120619072140/http://www.onb.ac.at/sammlungen/CSS_template_literaturarchiv_small.css", currKey.toString());
                assertEquals("MIME Type not equal","text/css", currMIMEType);
                assertEquals("Response type not equal","response", currType);
                assertEquals("URL not equal","http://www.onb.ac.at/sammlungen/CSS_template_literaturarchiv_small.css", currURL);
                assertEquals("Date Type not equal","Tue Jun 19 07:21:40 CEST 2012", currDate.toString());
                assertEquals("HTTPrc not equal",200, currHTTPrc);
                assertEquals("Length Type not equal",185, currLength);
                assertEquals("Content seems not to be correct",117, myContentStringIndex);
            }
            // check example record 121 (last one)
            if (start == 121) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.   
                currContent = content2String(currStream);
                myContentString = "enYf*X";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("20120619072145/http://www.onb.ac.at/images/HAN/HAN_Hauptbild_225_120.jpg", currKey.toString());
                assertEquals("MIME Type not equal","image/jpeg", currMIMEType);
                assertEquals("Response type not equal","response", currType);
                assertEquals("URL not equal","http://www.onb.ac.at/images/HAN/HAN_Hauptbild_225_120.jpg", currURL);
                assertEquals("Date Type not equal","Tue Jun 19 07:21:45 CEST 2012", currDate.toString());
                assertEquals("HTTPrc not equal",200, currHTTPrc);
                assertEquals("Length Type not equal",49443, currLength);
                assertEquals("Content seems not to be correct",45770, myContentStringIndex);
            }
            start++;
        }
    }



    private String content2String(InputStream contents) throws IOException {
        StringWriter myWriter = new StringWriter();
        IOUtils.copy(contents, myWriter, null);
        String out = myWriter.toString();
        //System.out.print("CONTENT: " + out);  //uncomment this line to print the inputsream (e.g. to find new "myContentString"
        return out;
    }
}
