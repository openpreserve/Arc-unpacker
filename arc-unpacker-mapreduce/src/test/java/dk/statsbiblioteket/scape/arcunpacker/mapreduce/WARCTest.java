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

import java.io.*;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author onbram
 */

public class WARCTest {

    private ArcInputFormat myArcF;
    private FileSplit split;
    private TaskAttemptContext tac;


    public void setUpONBSample() throws Exception {

        InputStream is = ArcRecordReaderTest.class.getResourceAsStream("/ONBdevSample.warc");
        File file = FileUtils.getTmpFile("archd", "warc");
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

    public void setUpIAH() throws Exception {

        InputStream is = ArcRecordReaderTest.class.getResourceAsStream("/IAH-20080430204825-00000-blackbook.warc.gz");
        File file = FileUtils.getTmpFile("archd", "warc.gz");
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

    @Test
    public void testONB() throws Exception {
        setUpONBSample();
        testNextKeyValue();
    }

    @Test
    public void testIAH() throws Exception {
        setUpIAH();
        testIAHNextKeyValue();
    }


    /**
     * Test of nextKeyValue method, of class ArcRecordReader.
     */

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

            // check example record 1 (first one and the header of the WARC file)
            if (start == 1) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.                
                currContent = content2String(currStream);
                myContentString = "isPartOf: scapewarcgz";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("ID not equal","<urn:uuid:ebb11a78-e0c4-4396-8c62-b08581b0ef28>", currKey.toString());
                assertEquals("MIME Type not equal","application/warc-fields", currMIMEType);
                assertEquals("Response type not equal","warcinfo", currType);
                assertEquals("URL not equal",null, currURL);
                assertEquals("Date not equal","Tue Jun 19 07:06:45 CEST 2012", currDate.toString());
                assertEquals("HTTPrc not equal",-1, currHTTPrc);
                assertEquals("Record length not equal",457, currLength);
                assertEquals("Content seems not to be correct",205, myContentStringIndex);
            }
            // check example record 195 (in the middle)
            if (start == 195) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.   
                currContent = content2String(currStream);
                myContentString = "56789:CDEFGH";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("<urn:uuid:9e0fe029-12e9-43d0-acbe-4cb83c2c766c>", currKey.toString());
                assertEquals("MIME Type not equal","image/jpeg", currMIMEType);
                assertEquals("Response type not equal","response", currType);
                assertEquals("URL not equal","http://www.onb.ac.at/images/Musiksammlung/exlibris.jpg", currURL);
                assertEquals("Date Type not equal","Tue Jun 19 07:09:09 CEST 2012", currDate.toString());
                assertEquals("HTTPrc not equal",200, currHTTPrc);
                assertEquals("Length Type not equal",18538, currLength);
                assertEquals("Content seems not to be correct",282, myContentStringIndex);
            }
            // check example record 379 (last REQUEST record)
            if (start == 379) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.   
                currContent = content2String(currStream);
                myContentString = "";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("<urn:uuid:3b9dca99-481c-449a-8b4a-d3022d4d23de>", currKey.toString());
                assertEquals("MIME Type not equal","application/http; msgtype=request", currMIMEType);
                assertEquals("Response type not equal","request", currType);
                assertEquals("URL not equal","http://www.onb.ac.at/images/kartensammlung/globenmuseum1_indexA.jpg", currURL);
                assertEquals("Date Type not equal","Tue Jun 19 07:11:21 CEST 2012", currDate.toString());
                assertEquals("HTTPrc not equal",-1, currHTTPrc);
                assertEquals("Length Type not equal",0, currLength);
                assertEquals("Content seems not to be correct",0, myContentStringIndex);
            }
            start++;
        }
    }


    /**
     * Test of nextKeyValue method, of class ArcRecordReader.
     */

    public void testIAHNextKeyValue() throws Exception {



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

            // check example record 1 (first one and the header of the WARC file)
            if (start == 1) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.
                currContent = content2String(currStream);
                myContentString = "isPartOf: scapewarcgz";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("ID not equal","<urn:uuid:35f02b38-eb19-4f0d-86e4-bfe95815069c>", currKey.toString());
                assertEquals("MIME Type not equal","application/warc-fields", currMIMEType);
                assertEquals("Response type not equal","warcinfo", currType);
                assertEquals("URL not equal",null, currURL);
                assertEquals("Date not equal","Wed Apr 30 20:48:25 CEST 2008", currDate.toString());
                assertEquals("HTTPrc not equal",-1, currHTTPrc);
                assertEquals("Record length not equal",482, currLength);
                assertEquals("Content seems not to be correct",-1, myContentStringIndex);
            }
            // check example record 195 (in the middle)
            if (start == 195) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.
                currContent = content2String(currStream);
                myContentString = "56789:CDEFGH";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("<urn:uuid:59500b0d-3c35-470b-a6bf-060d83b627dd>", currKey.toString());
                assertEquals("MIME Type not equal","text/dns", currMIMEType);
                assertEquals("Response type not equal","response", currType);
                assertEquals("URL not equal","dns:ia341007.us.archive.org", currURL);
                assertEquals("Date Type not equal","Wed Apr 30 20:49:12 CEST 2008", currDate.toString());
                assertEquals("HTTPrc not equal",-1, currHTTPrc);
                assertEquals("Length Type not equal",64, currLength);
                assertEquals("Content seems not to be correct",-1, myContentStringIndex);
            }
            // check example record 379 (last REQUEST record)
            if (start == 379) {
                //"myContentString" is arbitrary sting snipped of which we know that it exists in the content stream and of which we know the position in the stream.
                //We will search for the string int the content we read and compare it to the values we know.
                currContent = content2String(currStream);
                myContentString = "";
                myContentStringIndex = currContent.indexOf(myContentString);
                //System.out.println("Search for: " + myContentString + "=> Index is: " + myContentStringIndex);

                assertEquals("<urn:uuid:fe0093b9-5919-476e-a49f-ac95fd610df9>", currKey.toString());
                assertEquals("MIME Type not equal","application/http; msgtype=request", currMIMEType);
                assertEquals("Response type not equal","request", currType);
                assertEquals("URL not equal","http://www.archive.org/images/guitar.jpg", currURL);
                assertEquals("Date Type not equal","Wed Apr 30 20:50:15 CEST 2008", currDate.toString());
                assertEquals("HTTPrc not equal",-1, currHTTPrc);
                assertEquals("Length Type not equal",0, currLength);
                assertEquals("Content seems not to be correct",0, myContentStringIndex);
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
