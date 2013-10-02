/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.scape_project.arcunpacker.mapreduce;

import eu.scape_project.arcunpacker.HadoopArcRecord;
import eu.scape_project.arcunpacker.mapreduce.ArcInputFormat;

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

public class ArcRecordReaderTest2 {

    private ArcInputFormat myArcF;
    private FileSplit split;
    private TaskAttemptContext tac;


    @Before
    public void setUp() throws Exception {

        InputStream is = ArcRecordReaderTest2.class.getResourceAsStream("/real.arc");
        File file = FileUtils.getTmpFile("archd", "arc");
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
