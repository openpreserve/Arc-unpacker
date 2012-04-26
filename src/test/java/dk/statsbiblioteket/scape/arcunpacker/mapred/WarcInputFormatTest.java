package dk.statsbiblioteket.scape.arcunpacker.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/26/12
 * Time: 12:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class WarcInputFormatTest {
    @Test
    public void testGetRecordReaderOnArc() throws Exception {
        WarcInputFormat inputFormat = new WarcInputFormat();
        JobConf jobConf = new JobConf();
        File file = new File("src/test/resources/IAH-20080430204825-00000-blackbook.arc.gz");
        InputSplit split = new FileSplit(new Path(file.getAbsolutePath()),0,file.length(),jobConf);
        Reporter reporter = Reporter.NULL;
        RecordReader<Text, WarcRecord> recordReader = inputFormat.getRecordReader(split, jobConf, reporter);
        Text key = recordReader.createKey();
        WarcRecord value = recordReader.createValue();
        boolean more = true;
        while (more){
            more = recordReader.next(key,value);

            System.out.println(recordReader.getProgress());
            System.out.println(key);
            System.out.println(value.getMimeType());
            System.out.println(value.getHttpReturnCode());
            System.out.println(value.getLength());
            System.out.println();
        }

    }

     @Test
    public void testGetRecordReaderOnWArc() throws Exception {
        WarcInputFormat inputFormat = new WarcInputFormat();
        JobConf jobConf = new JobConf();
        File file = new File("src/test/resources/IAH-20080430204825-00000-blackbook.warc.gz");
        InputSplit split = new FileSplit(new Path(file.getAbsolutePath()),0,file.length(),jobConf);
        Reporter reporter = Reporter.NULL;
        RecordReader<Text, WarcRecord> recordReader = inputFormat.getRecordReader(split, jobConf, reporter);
        Text key = recordReader.createKey();
        WarcRecord value = recordReader.createValue();
        boolean more = true;
        while (more){
            more = recordReader.next(key,value);

/*
            System.out.println(recordReader.getProgress());
            System.out.println(key);
            System.out.println(value.getMimeType());
            System.out.println(value.getHttpReturnCode());
            System.out.println(value.getLength());
            System.out.println();
*/
            printValue(value);
        }

    }

    public void printValue(WarcRecord value) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(value.getContents()));
        while (true){
            String line = reader.readLine();
            if (line == null){
                break;
            } else {
                System.out.println(line);
            }

        }
        System.out.println();
        System.out.println("---------------------------------------");
        System.out.println();

    }
}
