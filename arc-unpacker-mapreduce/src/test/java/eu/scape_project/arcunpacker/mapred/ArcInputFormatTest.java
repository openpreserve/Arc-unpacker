package eu.scape_project.arcunpacker.mapred;

import eu.scape_project.arcunpacker.ArcRecord;
import eu.scape_project.arcunpacker.HadoopArcRecord;
import eu.scape_project.arcunpacker.mapred.ArcInputFormat;

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
public class ArcInputFormatTest {
    @Test
    public void testGetRecordReaderOnArc() throws Exception {
        ArcInputFormat inputFormat = new ArcInputFormat();
        JobConf jobConf = new JobConf();
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("IAH-20080430204825-00000-blackbook.arc.gz").toURI());
        InputSplit split = new FileSplit(new Path(file.getAbsolutePath()),0,file.length(),jobConf);
        Reporter reporter = Reporter.NULL;
        RecordReader<Text, HadoopArcRecord> recordReader = inputFormat.getRecordReader(split, jobConf, reporter);
        Text key = recordReader.createKey();
        HadoopArcRecord value = recordReader.createValue();
        boolean more = true;
        while (more){
            System.out.println(key);
           // printValue(value);
            more = recordReader.next(key,value);

//            System.out.println(recordReader.getProgress());

            System.out.println(value.getHttpReturnCode());
//            System.out.println(value.getLength());
            System.out.println();
        }

    }

     @Test
    public void testGetRecordReaderOnWArc() throws Exception {
        ArcInputFormat inputFormat = new ArcInputFormat();
        JobConf jobConf = new JobConf();
         File file = new File(Thread.currentThread().getContextClassLoader().getResource("IAH-20080430204825-00000-blackbook.warc.gz").toURI());
        InputSplit split = new FileSplit(new Path(file.getAbsolutePath()),0,file.length(),jobConf);
        Reporter reporter = Reporter.NULL;
        RecordReader<Text, HadoopArcRecord> recordReader = inputFormat.getRecordReader(split, jobConf, reporter);
        Text key = recordReader.createKey();
        HadoopArcRecord value = recordReader.createValue();
        boolean more = true;
        while (more){

            System.out.println(key);
            System.out.println(value.getUrl());
            System.out.println(value.getHttpReturnCode());
            System.out.println(value.getType());
            more = recordReader.next(key,value);


//            System.out.println(recordReader.getProgress());
  //          System.out.println(value.getMimeType());

    //        System.out.println(value.getLength());
            System.out.println();
            //printValue(value);
        }

    }

    public void printValue(ArcRecord value) throws IOException {
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
