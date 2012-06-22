package dk.statsbiblioteket.scape.arcunpacker.mapred;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCRecord;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import static org.archive.io.warc.WARCConstants.*;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/26/12
 * Time: 10:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class WarcRecordReader implements RecordReader<Text,WarcRecord>{


    private HeritrixWrapper archiveReaderDelegate;
    private WarcRecord record;

    public WarcRecordReader(JobConf jobConf, FileSplit fileSplit) throws IOException {
        Path path = fileSplit.getPath();
        FileSystem fileSystem = path.getFileSystem(jobConf);
        FSDataInputStream fileInputStream = fileSystem.open(path);
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        long fileLength = fileStatus.getLen();
        archiveReaderDelegate = new HeritrixWrapper(path.getName(),fileInputStream,fileLength);

    }


    @Override
    public synchronized boolean next(Text key, WarcRecord value) throws IOException {
        value.clear();
        boolean more = archiveReaderDelegate.next(value);
        key.set(value.getID());
        return more;
    }


    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public WarcRecord createValue() {
        if (record == null){
            record = new WarcRecord();
        } else {
            record.clear();
        }
        return record;
    }


    @Override
    public long getPos() throws IOException {
        return archiveReaderDelegate.getPosition();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        return  ((archiveReaderDelegate.getPosition()+0.0f)/archiveReaderDelegate.getFileLength());
    }
}
