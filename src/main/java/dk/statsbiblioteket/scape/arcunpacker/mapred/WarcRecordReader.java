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
import org.archive.io.arc.ARCRecord;

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


    private Iterator<ArchiveRecord> recordIterator;

    private long position = 0;
    private long fileLength = 0;
    private WarcRecord record;

    private static final SimpleDateFormat arcDateFormat = new SimpleDateFormat( "yyyyMMddHHmmss" );
    private static final SimpleDateFormat warcDateformat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
    public WarcRecordReader(JobConf jobConf, FileSplit fileSplit) throws IOException {
        Path path = fileSplit.getPath();
        FileSystem fileSystem = path.getFileSystem(jobConf);
        FSDataInputStream fileInputStream = fileSystem.open(path);
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        fileLength = fileStatus.getLen();
        ArchiveReader reader = ArchiveReaderFactory.get(path.getName(), fileInputStream, true);
        recordIterator = reader.iterator();

    }


    @Override
    public synchronized boolean next(Text key, WarcRecord value) throws IOException {
        if (!recordIterator.hasNext()){
            return false;
        }
        value.clear();
        ArchiveRecord nativeRecord = recordIterator.next();
        long recordLength = nativeRecord.getHeader().getLength();
        long contentBegin = nativeRecord.getHeader().getContentBegin();
        long positionInFile = nativeRecord.getHeader().getOffset();
        long contentSize = recordLength - contentBegin;
        value.setContents(nativeRecord, (int) contentSize);
        key.set(getResourceUrl(nativeRecord));
        value.setUrl(getResourceUrl(nativeRecord));
        value.setMimeType(nativeRecord.getHeader().getMimetype());
        value.setDate(getResourceDate(nativeRecord));
        value.setHttpReturnCode(getHttpReturnCode(nativeRecord));
        position = positionInFile;
        return true;
    }

    private int getHttpReturnCode(ArchiveRecord nativeRecord) throws IOException {
        if (nativeRecord instanceof ARCRecord){
            return ((ARCRecord) nativeRecord).getStatusCode();
        }

        String url = getResourceUrl(nativeRecord);
        if( url.matches( "^http.*$" ) ) {
            Header[] headers = HttpParser.parseHeaders(nativeRecord, DEFAULT_ENCODING);
            //first line is of the format   HttpClient-Bad-Header-Line-Failed-Parse : HTTP/1.0 200 OK
            if (headers != null && headers.length >=1){
                Header firstHeader = headers[0];
                if (firstHeader.getName().equals("HttpClient-Bad-Header-Line-Failed-Parse")){
                    if (firstHeader.getValue().startsWith("HTTP/1.")){
                        //We have a http response header
                        String[] elements = firstHeader.getValue().split(" ");
                        if (elements.length == 3){
                            String codeString = elements[1];
                            int returnCode = Integer.parseInt(codeString);
                            return returnCode;
                        }
                    }
                }
            }

        }
        return -1;
    }

    private Date getResourceDate(ArchiveRecord nativeRecord) throws IOException {
        try {
        if( nativeRecord instanceof ARCRecord) {
            return arcDateFormat.parse( nativeRecord.getHeader().getDate());
        } else {
            return  warcDateformat.parse( nativeRecord.getHeader().getHeaderValue( HEADER_KEY_DATE ).toString() );
        }
        } catch (ParseException e){
            throw new IOException("Failed to parse the date",e);
        }
    }

    private String getResourceUrl(ArchiveRecord nativeRecord) {
        if( nativeRecord instanceof ARCRecord) {
            return nativeRecord.getHeader().getUrl();
        } else {
            Object url = nativeRecord.getHeader().getHeaderValue(HEADER_KEY_URI);
            if (url == null ){
                return nativeRecord.getHeader().getHeaderValue(HEADER_KEY_ID).toString();
            } else {
                return url.toString();
            }
        }
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
        return position;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        return  ((position+0.0f)/fileLength);
    }
}
