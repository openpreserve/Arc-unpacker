package dk.statsbiblioteket.scape.arcunpacker.mapred;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
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
public class HeritrixWrapper {


    private Iterator<ArchiveRecord> recordIterator;

    private long position = 0;
    private long fileLength = 0;
    private WarcRecord record;

    private static final SimpleDateFormat arcDateFormat = new SimpleDateFormat( "yyyyMMddHHmmss" );
    private static final SimpleDateFormat warcDateformat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );

    public HeritrixWrapper(String fileName, InputStream fileInputStream,long fileLength) throws IOException {
        ArchiveReader reader = ArchiveReaderFactory.get(fileName, fileInputStream, true);
        recordIterator = reader.iterator();


    }
    public synchronized boolean next(WarcRecord value) throws IOException {
        if (!recordIterator.hasNext()){
            return false;
        }

        ArchiveRecord nativeRecord = recordIterator.next();
        long recordLength = nativeRecord.getHeader().getLength();
        long contentBegin = nativeRecord.getHeader().getContentBegin();
        if (contentBegin < 0){
            contentBegin = 0;
        }
        long positionInFile = nativeRecord.getHeader().getOffset();
        long contentSize = recordLength-contentBegin;

        value.setUrl(getResourceUrl(nativeRecord));
        value.setID(getID(nativeRecord));
        value.setMimeType(nativeRecord.getHeader().getMimetype());
        value.setDate(getResourceDate(nativeRecord));
        value.setType(getType(nativeRecord));
        Header[] headers = getHttpHeaders(nativeRecord);
        value.setHttpReturnCode(getHttpReturnCode(nativeRecord,headers));
        nativeRecord.skip(contentBegin);
        value.setContents(nativeRecord, (int) contentSize);
        position = positionInFile;
        return true;
    }

    private String getType(ArchiveRecord nativeRecord) {
        if (nativeRecord instanceof WARCRecord) {
            WARCRecord warcRecord = (WARCRecord) nativeRecord;
            return warcRecord.getHeader().getHeaderValue(HEADER_KEY_TYPE).toString();
        } else {
            return "response";
        }
    }

    private String getID(ArchiveRecord nativeRecord){
        if (nativeRecord instanceof ARCRecord) {
            ARCRecord arcRecord = (ARCRecord) nativeRecord;
            ArchiveRecordHeader header = nativeRecord.getHeader();
            return header.getRecordIdentifier();
        } else if (nativeRecord instanceof WARCRecord) {
            WARCRecord warcRecord = (WARCRecord) nativeRecord;
            return warcRecord.getHeader().getHeaderValue(HEADER_KEY_ID).toString();
        }
        return getResourceUrl(nativeRecord);

    }

    private Header[]  getHttpHeaders(ArchiveRecord nativeRecord) throws IOException {
        if (nativeRecord instanceof ARCRecord){
            return ((ARCRecord) nativeRecord).getHttpHeaders();
        } else if (nativeRecord instanceof WARCRecord) {
            WARCRecord warcRecord = (WARCRecord) nativeRecord;
            if (warcRecord.hasContentHeaders()){
                Header[] headers = HttpParser.parseHeaders(nativeRecord, DEFAULT_ENCODING);
                return headers;
            }
        }
        return new Header[0];
    }

    private int getHttpReturnCode(ArchiveRecord nativeRecord, Header[] headers) throws IOException {
        if (nativeRecord instanceof ARCRecord) {
            ARCRecord arcRecord = (ARCRecord) nativeRecord;
            return arcRecord.getStatusCode();
        }

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
            if (url != null ){
                return url.toString();
            }
        }
        return null;
    }


    public long getPosition() {
        return position;
    }

    public long getFileLength() {
        return fileLength;
    }
}
