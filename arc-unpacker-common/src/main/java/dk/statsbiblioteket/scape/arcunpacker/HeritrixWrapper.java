package dk.statsbiblioteket.scape.arcunpacker;

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

    private static final SimpleDateFormat arcDateFormat = new SimpleDateFormat( "yyyyMMddHHmmss" );
    private static final SimpleDateFormat warcDateformat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );


    private ArchiveRecord nativeArchiveRecord;

    private ArcRecord currentArcRecord;
    private String currentID;


    public HeritrixWrapper(String fileName, InputStream fileInputStream,long fileLength) throws IOException {
        this.fileLength = fileLength;
        ArchiveReader reader = ArchiveReaderFactory.get(fileName, fileInputStream, true);
        recordIterator = reader.iterator();

    }

    public synchronized boolean nextKeyValue() throws IOException {
        if (recordIterator.hasNext()) {
            nativeArchiveRecord = recordIterator.next();
            currentID = getID(nativeArchiveRecord);
            return true;
        }
        return false;
    }

    public String getCurrentID() {
        return currentID;
    }

    public synchronized ArcRecord getCurrentArcRecord(ArcRecord value) throws IOException {
        currentArcRecord = value;
        loadCurrentArdRecord();
        return currentArcRecord;
    }

    private void loadCurrentArdRecord() throws IOException {

        currentArcRecord.clear();
        long recordLength = nativeArchiveRecord.getHeader().getLength();
        long contentBegin = nativeArchiveRecord.getHeader().getContentBegin();
        if (contentBegin < 0) {
            contentBegin = 0;
        }
        long positionInFile = nativeArchiveRecord.getHeader().getOffset();
        long contentSize = recordLength - contentBegin;
        currentArcRecord.setUrl(getResourceUrl(nativeArchiveRecord));
        //currentArcRecord.setMimeType(nativeArchiveRecord.getHeader().getMimetype());
        currentArcRecord.setDate(getResourceDate(nativeArchiveRecord));
        currentArcRecord.setType(getType(nativeArchiveRecord));
        Header[] headers = getHttpHeaders(nativeArchiveRecord);
        currentArcRecord.setHttpReturnCode(getHttpReturnCode(nativeArchiveRecord, headers));
        currentArcRecord.setMimeType(getMimeType(nativeArchiveRecord, headers)); // to support ARC and WARC
        nativeArchiveRecord.skip(contentBegin);
        currentArcRecord.setContents(nativeArchiveRecord, (int) contentSize);
        position = positionInFile;


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




    private String getMimeType(ArchiveRecord nativeRecord, Header[] headers) {

        // *** 4 cases are covered here
        // 1) ARCRecord
        // 2) WARCRecord with a HTTPHeader (WARC RESPONSE records)
        // 3) WARCRecord - no HttpHeader (WARCINFO record [the WARC container header] and DNS records)
        // 4) Neither a ARCRecord nor a WARCRecord (REQUEST and METADATA records of WARCs)
        // *** 1, 3, 4 do return the record MIME TYPE (which is is the content MIME TYPE of ARC records and the record MIME TYPE of WARC REQUEST and METADATA records)
        // *** 2 returns the MIME TYPE stored in the HTTPHeader of the RESPONSE (content) record.
        //          Otherwise this record returns: "application/http; msgtype=response") - which is true too but not the information we want. We want to see the MIME TYPE of the content stream as the result.

        // CASE 2:
        if (nativeRecord instanceof WARCRecord) {
            if (headers != null && headers.length >= 1) {
                String currentHeaderName;
                for (Header currentHeader : headers) {
                    currentHeaderName = currentHeader.getName().toLowerCase();
                    if (currentHeaderName.equals("content-type")) {
                        return currentHeader.getValue();
                    }
                }
            }
        }

        // CASE 1, 3, 4:
        return nativeRecord.getHeader().getMimetype();

    }


    public long getPosition() {
        return position;
    }

    public long getFileLength() {
        return fileLength;
    }
}
