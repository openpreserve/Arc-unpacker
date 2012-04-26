package dk.statsbiblioteket.scape.arcunpacker;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpParser;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;

import static org.archive.io.warc.WARCConstants.DEFAULT_ENCODING;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/25/12
 * Time: 11:22 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class ArchivedResource {
    private static final int blockSize = 512;
    private ArchiveRecordHeader header;
    private boolean directory = false;

    private String archiveID;
    private ArchiveRecord record;


    public ArchivedResource(ArchiveRecord record, String archiveID) {

        this.record = record;
        this.archiveID = archiveID;

        header = record.getHeader();

    }

    protected ArchiveRecordHeader getHeader() {
        return header;
    }

    public long getFileSizeInBytes(){
        //Length is the length of the entire record
        //ContentBegin is the offset into the record, where the content begins
        return header.getLength()-header.getContentBegin();
    }
    public abstract String getResourceURL();

    public long getBlocks(){
        return (header.getLength() + blockSize - 1) / blockSize;
    }

    public abstract Date getServerReportedLastModified() throws ParseException;

    public String getEncodedPath(UnpackConfig.Naming pathNamingScheme) {

        String path = "/" + getResourceURL().replace(":", "/").replaceAll("/+",
                                                                          "/");

        switch (pathNamingScheme){
            case URL:
                if( path.endsWith( "/" ) ) {
                    path = path + "ROOT";
                }

                break;
            case MD5:
                path = DigestUtils.md5Hex(path)+":"+ getServerReturnedMimeType();
                break;
            case OFFSET:
                path = archiveID+":"+ getOffsetInArchiveFile()+":"+ getServerReturnedMimeType().split(";")[0];
                break;
        }
        path = path.replace("/", "-");
        return path;
    }

    private String getServerReturnedMimeType() {
        return header.getMimetype();
    }

    public int getHttpReturnCode() throws IOException {

        //if( ( record instanceof ARCRecord) || archivedResource.header.getHeaderValue( HEADER_KEY_TYPE ).equals( "response" ) ) {
        String url = getResourceURL();
        if( url.matches( "^http.*$" ) ) {
            Header[] headers = HttpParser.parseHeaders(record, DEFAULT_ENCODING);
            //first line is of the format   HttpClient-Bad-Header-Line-Failed-Parse : HTTP/1.0 200 OK
            if (headers != null){
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

    public InputStream getContents() throws IOException {
        BufferedInputStream input = new BufferedInputStream(record );
        input.skip(header.getContentBegin());
        return input;
    }

    public long getOffsetInArchiveFile() {
        return header.getOffset();
    }

    public abstract boolean isUnprintable();
}
