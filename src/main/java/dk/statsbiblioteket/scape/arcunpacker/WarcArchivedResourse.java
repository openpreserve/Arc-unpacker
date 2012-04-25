package dk.statsbiblioteket.scape.arcunpacker;

import org.archive.io.warc.WARCRecord;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_DATE;
import static org.archive.io.warc.WARCConstants.HEADER_KEY_TYPE;
import static org.archive.io.warc.WARCConstants.HEADER_KEY_URI;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/25/12
 * Time: 11:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class WarcArchivedResourse extends ArchivedResource{
    public WarcArchivedResourse(WARCRecord record, File archiveFile) {
        super(record, archiveFile);
    }

    @Override
    public String getResourceURL() {
        String url = getHeader().getHeaderValue(HEADER_KEY_URI).toString();
        return url;
    }

    @Override
    public Date getServerReportedLastModified() throws ParseException {
        SimpleDateFormat dateformat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
        return  dateformat.parse( getHeader().getHeaderValue( HEADER_KEY_DATE ).toString() );

    }

    @Override
    public boolean isUnprintable() {
        String type = getHeader().getHeaderValue(HEADER_KEY_TYPE).toString();
        if (type.equals("warcinfo")){
            return true;
        }
        return false;
    }

    @Override
    public String getEncodedPath(UnpackConfig.Naming pathNamingScheme) {
        String path = super.getEncodedPath(
                pathNamingScheme);
        String recordType = ( String ) getHeader().getHeaderValue( HEADER_KEY_TYPE );
        if( !recordType.equals( "response" ) ) {
            path = path + "_" + recordType.toUpperCase();
        }
        return path;

    }
}
