package dk.statsbiblioteket.scape.arcunpacker;

import org.archive.io.arc.ARCRecord;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/25/12
 * Time: 11:31 AM
 * To change this template use File | Settings | File Templates.
 */
public class ArcArchivedResource extends ArchivedResource{
    public ArcArchivedResource(ARCRecord record, File archiveFile) {
        super(record, archiveFile);
    }

    @Override
    public String getResourceURL() {
        return getHeader().getUrl();
    }

    @Override
    public Date getServerReportedLastModified() throws ParseException {
        SimpleDateFormat dateformat = new SimpleDateFormat( "yyyyMMddHHmmss" );
        return dateformat.parse( getHeader().getDate() );

    }

    @Override
    public boolean isUnprintable() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
