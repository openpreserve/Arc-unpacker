package dk.statsbiblioteket.scape.arcunpacker;


import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.google.common.io.Files;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;


import static org.archive.io.warc.WARCConstants.*;

public class Archive {

    private static final Log log = LogFactory.getLog(Archive.class );
    private static final int blockSize = 512;

    private File file;
    private File outdir;
    //private ArchiveReader archivereader;



    public class ArchiveEntry {
        ArchiveRecordHeader header;
        boolean isDirectory = false;
        long lastModified;
        long offset = -1L;
        int position = -1;
        String path;
    }

    public Archive(File file) {
        this.file = file;
    }

    public void unpack(File outdir){
        this.outdir = outdir;

        try {
            int files = 0;
            int blocks = 0;

            ArchiveEntry dirEntry = new ArchiveEntry();
            dirEntry.header = null;
            dirEntry.isDirectory = true;
            dirEntry.lastModified = ( int ) ( file.lastModified() / 1000L );

            ArchiveReader archivereader = ArchiveReaderFactory.get(file, 0);
            for (ArchiveRecord record : archivereader) {
                ArchiveEntry archiveEntry = getEntry(record);
                if (archiveEntry != null) {
                    blocks += (archiveEntry.header.getLength() + blockSize - 1) / blockSize;
                    if (!archiveEntry.isDirectory) {
                        ByteBuffer buf = ByteBuffer.allocate(
                                (int) archiveEntry.header.getLength() - archiveEntry.header.getContentBegin());
                        if (readEntry(record, archiveEntry, buf) > 0) {
                            writeFile(archiveEntry, buf);
                        }
                    }
                    files++;
                }
            }
            log.info( "Archive file " + file + " structure evaluated: " + files + " URIs, " + blocks + " blocks." );
        } catch( Exception e ) {
            log.error( "ArchiveFilesystem(): " + e.getMessage(), e );
        }
    }

    private void writeFile(ArchiveEntry archiveEntry, ByteBuffer buf) throws IOException {
        File outfile = new File(outdir, archiveEntry.path);
        outfile.getParentFile().mkdirs();
        if (!outfile.createNewFile()){
            throw new IOException("File "+outfile.getAbsolutePath()+" already exists, aborting");
        }
        Files.write(buf.array(), outfile);
        outfile.setLastModified(archiveEntry.lastModified);
    }

    private int readEntry(ArchiveRecord record, ArchiveEntry archiveEntry, ByteBuffer buf) {
        if( !archiveEntry.isDirectory ) {
            try {
                if( ( record instanceof ARCRecord ) || archiveEntry.header.getHeaderValue( HEADER_KEY_TYPE ).equals( "response" ) ) {
                    String url = archiveEntry.header.getUrl();
                    if( url.matches( "^http.*$" ) ) {
                        HttpParser.parseHeaders( record, DEFAULT_ENCODING );
                    }
                }
                BufferedInputStream input = new BufferedInputStream( record );
                input.skip( archiveEntry.position );


                byte[] bytes = new byte[ buf.capacity() ];
                int n = input.read( bytes );


                if( n > 0 ) {
                    buf.put( bytes, 0, n );
                }
                return n;
            } catch( Exception e ) {
                log.error( "read(): " + e.toString(), e );
                return -1;
            }
        }
        return 0;

    }

    private ArchiveEntry getEntry(ArchiveRecord record) throws ParseException {
        if( record instanceof ARCRecord ) {
            ARCRecord arcrecord = ( ARCRecord ) record;
            return getEntry(arcrecord);
        } else {
            WARCRecord warcrecord = ( WARCRecord ) record;
            return getEntry(warcrecord);
        }
    }

    private ArchiveEntry getEntry(WARCRecord record) throws ParseException {
        ArchiveRecordHeader header = record.getHeader();
        ArchiveEntry warcEntry = new ArchiveEntry();
        String recordType = ( String ) header.getHeaderValue( HEADER_KEY_TYPE );
        if( header.getHeaderFieldKeys().contains( HEADER_KEY_URI ) ) {
            SimpleDateFormat dateformat = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss'Z'" );
            String path = "/" + header.getHeaderValue( HEADER_KEY_URI ).toString().replace( ":", "/" ).replaceAll( "/+", "/" );
            if( path.endsWith( "/" ) ) {
                path = path + "ROOT";
            }
            if( !recordType.equals( "response" ) ) {
                path = path + "_" + recordType.toUpperCase();
            }
            warcEntry.header = header;
            warcEntry.offset = header.getOffset();
            warcEntry.position = 0;
            warcEntry.lastModified =  ( dateformat.parse( header.getHeaderValue( HEADER_KEY_DATE ).toString() ).getTime() / 1000L );
            warcEntry.path = path;
            return warcEntry;
        } else {
            return null;
        }

    }


    private ArchiveEntry getEntry(ARCRecord record) throws ParseException {
        ArchiveRecordHeader header = record.getHeader();
        ArchiveEntry arcEntry = new ArchiveEntry();
        SimpleDateFormat dateformat = new SimpleDateFormat( "yyyyMMddHHmmss" );
        String path = "/" + header.getUrl().replace( ":", "/" ).replaceAll( "/+", "/" );
        if( path.endsWith( "/" ) ) {
            path = path + "ROOT";
        }
        arcEntry.header = header;
        arcEntry.offset = header.getOffset();
        arcEntry.position = 0;
        arcEntry.lastModified =  ( dateformat.parse( header.getDate() ).getTime() / 1000L );
        arcEntry.path = path;
        return arcEntry;
    }





    public static void main( String[] args ) {
        if( args.length < 1 ) {
            System.out.println( "Must specify Archive file" );
            System.exit( -1 );
        }


        File warcFile = new File( args[0]);
        File outdir = new File( args[1]);



        try {
            Archive archive = new Archive(warcFile);
            archive.unpack(outdir);
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }


}
