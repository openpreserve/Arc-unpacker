package dk.statsbiblioteket.scape.arcunpacker;


import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HeaderElement;
import org.apache.commons.httpclient.HttpParser;
import org.apache.commons.httpclient.NameValuePair;
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
    private int minReturnCode=0,maxReturnCode=1000;
    private Naming naming = Naming.URL;

    public void setMinReturnCode(int minReturnCode) {
        this.minReturnCode = minReturnCode;
    }

    public void setMaxReturnCode(int maxReturnCode) {
        this.maxReturnCode = maxReturnCode;
    }

    public void setNaming(Naming naming) {
        this.naming = naming;
    }

    public class ArchiveEntry {
        ArchiveRecordHeader header;
        boolean isDirectory = false;
        long lastModified;
        long offset = -1L;
        int position = -1;
        String path;
        String mimeType;
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
                            files++;
                        }
                    }

                }
            }
            log.info( "Archive file " + file + " structure evaluated: " + files + " URIs, " + blocks + " blocks." );
        } catch( Exception e ) {
            log.error( "ArchiveUnpacker(): " + e.getMessage(), e );
        }
    }

    private void writeFile(ArchiveEntry archiveEntry, ByteBuffer buf) throws IOException {
        File outfile = new File(outdir, archiveEntry.path);
        outfile.getParentFile().mkdirs();
        System.out.println(outfile.getAbsolutePath());
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
                                        if (!(returnCode >= minReturnCode && returnCode <= maxReturnCode)){
                                            return -1;
                                        }
                                    }
                                }
                            }
                        }
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

            if( !recordType.equals( "response" ) ) {
                path = path + "_" + recordType.toUpperCase();
            }
            warcEntry.header = header;
            warcEntry.offset = header.getOffset();
            warcEntry.position = 0;
            warcEntry.lastModified =  ( dateformat.parse( header.getHeaderValue( HEADER_KEY_DATE ).toString() ).getTime() / 1000L );
            warcEntry.mimeType = header.getMimetype().replaceAll("/","-");
            switch (naming){
                case URL:
                    if( path.endsWith( "/" ) ) {
                        path = path + "ROOT";
                    }
                    warcEntry.path = path;
                    break;
                case MD5:
                    warcEntry.path = DigestUtils.md5Hex(path)+":"+warcEntry.mimeType;
                    break;
                case OFFSET:
                    warcEntry.path = file.getName()+":"+warcEntry.offset+":"+warcEntry.mimeType;
                    break;
            }
            return warcEntry;
        } else {
            return null;
        }

    }

    private ArchiveEntry getEntry(ARCRecord record) throws ParseException {
        if (!(record.getStatusCode() >= minReturnCode && record.getStatusCode() <=maxReturnCode)){
            return null;
        }
        ArchiveRecordHeader header = record.getHeader();
        ArchiveEntry arcEntry = new ArchiveEntry();
        SimpleDateFormat dateformat = new SimpleDateFormat( "yyyyMMddHHmmss" );
        String path = "/" + header.getUrl().replace( ":", "/" ).replaceAll( "/+", "/" );
        arcEntry.header = header;
        arcEntry.offset = header.getOffset();
        arcEntry.position = 0;
        arcEntry.lastModified =  ( dateformat.parse( header.getDate() ).getTime() / 1000L );
        arcEntry.mimeType = header.getMimetype().replaceAll("/","-");
        switch (naming){
            case URL:
                if( path.endsWith( "/" ) ) {
                    path = path + "ROOT";
                }
                arcEntry.path = path;
                break;
            case MD5:
                arcEntry.path = DigestUtils.md5Hex(path)+":"+arcEntry.mimeType;
                break;
            case OFFSET:
                arcEntry.path = file.getName()+":"+arcEntry.offset+":"+arcEntry.mimeType;
                break;
        }
        return arcEntry;
    }


    private static Options options;

    private static final String INFILE_OPTION = "f";

    private static final String OUTDIR_OPTION = "o";

    private static final String DEFAULT_OUTPUT_FILE = ".";

    private static final String MINRESPONSE_OPTION = "minResp";

    private static int DEFAULT_MINRESPONSE = 0;

    private static final int DEFAULT_MAXRESPONSE = 1000;

    private static final String MAXRESPONSE_OPTION = "maxResp";

    private static final String NAME_OPTION = "naming";

    static{
        options = new Options();
        options.addOption(INFILE_OPTION, true,
                          "Data file location");
        options.addOption(OUTDIR_OPTION, true,
                          "Directory to extract to. " + DEFAULT_OUTPUT_FILE + " by default ");
        options.addOption(MINRESPONSE_OPTION,true, "Minimum http response code. "+DEFAULT_MINRESPONSE+" by default");
        options.addOption(MAXRESPONSE_OPTION,true, "Maximum http response code. "+DEFAULT_MAXRESPONSE+" by default");
        options.addOption(NAME_OPTION,true, "Naming. One of "+Naming.MD5+","+Naming.OFFSET+","+Naming.URL+". "+Naming.URL+" by default");

    }


    public static void main( String[] args ) {

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.err.println("Error parsing arguments");
            e.printStackTrace();
            System.exit(1);
            return;
        }

        for (String arg : args) {
            System.out.println(arg);
        }

        File warcFile;
        if (cmd.hasOption(INFILE_OPTION)){
            warcFile = new File(cmd.getOptionValue(INFILE_OPTION));
        } else {
            System.err.println("Must specify an input file");
            System.exit(1);
            return;

        }
        File outdir;
        if (cmd.hasOption(OUTDIR_OPTION)){
            outdir = new File(cmd.getOptionValue(OUTDIR_OPTION));
        } else {
            outdir = new File(DEFAULT_OUTPUT_FILE);
        }
        int minResponse;
        if (cmd.hasOption(MINRESPONSE_OPTION)){
            minResponse = Integer.parseInt(cmd.getOptionValue(MINRESPONSE_OPTION));
        } else {
            minResponse = DEFAULT_MINRESPONSE;
        }
        int maxResponse;
        if (cmd.hasOption(MAXRESPONSE_OPTION)){
            maxResponse = Integer.parseInt(cmd.getOptionValue(MAXRESPONSE_OPTION));
        } else {
            maxResponse = DEFAULT_MAXRESPONSE;
        }
        Naming naming;
        if (cmd.hasOption(NAME_OPTION)){
            naming = Naming.valueOf(cmd.getOptionValue(NAME_OPTION));
        } else {
            naming = Naming.MD5;
        }







        try {
            Archive archive = new Archive(warcFile);
            archive.setMaxReturnCode(maxResponse);
            archive.setMinReturnCode(minResponse);
            archive.setNaming(naming);
            archive.unpack(outdir);
        } catch( Exception e ) {
            System.err.println("Failed to Unpack");
            e.printStackTrace();
            System.exit(1);
            return;

        }
    }


    public enum Naming {
        URL,OFFSET, MD5;
    }
}
