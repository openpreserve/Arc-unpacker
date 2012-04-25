package dk.statsbiblioteket.scape.arcunpacker;


import java.io.*;
import java.nio.ByteBuffer;
import java.text.ParseException;

import com.google.common.io.Files;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;

public class WebArchiveFile {

    private static final Log log = LogFactory.getLog(WebArchiveFile.class );


    private File archiveFile;

    public WebArchiveFile(File archiveFile) {
        this.archiveFile = archiveFile;
    }

    public void unpack(File outdir,UnpackConfig unpackConfig) throws IOException, ParseException {



        int filesExtracted = 0;
        int blocks = 0;

        ArchiveReader archivereader = ArchiveReaderFactory.get(archiveFile);
        for (ArchiveRecord record : archivereader) {

            ArchivedResource archivedResource = convertRecordToResource(record);
            if (archivedResource != null) {

                if (!archivedResource.isDirectory()) {
                    try {
                        unpackResource(archivedResource,outdir, unpackConfig);
                        filesExtracted++;
                        blocks += archivedResource.getBlocks();
                    } catch (ExcludedResourceContingency excludedResourceContingency) {
                        //Excluded resources are expected
                    }
                }
            }
        }
        log.info( "WebArchiveFile archiveFile " + archiveFile + " structure evaluated: " + filesExtracted + " URIs, " + blocks + " blocks." );
    }

    private void unpackResource(ArchivedResource archivedResource, File outdir, UnpackConfig config)
            throws IOException, ExcludedResourceContingency, ParseException {
        if (!outputThisResource(archivedResource,config)){
            throw new ExcludedResourceContingency();
        }
        ByteBuffer contents = readEntry(archivedResource);
        File outfile = new File(outdir, archivedResource.getEncodedPath(config.getNaming()));
        writeContentToFile(outfile, contents);
        outfile.setLastModified(archivedResource.getServerReportedLastModified().getTime());
    }

    private void writeContentToFile(File outfile, ByteBuffer contents) throws IOException {
        outfile.getParentFile().mkdirs();
        System.out.println(outfile.getAbsolutePath());
        if (!outfile.createNewFile()){
            throw new IOException("File "+outfile.getAbsolutePath()+" already exists, aborting");
        }
        Files.write(contents.array(), outfile);
    }

    private boolean outputThisResource(ArchivedResource archivedResource, UnpackConfig config) throws IOException {
        if (archivedResource.isUnprintable()){
            return false;
        }
        int returnCode = archivedResource.getHttpReturnCode();
        if (returnCode >= config.getMinResponseCode() && returnCode <= config.getMaxResponseCode()){
            return true;
        }
        return false;
    }

    private ByteBuffer readEntry(ArchivedResource archivedResource) throws IOException {
        InputStream input = archivedResource.getContents();
        ByteBuffer buf = ByteBuffer.allocate((int) archivedResource.getFileSizeInBytes());
        byte[] bytes = buf.array();
        int n = input.read( bytes );
        return buf;
    }

    private ArchivedResource convertRecordToResource(ArchiveRecord record) throws ParseException {
        if( record instanceof ARCRecord ) {
            return new ArcArchivedResource(( ARCRecord )record,archiveFile);
        } else {
            return new WarcArchivedResourse(( WARCRecord )record,archiveFile);
        }
    }


}
