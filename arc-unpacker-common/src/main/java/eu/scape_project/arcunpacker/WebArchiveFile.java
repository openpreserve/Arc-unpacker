package eu.scape_project.arcunpacker;


import java.io.*;
import java.text.ParseException;

public class WebArchiveFile {

    private String archiveID;
    private UnpackConfig unpackConfig;
    private InputStream contents;
    private HeritrixWrapper archivereader;


    public WebArchiveFile(String archiveID,File contents,long length, UnpackConfig unpackConfig) throws IOException {

        this.archiveID = archiveID;
        this.unpackConfig = unpackConfig;
        this.contents = new FileInputStream(contents);
        archivereader = new HeritrixWrapper(archiveID,this.contents,length);//ArchiveReaderFactory.getARC(archiveID, contents, true);

    }



    public ArcRecord next() throws IOException, ParseException {
        boolean more = archivereader.nextKeyValue();
        if (!more){
            return null;
        }

        ArcRecord value = new ArcRecord();
        archivereader.getCurrentArcRecord(value);

        if (!outputThisResource(value)){
            value = next();
        }
        return value;

    }


    private boolean outputThisResource(ArcRecord archivedResource) throws IOException {
        if (archivedResource == null){
            return true;
        }
        int returnCode = archivedResource.getHttpReturnCode();
        if (returnCode >= unpackConfig.getMinResponseCode() && returnCode <= unpackConfig.getMaxResponseCode()){
            return true;
        }
        return false;
    }



}
