package dk.statsbiblioteket.scape.arcunpacker;

import org.archive.io.ArchiveRecordHeader;

/**
* Created by IntelliJ IDEA.
* User: abr
* Date: 4/25/12
* Time: 11:22 AM
* To change this template use File | Settings | File Templates.
*/
public class ArchivedResource {
    private ArchiveRecordHeader header;
    private boolean directory = false;
    private long lastModified;
    private long offset = -1L;
    private int position = -1;
    private String path;
    private String mimeType;

    public long getSizeInBytes(){
        return header.getLength()-header.getContentBegin();
    }
    public String getResourceURL(){
        return header.getUrl();
    }

    public boolean isDirectory() {
        return directory;
    }

    public long getLastModified() {
        return lastModified;
    }

    public long getOffset() {
        return offset;
    }

    public int getPosition() {
        return position;
    }

    public String getPath() {
        return path;
    }

    public String getMimeType() {
        return mimeType;
    }
}
