package eu.scape_project.arcunpacker;


import java.io.*;
import java.nio.ByteBuffer;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/26/12
 * Time: 10:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class ArcRecord {
    private String url = null;
    private String mimeType = null;
    private Date date = null;
    private int httpReturnCode = -1;
    private int length = 0;
    private String type;
    private byte[] contents = new byte[0];
    private String ID;
    private String arcFile;
    private long offsetInArc;

    public void clear(){
        mimeType = null;
        date = null;
        httpReturnCode = -1;
        length = 0;
        arcFile = "";
        offsetInArc = 0;
        //We do not clear contents, to prevent memory thrashing
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setHttpReturnCode(int httpReturnCode) {
        this.httpReturnCode = httpReturnCode;
    }

    public void setContents(InputStream input, int length) throws IOException {
        this.length = length;
        ensureSpace();
        int offset = 0;
        while (true){
            int read = input.read(contents, offset, length-offset);
            if (read > 0){
                offset +=read;
            } else {
                this.length = offset;
                break;
            }
        }

    }

    public void setType(String type) {
        this.type = type;
    }

    protected synchronized void ensureSpace() {
        if (length > contents.length){
            //System.out.println("Upgrading array from "+contents.length+" to "+(2*length));
            contents = new byte[2*length];
        }
    }


    public String getUrl() {
        return url;
    }

    public String getMimeType() {
        return mimeType;
    }

    public Date getDate() {
        return date;
    }

    public int getHttpReturnCode() {
        return httpReturnCode;
    }

    public int getLength() {
        return length;
    }

    public InputStream getContents(){
        return new ByteArrayInputStream(contents,0,length);
    }

    public String getType() {
        return type;
    }

    protected byte[] getBytes(){
        return contents;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getID() {
        return ID;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public String getArcFile() {
        return arcFile;
    }

    public void setArcFile(String arcFile) {
        this.arcFile = arcFile;
    }

    public long getOffsetInArc() {
        return offsetInArc;
    }

    public void setOffsetInArc(long offsetInArc) {
        this.offsetInArc = offsetInArc;
    }
}
