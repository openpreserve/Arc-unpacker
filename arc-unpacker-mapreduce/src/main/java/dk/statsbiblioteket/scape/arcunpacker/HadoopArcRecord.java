package dk.statsbiblioteket.scape.arcunpacker;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 6/22/12
 * Time: 11:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class HadoopArcRecord extends ArcRecord implements Writable{

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.getUrl());
        out.writeUTF(this.getMimeType());
        out.writeLong(this.getDate().getTime());
        out.writeInt(this.getHttpReturnCode());
        out.writeUTF(this.getType());
        out.write(this.getLength());
        out.write(this.getBytes(),0,this.getLength());
    }

    public void readFields(DataInput in) throws IOException {
        this.setUrl(in.readUTF());
        this.setMimeType(in.readUTF());
        this.setDate(new Date(in.readLong()));
        this.setHttpReturnCode(in.readInt());
        this.setType(in.readUTF());
        this.setLength(in.readInt());
        this.ensureSpace();
        in.readFully(this.getBytes(), 0, this.getLength());
    }

}
