package dk.statsbiblioteket.scape.arcunpacker.mapred;

import dk.statsbiblioteket.scape.arcunpacker.ArcRecord;
import dk.statsbiblioteket.scape.arcunpacker.HadoopArcRecord;
import dk.statsbiblioteket.scape.arcunpacker.HeritrixWrapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/26/12
 * Time: 10:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class ArcRecordReader implements RecordReader<Text,HadoopArcRecord>{


    private HeritrixWrapper archiveReaderDelegate;
    private HadoopArcRecord record;

    public ArcRecordReader(JobConf jobConf, FileSplit fileSplit) throws IOException {
        Path path = fileSplit.getPath();
        FileSystem fileSystem = path.getFileSystem(jobConf);
        FSDataInputStream fileInputStream = fileSystem.open(path);
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        long fileLength = fileStatus.getLen();
        archiveReaderDelegate = new HeritrixWrapper(path.getName(),fileInputStream,fileLength);

    }


    @Override
    public boolean next(Text key, HadoopArcRecord value) throws IOException {
        try {
            boolean more = archiveReaderDelegate.nextKeyValue();
            key.set(archiveReaderDelegate.getCurrentID());
            archiveReaderDelegate.getCurrentArcRecord(value);
            return more;
        } catch (Exception e){
            System.out.println(e);
            return false;
        }
    }



    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public HadoopArcRecord createValue() {
        if (record == null){
            record = new HadoopArcRecord();
        } else {
            record.clear();
        }
        return record;
    }


    @Override
    public long getPos() throws IOException {
        return archiveReaderDelegate.getPosition();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        return  ((archiveReaderDelegate.getPosition()+0.0f)/archiveReaderDelegate.getFileLength());
    }
}
