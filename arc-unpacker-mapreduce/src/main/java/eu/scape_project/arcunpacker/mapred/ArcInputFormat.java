package eu.scape_project.arcunpacker.mapred;

import eu.scape_project.arcunpacker.ArcRecord;
import eu.scape_project.arcunpacker.HadoopArcRecord;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/26/12
 * Time: 9:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class ArcInputFormat extends FileInputFormat<Text,HadoopArcRecord> {



    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, HadoopArcRecord> getRecordReader(
            InputSplit inputSplit,
            JobConf jobConf,
            Reporter reporter)
            throws IOException {
        reporter.setStatus(inputSplit.toString());
        return new ArcRecordReader(jobConf,(org.apache.hadoop.mapred.FileSplit)inputSplit);
    }


}
