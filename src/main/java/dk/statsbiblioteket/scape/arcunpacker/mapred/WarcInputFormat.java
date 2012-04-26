package dk.statsbiblioteket.scape.arcunpacker.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/26/12
 * Time: 9:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class WarcInputFormat extends FileInputFormat<Text,WarcRecord> {



    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, WarcRecord> getRecordReader(
            InputSplit inputSplit,
            JobConf jobConf,
            Reporter reporter)
            throws IOException {
        reporter.setStatus(inputSplit.toString());
        return new WarcRecordReader(jobConf,(org.apache.hadoop.mapred.FileSplit)inputSplit);
    }


}
