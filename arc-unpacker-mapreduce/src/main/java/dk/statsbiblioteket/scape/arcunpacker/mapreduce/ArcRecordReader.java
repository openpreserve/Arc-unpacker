/*
 *  Copyright 2012 The SCAPE Project Consortium.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */
package dk.statsbiblioteket.scape.arcunpacker.mapreduce;

import dk.statsbiblioteket.scape.arcunpacker.ArcRecord;
import dk.statsbiblioteket.scape.arcunpacker.HadoopArcRecord;
import dk.statsbiblioteket.scape.arcunpacker.HeritrixWrapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author shsdev https://github.com/shsdev
 * @version 0.2
 */
public final class ArcRecordReader extends RecordReader<Text, HadoopArcRecord> {


    private HeritrixWrapper archiveReaderDelegate;
    private Text key;
    private HadoopArcRecord value;

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        //throw new UnsupportedOperationException("Unused.");

        FileSplit fileSplit = (FileSplit) is;
        try {
            Path path = fileSplit.getPath();

            FileSystem fileSystem = path.getFileSystem(tac.getConfiguration());

            FSDataInputStream fileInputStream = fileSystem.open(path);
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            long fileLength = fileStatus.getLen();

            archiveReaderDelegate = new HeritrixWrapper(path.getName(),fileInputStream,fileLength);
            key = new Text();
            value = new HadoopArcRecord();

        } catch (IOException ex) {
            Logger.getLogger(ArcRecordReader.class.getName()).log(Level.SEVERE, null, ex);
        }



    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return archiveReaderDelegate.nextKeyValue();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        key.set(archiveReaderDelegate.getCurrentID());
        return key;
    }

    @Override
    public HadoopArcRecord getCurrentValue() throws IOException, InterruptedException {
        archiveReaderDelegate.getCurrentArcRecord(value);
        return value;
    }


    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        return  ((archiveReaderDelegate.getPosition()+0.0f)/archiveReaderDelegate.getFileLength());
    }

}
