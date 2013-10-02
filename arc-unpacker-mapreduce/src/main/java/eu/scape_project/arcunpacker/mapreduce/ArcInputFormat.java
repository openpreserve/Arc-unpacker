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
package eu.scape_project.arcunpacker.mapreduce;

import eu.scape_project.arcunpacker.HadoopArcRecord;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 *
 * @author shsdev https://github.com/shsdev
 * @version 0.2
 */
public class ArcInputFormat extends FileInputFormat<Text, HadoopArcRecord> {

    @Override
    public RecordReader<Text, HadoopArcRecord> createRecordReader(InputSplit is, TaskAttemptContext tac)
            throws IOException {
        return new ArcRecordReader();
        
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    
}
