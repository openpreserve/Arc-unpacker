package org.archive.io.warc;

import com.google.common.io.CountingInputStream;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.GZIPMembersInputStream;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCReaderFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 9/26/12
 * Time: 1:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class ARCbetterReaderFactory extends ARCReaderFactory implements WARCConstants {

    @Override
    public ArchiveReader getArchiveReader(String arc, InputStream is, boolean atFirstRecord) throws IOException {
        final InputStream stream = new BufferedInputStream(is);
        if (ARCReaderFactory.isARCSuffix(arc)) {
            return getARC(arc, stream, atFirstRecord);
        } else if (WARCReaderFactory.isWARCSuffix(arc)) {
            return getWARC(arc, stream, atFirstRecord);
        }
        throw new IOException("Unknown extension (Not ARC nor WARC): " + arc);
    }

    private ArchiveReader getWARC(String arc, InputStream stream, boolean atFirstRecord) throws IOException {
        if (arc.toLowerCase().endsWith(COMPRESSED_WARC_FILE_EXTENSION)){
            //compressed
            return new CompressedWARCReader(arc, stream, atFirstRecord);
        } else {
            //uncompressed
            return new UncompressedWARCReader(arc,stream);
        }
    }

    public  ArchiveReader getARC(final String s, InputStream is,
                                       final boolean atFirstRecord)
            throws IOException {
        is = new CountingInputStream(is);
        if (s.toLowerCase().endsWith(DOT_COMPRESSED_ARC_FILE_EXTENSION)){
            //compressed
            return new CompressedARCReader(s, is, atFirstRecord);
        } else {
            //uncompressed
            return new UncompressedARCReader(s,is);
        }

    }

    /**
     * Uncompressed WARC file reader.
     * @author stack
     */
    public class UncompressedWARCReader extends WARCReader {
        /**
         * Constructor.
         * @param f Uncompressed arcfile to read.
         * @throws IOException
         */
        public UncompressedWARCReader(final File f)
        throws IOException {
            this(f, 0);
        }

        /**
         * Constructor.
         *
         * @param f Uncompressed file to read.
         * @param offset Offset at which to position Reader.
         * @throws IOException
         */
        public UncompressedWARCReader(final File f, final long offset)
        throws IOException {
            // File has been tested for existence by time it has come to here.
            setIn(new CountingInputStream(getInputStream(f, offset)));
            getIn().skip(offset);
            initialize(f.getAbsolutePath());
        }

        /**
         * Constructor.
         *
         * @param f Uncompressed file to read.
         * @param is InputStream.
         */
        public UncompressedWARCReader(final String f, final InputStream is) {
            // Arc file has been tested for existence by time it has come
            // to here.
            setIn(new CountingInputStream(is));
            initialize(f);
        }
    }

    /**
     * Compressed WARC file reader.
     *
     * @author stack
     */
    public class CompressedWARCReader extends WARCReader {
        /**
         * Constructor.
         *
         * @param f Compressed file to read.
         * @throws IOException
         */
        public CompressedWARCReader(final File f) throws IOException {
            this(f, 0);
        }

        /**
         * Constructor.
         *
         * @param f Compressed arcfile to read.
         * @param offset Position at where to start reading file.
         * @throws IOException
         */
        public CompressedWARCReader(final File f, final long offset)
                throws IOException {
            // File has been tested for existence by time it has come to here.
            setIn(new GZIPMembersInputStream(getInputStream(f, offset)));
            ((GZIPMembersInputStream)getIn()).compressedSeek(offset);
            setCompressed((offset == 0)); // TODO: does this make sense?!?!
            initialize(f.getAbsolutePath());
        }

        /**
         * Constructor.
         *
         * @param f Compressed arcfile.
         * @param is InputStream to use.
         * @param atFirstRecord
         * @throws IOException
         */
        public CompressedWARCReader(final String f, final InputStream is,
            final boolean atFirstRecord)
        throws IOException {
            // Arc file has been tested for existence by time it has come
            // to here.
            setIn(new GZIPMembersInputStream(is));
            setCompressed(true);
            initialize(f);
            // TODO: Ignore atFirstRecord. Probably doesn't apply in WARC world.
        }

        /**
         * Get record at passed <code>offset</code>.
         *
         * @param offset Byte index into file at which a record starts.
         * @return A WARCRecord reference.
         * @throws IOException
         */
        public WARCRecord get(long offset) throws IOException {
            cleanupCurrentRecord();
            ((GZIPMembersInputStream)getIn()).compressedSeek(offset);
            return (WARCRecord) createArchiveRecord(getIn(), offset);
        }

        public Iterator<ArchiveRecord> iterator() {
            /**
             * Override ArchiveRecordIterator so can base returned iterator on
             * GzippedInputStream iterator.
             */
            return new ArchiveRecordIterator() {
                private GZIPMembersInputStream gis =
                    (GZIPMembersInputStream)getIn();

                private Iterator<GZIPMembersInputStream> gzipIterator = this.gis.memberIterator();

                protected boolean innerHasNext() {
                    return this.gzipIterator.hasNext();
                }

                protected ArchiveRecord innerNext() throws IOException {
                    // Get the position before gzipIterator.next moves
                    // it on past the gzip header.
                    InputStream is = (InputStream) this.gzipIterator.next();
                    return createArchiveRecord(is, Math.max(gis.getCurrentMemberStart(), gis.getCurrentMemberEnd()));
                }
            };
        }

        protected void gotoEOR(ArchiveRecord rec) throws IOException {
            long skipped = 0;
            while (getIn().read()>-1) {
                skipped++;
            }
            if(skipped>4) {
                System.err.println("unexpected extra data after record "+rec);
            }
            return;
        }
    }


}
