package dk.statsbiblioteket.scape.arcunpacker;

import com.google.common.io.Files;
import org.apache.commons.cli.*;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.*;


public class CLI {


    private static final String DEFAULT_OUTPUT_FILE = ".";

    private static final int DEFAULT_MINRESPONSE = 0;

    private static final int DEFAULT_MAXRESPONSE = 1000;

    private static final Option INFILE_OPTION =
            new Option("f", true,"Data file to extract");
    public static final Option OUTDIR_OPTION =
            new Option("o", true,"Directory to extract to. " + "'"+DEFAULT_OUTPUT_FILE+"' by default ");
    public static final Option MINRESPONSE_OPTION =
            new Option("minResp",true, "Minimum http response code. "+DEFAULT_MINRESPONSE+" by default");

    public static final Option MAXRESPONSE_OPTION =
            new Option("maxResp",true, "Maximum http response code. "+DEFAULT_MAXRESPONSE+" by default");

    public static final Option NAME_OPTION =
            new Option("naming",true, "Naming. One of "+ UnpackConfig.Naming.MD5+","+ UnpackConfig.Naming.OFFSET+","+
                                      UnpackConfig.Naming.URL+". "+ UnpackConfig.Naming.URL+" by default");

    private static Options options;
    static{
        options = new Options();
        INFILE_OPTION.setRequired(true);
        options.addOption(INFILE_OPTION);
        options.addOption(OUTDIR_OPTION);
        options.addOption(MINRESPONSE_OPTION);
        options.addOption(MAXRESPONSE_OPTION);
        options.addOption(NAME_OPTION);

    }

    public static void printUsage(){
        final HelpFormatter usageFormatter = new HelpFormatter();
        usageFormatter.printHelp("arc-unpack",options,true);
    }


    public static void main( String[] args ) {

        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.err.println("Error parsing arguments");
            printUsage();
            System.exit(1);
            return;
        }

        File warcFile;
        if (cmd.hasOption(INFILE_OPTION.getOpt())){
            warcFile = new File(cmd.getOptionValue(INFILE_OPTION.getOpt()));
        } else {
            System.err.println("Must specify an input file");
            printUsage();
            System.exit(1);
            return;

        }

        File outdir = new File(cmd.getOptionValue(OUTDIR_OPTION.getOpt(), DEFAULT_OUTPUT_FILE));
        int minResponse = Integer.parseInt(cmd.getOptionValue(MINRESPONSE_OPTION.getOpt(), DEFAULT_MINRESPONSE + ""));
        int maxResponse = Integer.parseInt(cmd.getOptionValue(MAXRESPONSE_OPTION.getOpt(), DEFAULT_MAXRESPONSE + ""));
        UnpackConfig.Naming naming =
                UnpackConfig.Naming.valueOf(cmd.getOptionValue(NAME_OPTION.getOpt(), UnpackConfig.Naming.MD5.name()));

        try {
            UnpackConfig unpackConfig = new UnpackConfig(minResponse,maxResponse,naming);
            WebArchiveFile webArchiveFile = new WebArchiveFile(warcFile.getName(),warcFile,warcFile.length(),unpackConfig);
            ArcRecord resource = webArchiveFile.next();
            while (resource != null){
                unpackResource(resource,outdir,unpackConfig);
                resource = webArchiveFile.next();
            }

        } catch( Exception e ) {
            System.err.println("Failed to Unpack");
            e.printStackTrace();
            System.exit(1);
        }
    }



    private static void unpackResource(ArcRecord archivedResource, File outdir, UnpackConfig config)
            throws IOException, java.text.ParseException {
        ByteBuffer contents = readEntry(archivedResource);
        File outfile = new File(outdir, getEncodedPath(archivedResource,config.getNaming()));
        writeContentToFile(outfile, contents);
        outfile.setLastModified(archivedResource.getDate().getTime());
    }



    private static void writeContentToFile(File outfile, ByteBuffer contents) throws IOException {
        outfile.getParentFile().mkdirs();
        System.out.println(outfile.getAbsolutePath());
        if (!outfile.createNewFile()){
            throw new IOException("File "+outfile.getAbsolutePath()+" already exists, aborting");
        }
        Files.write(contents.array(), outfile);
    }



    private static ByteBuffer readEntry(ArcRecord archivedResource) throws IOException {
        InputStream input = archivedResource.getContents();
        ByteBuffer buf = ByteBuffer.allocate((int) archivedResource.getLength());
        byte[] bytes = buf.array();
        int n = input.read( bytes );
        return buf;
    }


    public static String getEncodedPath(ArcRecord record, UnpackConfig.Naming pathNamingScheme) {

        String path = "/" + record.getUrl().replace(":", "/").replaceAll("/+",
                                                                         "/");

        switch (pathNamingScheme){
            case URL:
                if( path.endsWith( "/" ) ) {
                    path = path + "ROOT";
                }

                break;
            case MD5:
                path = DigestUtils.md5Hex(path)+":"+ record.getMimeType();
                break;
            case OFFSET:
                path = record.getArcFile()+":"+ record.getOffsetInArc()+":"+ record.getMimeType().split(";")[0];
                break;
        }
        path = path.replace("/", "-");
        return path;
    }



}
