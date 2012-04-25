package dk.statsbiblioteket.scape.arcunpacker;

import org.apache.commons.cli.*;
import java.io.File;


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
            WebArchiveFile webArchiveFile = new WebArchiveFile(warcFile);
            UnpackConfig unpackConfig = new UnpackConfig(minResponse,maxResponse,naming);
            webArchiveFile.unpack(outdir,unpackConfig);
        } catch( Exception e ) {
            System.err.println("Failed to Unpack");
            e.printStackTrace();
            System.exit(1);
        }
    }


}
