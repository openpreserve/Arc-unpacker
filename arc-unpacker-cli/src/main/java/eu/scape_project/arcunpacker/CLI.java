package eu.scape_project.arcunpacker;

import com.google.common.io.Files;

import eu.scape_project.arcunpacker.ArcRecord;
import eu.scape_project.arcunpacker.UnpackConfig;
import eu.scape_project.arcunpacker.WebArchiveFile;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
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
    private static Option REPORTS_OPTION = new Option("reports",true, "Create reports with header information and URL information from the server");


    static{
        options = new Options();
        INFILE_OPTION.setRequired(true);
        options.addOption(INFILE_OPTION);
        options.addOption(OUTDIR_OPTION);
        options.addOption(MINRESPONSE_OPTION);
        options.addOption(MAXRESPONSE_OPTION);
        options.addOption(NAME_OPTION);
        options.addOption(REPORTS_OPTION);

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
        String reportDirString = cmd.getOptionValue(REPORTS_OPTION.getOpt());
        File reportDir = null;
        if (reportDirString != null){
            reportDir = new File(reportDirString);
            reportDir.mkdirs();
        }
        UnpackConfig.Naming naming =
                UnpackConfig.Naming.valueOf(cmd.getOptionValue(NAME_OPTION.getOpt(), UnpackConfig.Naming.MD5.name()));

        try {
            UnpackConfig unpackConfig = new UnpackConfig(minResponse,maxResponse,naming);
            WebArchiveFile webArchiveFile = new WebArchiveFile(warcFile.getName(),warcFile,warcFile.length(),unpackConfig);
            ArcRecord resource = webArchiveFile.next();
            while (resource != null){
                unpackResource(resource,outdir,unpackConfig);
                if (reportDir != null){
                    unpackReport(resource,reportDir,unpackConfig);
                }
                resource = webArchiveFile.next();
            }

        } catch( Exception e ) {
            System.err.println("Failed to Unpack");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void unpackReport(ArcRecord resource, File reportDir, UnpackConfig config) throws IOException {
        ByteBuffer contents = readReport(resource);
        File outfile = new File(reportDir, getEncodedPath(resource, config.getNaming())+".report");
        writeContentToFile(outfile, contents);
        outfile.setLastModified(resource.getDate().getTime());
    }

    private static ByteBuffer readReport(ArcRecord resource) {
        StringWriter report = new StringWriter();
        report.append("Arc-file:");
        report.append(resource.getArcFile());
        report.append("\n");

        report.append("ID:");
        report.append(resource.getID());
        report.append("\n");

        report.append("MIME-Type:");
        report.append(resource.getMimeType());
        report.append("\n");


        report.append("Type:");
        report.append(resource.getType());
        report.append("\n");


        report.append("Url:");
        report.append(resource.getUrl());
        report.append("\n");

        report.append("Date:");
        report.append(resource.getDate().toString());
        report.append("\n");

        report.append("Http-Return-Code:");
        report.append(resource.getHttpReturnCode()+"");
        report.append("\n");

        report.append("Length:");
        report.append(resource.getLength()+"");
        report.append("\n");

        report.append("Offset:");
        report.append(resource.getOffsetInArc()+"");
        report.append("\n");


        return ByteBuffer.wrap(report.toString().getBytes());
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
