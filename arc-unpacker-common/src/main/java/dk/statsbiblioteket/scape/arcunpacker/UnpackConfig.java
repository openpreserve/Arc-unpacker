package dk.statsbiblioteket.scape.arcunpacker;

/**
 * Created by IntelliJ IDEA.
 * User: abr
 * Date: 4/25/12
 * Time: 9:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class UnpackConfig {

    private int minResponseCode;
    private int maxResponseCode;
    private Naming naming;

    public UnpackConfig(int minResponseCode, int maxResponseCode, Naming naming) {
        this.minResponseCode = minResponseCode;
        this.maxResponseCode = maxResponseCode;
        this.naming = naming;
    }

    public int getMinResponseCode() {
        return minResponseCode;
    }

    public int getMaxResponseCode() {
        return maxResponseCode;
    }

    public Naming getNaming() {
        return naming;
    }

    public static enum Naming {
        URL,OFFSET, MD5;
    }
}
