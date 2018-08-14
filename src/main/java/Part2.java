import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class Part2 {

    public static void main( String[] args ) throws IOException
    {
        String url = args[0];   //File URL from where to transfer file
        String dst = args[1];   //HDFS directory where the file will be transferred in form of a text file

        //String urlPrefix = "http://corpus.byu.edu/wikitext-samples/";
        String urlZipPart = url.substring(url.indexOf("t/")+2);

        String hadoopURL = dst + urlZipPart;

        URL urlReal = new URL(url);
        URLConnection connection = urlReal.openConnection();
        String redirect = connection.getHeaderField("Location");
        if(redirect != null) {
            connection = new URL(redirect).openConnection();
        }

        InputStream a = connection.getInputStream();

        ZipInputStream bf = new ZipInputStream(a);

        //Copying the Contents of the zip file into the Hadoop File System
        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

        ZipEntry entry = bf.getNextEntry();

        // iterates over entries in the zip file
        while (entry != null) {
            FileSystem fs = FileSystem.get(URI.create(hadoopURL), conf);
            if (!entry.isDirectory()) {
                // if the entry is a file, extracts it
                OutputStream out = fs.create(new Path(args[1]+entry.getName()), new Progressable() {
                    public void progress() {
                        System.out.print(".");
                    }
                });
                IOUtils.copyBytes(bf, out, 4096, false);
            }
            entry = bf.getNextEntry();
        }

        IOUtils.closeStream(bf);
        IOUtils.closeStream(a);

    }
}
