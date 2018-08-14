import java.io.DataInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class Part1 {
    public static void main(String[] args) throws Exception {
        ArrayList<String> urls = new ArrayList<String>();
        //User argument for hadoop cluster link where to decompress the text files.
        String hadoopUrl = args[0];

        //List of names of compressed files
        urls.add("20417.txt.bz2");
        urls.add("5000-8.txt.bz2");
        urls.add("132.txt.bz2");
        urls.add("1661-8.txt.bz2");
        urls.add("972.txt.bz2");
        urls.add("19699.txt.bz2");

        String urlPrefix = "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/";
        //to upload the books in urls list to a HDFS directory path(User argument args[0])
        for(String urlIter: urls) {
            String localSrc = urlPrefix + urlIter;
            String dst = hadoopUrl + urlIter;

            URL url = new URL(localSrc);
            URLConnection connection = url.openConnection();
            String redirect = connection.getHeaderField("Location");
            if (redirect != null){
                connection = new URL(redirect).openConnection();
            }
            InputStream in = connection.getInputStream();

            Configuration conf = new Configuration();
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out = fs.create(new Path(dst), new Progressable() {
                public void progress() {
                    System.out.print(".");
                }
            });

            IOUtils.copyBytes(in, out, 4096, true);

            //Decompressing the File from .bz2
            Path inputPath = new Path(dst);
            CompressionCodecFactory factory = new CompressionCodecFactory(conf);
            CompressionCodec codec = factory.getCodec(inputPath);
            if (codec == null) {
                System.err.println("No codec found for " + dst);
                System.exit(1);
            }

            String outputUri =
                    CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());

            in = null;
            OutputStream outDecompress = null;
            try {
                in = codec.createInputStream(fs.open(inputPath));
                outDecompress = fs.create(new Path(outputUri));

                IOUtils.copyBytes(in, outDecompress, conf);

                if(fs.exists(inputPath)) {
                    fs.delete(inputPath, true);
                } else {
                    System.out.println("[Error]: File is not present in your HDFS!!");
                }

            } finally { //cleaning the input and output streams
                IOUtils.closeStream(in);
                IOUtils.closeStream(outDecompress);
                IOUtils.closeStream(out);
            }
        }
    }
}
