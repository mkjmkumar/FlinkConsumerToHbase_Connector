import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.fs.Path;

public class HBaseConfigUtil {

    public static Configuration getHBaseConfiguration() {
        Configuration configuration = HBaseConfiguration.create();

        configuration.addResource(new Path("/usr/hdp/2.5.6.0-40/hbase/conf/hbase-site.xml"));
        configuration.addResource(new Path("/usr/hdp/2.5.6.0-40/hbase/conf/core-site.xml"));
        return configuration;
    }
}