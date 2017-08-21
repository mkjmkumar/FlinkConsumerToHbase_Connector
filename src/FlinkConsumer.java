
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hbase.client.Connection;
/**
 *
 * @author Mukesh Kumar
 */
public class FlinkConsumer {
    /**
     * @param args the command line arguments
     * --topic imagetext --bootstrap.servers victoria.com:6667
     */
    public static void main(String[] args) {      
        // create execution environment
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	// parse user parameters
	
	ParameterTool parameterTool = ParameterTool.fromArgs(args);
	    // create datastream with the data coming from Kafka
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<>(
                parameterTool.getRequired("topic"), 
                new SimpleStringSchema(), 
                parameterTool.getProperties()));
        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;
            @Override
            public String map(String message) throws Exception {
                HBase hb = new HBase(new GenericObjectPool<Connection>(new ConexionFactory()));
                System.out.println("Ready to Insert in Hbase");
                hb.writeIntoHBase("messageJava", "java", message);
                System.out.println("Message written into HBase.");
                return "Kafka dice: " + message;
            }
        }).print();
        //execute env("Add to Hbase");
        try {
            env.execute();
        } catch (Exception ex) {
            Logger.getLogger(FlinkConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}