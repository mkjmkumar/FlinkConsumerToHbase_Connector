
import java.util.Date;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author Mukesh Kumar
 */
public class HBase{        
    private ObjectPool<Connection> pool;
    
    private TableName tableName = TableName.valueOf("kafkaMessages");            
    
    public HBase(ObjectPool<Connection> pool){
        this.pool = pool;
    }   
    
    public void writeIntoHBase(String colFamily, String colQualifier, String value) 
            throws Exception{
        Connection c = null;
        System.out.println(">>>>>");
        System.out.println("Table Name is >>>>>" + tableName + "colFamily>>>" + colFamily + "colQualifier>>>" + colQualifier + "value>>>>" + value );
        c = pool.borrowObject();
        Admin admin = c.getAdmin();
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(
                new HColumnDescriptor("messageJava")));
            System.out.println("Table does not exists, CREATED");
        }
        
        System.out.println("Instantiating Table class");
        // Instantiating Table class
        Table t = c.getTable(tableName);
        // Instantiating Put class. Accepts a row key.
        TimeStamp ts = new TimeStamp(new Date());        
        Date d = ts.getDate();        
        Put p = new Put(Bytes.toBytes(d.toString()));
        // adding values using addColumn() method. 
        // Accepts column family name, qualifier/row name ,value.                
        p.addColumn(
            Bytes.toBytes(colFamily),
            Bytes.toBytes(colQualifier),
            Bytes.toBytes(value));
        // Saving the put Instance to the Table.
        t.put(p);
        // closing Table & Connection
        t.close();
        pool.returnObject(c);   
    }    
}