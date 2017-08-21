import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hbase.client.Connection;
/**
 *
 * @author Mukesh Kumar
 */
public class ConexionFactory extends BasePooledObjectFactory<Connection>{    
    // the override method are used internaly by the implementation of ObjectPool
    @Override
    public Connection create() throws Exception {
        return HBaseConexion.getInstance().getConnection();
    }

    @Override
    public PooledObject<Connection> wrap(Connection c) {
        return new DefaultPooledObject<Connection>(c);
    }
}