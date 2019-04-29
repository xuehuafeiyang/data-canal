package data;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SimpleCanalClientExample {

    private static final String propPath = "conf.properties";
    private static JedisPool redisPool = null;
    static {
        try{
            Properties props = new Properties();
            InputStream in = ClassLoader.getSystemResourceAsStream(propPath);
            props.load(in);

            //创建jedis池配置实例
            JedisPoolConfig config = new JedisPoolConfig();

            //设置池配置项值
            config.setMaxTotal(Integer.valueOf(props.getProperty("jedis.pool.maxActive")));
            config.setMaxIdle(Integer.valueOf(props.getProperty("jedis.pool.maxIdle")));
            config.setMaxWaitMillis(Long.valueOf(props.getProperty("jedis.pool.maxWait")));
            config.setTestOnBorrow(Boolean.valueOf(props.getProperty("jedis.pool.testOnBorrow")));
            config.setTestOnReturn(Boolean.valueOf(props.getProperty("jedis.pool.testOnReturn")));

            //根据配置实例化jedis池
            redisPool = new JedisPool(config, props.getProperty("jedis.pool.host"), Integer.valueOf(props.getProperty("jedis.pool.port")));
            //writePool = new JedisPool(config, props.getProperty("redisWriteURL"), Integer.valueOf(props.getProperty("redisWritePort")));

        }catch (IOException e) {
           // logger.info("redis连接池异常",e);
            e.printStackTrace();
        }
    }
    public static Jedis getJedis(){
        return redisPool.getResource();
    }

    public static void main(String args[]) {

        //AddressUtils.getHostIp()
        // 创建链接
        System.out.println(AddressUtils.getHostIp());
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.44.131",
                11111), "example", "", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmptyCount = 120;
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

         //   System.out.println("empty too many times, exit");
        } finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================&gt; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    insertRedis(rowData.getAfterColumnsList(), entry.getHeader().getSchemaName(),entry.getHeader().getTableName() );
                } else {
                    System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());
                    updateRedis(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList(), entry.getHeader().getSchemaName(),entry.getHeader().getTableName() );
                }
            }
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static final String TABLES_TO_REDIS = "merchant_ctl,merchant_product";
    private static void insertRedis(List<CanalEntry.Column> columns, String database, String table){
        if( !TABLES_TO_REDIS.contains(table) ){
            return;
        }
        Jedis jedis = getJedis();
        Map<String, String> map = new HashMap<>();
        for(CanalEntry.Column column : columns){
            map.put(column.getName(), column.getValue());
        }
        if ("merchant_ctl".equals(table)){
            Map<String, String > mMap = new HashMap<>();
            mMap.put("mch_id", map.get("mch_id"));
            mMap.put("transkey", map.get("transkey"));
            mMap.put("status", map.get("status"));
            jedis.hmset(table+":"+map.get("mch_id"), mMap);
        }
        if ("merchant_product".equals(table)){
            jedis.set(String.format("%s:%s:%s:status", table, map.get("mch_id"), map.get("product_id")), map.get("status"));
        }
        jedis.close();
    }

    private static void updateRedis(List<CanalEntry.Column> columnsBerfore, List<CanalEntry.Column> columns, String database, String table){
        if( !TABLES_TO_REDIS.contains(table) ){
            return;
        }
        Jedis jedis = getJedis();
        Map<String, String> map = new HashMap<>();
        for(CanalEntry.Column column : columns){
            map.put(column.getName(), column.getValue());
        }
        if ("merchant_ctl".equals(table)){
            Map<String, String > mMap = new HashMap<>();
            mMap.put("mch_id", map.get("mch_id"));
            mMap.put("transkey", map.get("transkey"));
            mMap.put("status", map.get("status"));
            jedis.hmset(table+":"+map.get("mch_id"), mMap);
        }
        if ("merchant_product".equals(table)){
            jedis.set(String.format("%s:%s:%s:status", table, map.get("mch_id"), map.get("product_id")), map.get("status"));
        }
        jedis.close();
    }
}
