package test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class OrientDBCluster {
  private static Logger    logger      = Logger.getLogger(OrientDBCluster.class.getName());
  private static final int NUM_WRITERS = 50;
  private static final int NUM_READERS = 1;
  private static final int NUM_RETRIES = 5;
  private int              count;

  public void execute(int serverId, int count) throws Exception {
    this.count = count;
    String confDir = System.getProperty("g1.server.conf.dir");
    if (confDir == null) {
      confDir = "target/classes";
      System.setProperty("g1.server.conf.dir", confDir);
    }

    String databaseUrl = getDatabaseURL(serverId);

    logger.log(Level.INFO, "Starting EXECUTE test against database " + databaseUrl + " count = " + count);

    // start in clustered mode
    // OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.setValue(60000l);
    // OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.setValue(60000l);
    // OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.setValue(60000l);

    System.out.println("\n\nCurrent directory: " + new File(".").getAbsolutePath());

    // start in clustered mode
    OServer server = new OServer();
    server.startup(new FileInputStream("src/test/resources/orientdb-dserver-config-" + serverId + ".xml"));
    server.activate();

    server.getPluginByClass(OHazelcastPlugin.class).waitUntilOnline();

    ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin").close();

    logger.log(Level.INFO, "");
    logger.log(Level.INFO, "");
    logger.log(Level.INFO, "Hit return to start test");
    logger.log(Level.INFO, "");
    // System.in.read();

    ExecutorService executor = Executors.newCachedThreadPool();

    List<Callable<String>> callables = new ArrayList<>();
    for (int x = 0; x < NUM_WRITERS; x++) {
      Writer writer = new Writer(databaseUrl, String.valueOf(x), databaseUrl);
      callables.add(writer);
    }

    List<Reader> readers = new ArrayList<>();
    for (int x = 0; x < NUM_READERS; x++) {
      Reader reader = new Reader(databaseUrl, String.valueOf(x));
      readers.add(reader);
      callables.add(reader);
    }
    registerShutdownHook(readers, server, executor);

    List<Future<String>> futures = executor.invokeAll(callables);
    for (Future<String> future : futures) {
      try {
        logger.info(future.get());
      } catch (Throwable e) {
        logger.log(Level.SEVERE, "Error Occurred", e);
      }
    }

  }

  public void slave(int serverId) throws Exception {
    String confDir = System.getProperty("g1.server.conf.dir");
    if (confDir == null) {
      confDir = "target/classes";
      System.setProperty("g1.server.conf.dir", confDir);
    }

    getDatabaseURL(serverId);

    // start in clustered mode
    OServer server = new OServer();

    server.startup(new FileInputStream("src/test/resources/orientdb-dserver-config-" + serverId + ".xml"));
    server.activate();
    
    server.getPluginByClass(OHazelcastPlugin.class).waitUntilOnline();
    Thread.sleep(15000);
    server.shutdown();
    
  }

  public void create(int servers) throws Exception {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
    OFileUtils.deleteRecursively(new File("servers"));

    String confDir = System.getProperty("g1.server.conf.dir");
    if (confDir == null) {
      confDir = "target/classes";
      System.setProperty("g1.server.conf.dir", confDir);
    }

    String databaseUrl = getDatabaseURL(0);

    System.out.println("Create database: " + databaseUrl);

    ODatabaseDocumentTx database = new ODatabaseDocumentTx(databaseUrl).create();

    OClass personClass = database.getMetadata().getSchema().createClass("Person");
    OClass dataClass = database.getMetadata().getSchema().createClass("Data");
    try {
      personClass.createProperty("id", OType.STRING);
      personClass.createProperty("firstName", OType.STRING);
      personClass.createProperty("lastName", OType.STRING);
      personClass.createProperty("birthday", OType.DATE);
      personClass.createProperty("children", OType.INTEGER);
      personClass.createProperty("id-id", OType.STRING);
      personClass.createProperty("id-type", OType.STRING);
      personClass.createProperty("data", OType.EMBEDDED, dataClass);
      personClass.createProperty("bytes", OType.BINARY);
      
      personClass.createIndex("id_pk", INDEX_TYPE.UNIQUE, "id");
      personClass.createIndex("Name_Index", INDEX_TYPE.NOTUNIQUE, "firstName", "lastName");
      personClass.createIndex("Id_Index", INDEX_TYPE.UNIQUE, "id-id", "id-type");

    } finally {
      database.close();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2)
      System.out.println("Usage: create <serverid>, execute <serverid> [count], slave <serverid>");

    final int serverId = Integer.parseInt(args[1]);

    OrientDBCluster main = new OrientDBCluster();
    if (args[0].equals("create")) {
      main.create(serverId);
    } else if (args[0].equals("execute")) {
      int count = Integer.parseInt(args[2]);
      main.execute(serverId, count);
    } else if (args[0].equals("slave")) {
      main.slave(serverId);
    } else {
      System.out.println("Usage: create <serverid>, execute <serverid> [count], slave <serverid>");
    }
  }

  class Writer implements Callable<String> {
    private int    numWrites = 0;
    private String id;
    private String databaseUrl;
    private String serverId;

    Writer(String databaseUrl, String id, String serverId) {
      this.databaseUrl = databaseUrl;
      this.id = id;
      this.serverId = serverId;
    }

    @Override
    public String call() {
      logger.log(Level.INFO, "Starting Writer...");
      try {

        for (int i = 0; i < count; i++) {
          String id = createRecord(i);
          // for (int j = 0; j < 10; j++)
          // {
          // updateRecord(id);
          // }
          updateRecord(id);
          checkRecord(id);
          // deleteRecord(id);

          //cluster tests
         /* String clusterName = addCluster();
          logger.log(Level.INFO, "Added Cluster [" + clusterName + "]");
          printClusterNames();
          createRecord(i, clusterName);*/
        }
      } catch (RuntimeException e) {
        logger.log(Level.SEVERE, "Writer[" + id + "] caught exception.  Wrote " + numWrites + " records.", e);
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Writer[" + id + "] caught exception.  Wrote " + numWrites + " records.", e);
        throw new RuntimeException(e);
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      } finally {
        logger.info("Writer[" + id + "] stopping");
      }
      return "Writer[" + id + "] wrote " + numWrites + " records";
    }

    private void checkRecord(String id) throws Exception {
      ODatabaseDocumentTx database = null;
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        ODocument document = findById(database, id);
        if (!"test updated".equals(document.field("test"))) {
          throw new Exception("Found an value that was not updated");
        }
      } finally {
        if (database != null && !database.isClosed()) {
          database.close();
        }
      }
    }

    private void updateRecord(String id) throws Throwable {
      ODatabaseDocumentTx database = null;
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        while (numRetries < NUM_RETRIES) {
          try {
            database.begin();
            ODocument document = findById(database, id);
            document.field("test", "test updated");
            database.save(document);
            database.commit();
            break;
          } catch (Throwable e) {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Update Iteration=" + numRetries + ", id=" + id + ", " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES) {
              throw e;
            }
          }
        }
      } finally {
        if (database != null && !database.isClosed()) {
          database.close();
        }
      }
    }

    private void deleteRecord(String id) throws Throwable {
      ODatabaseDocumentTx database = null;
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        while (numRetries < NUM_RETRIES) {
          database.begin();
          try {
            ODocument document = findById(database, id);
            database.delete(document);
            database.commit();
            break;
          } catch (Throwable e) {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Delete Iteration=" + numRetries + ", id=" + id + ", " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES) {
              throw e;
            }
          }
        }
      } finally {
        if (database != null && !database.isClosed()) {
          database.close();
        }
      }
    }

    private String createRecord(int i) throws Throwable {
      ODatabaseDocumentTx database = null;
      String ID = getServerId() + "." + getId() + "-" + UUID.randomUUID().toString();
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        Random random = new Random();
        while (numRetries < NUM_RETRIES) {
          database.begin();
          try {
            
            byte[] bytes = new byte[50 * 1024];
            Arrays.fill(bytes, (byte)32);
            
            ODocument document = database.newInstance("Person");
            document.field("id", ID);
            document.field("firstName", "Billy" + i);
            document.field("lastName", "Mayes" + i);
            document.field("id-id", ID);
            document.field("id-type", "Platform.Job");
            document.field("birthday", new Date());
            document.field("children", i);
            document.field("test", "test");
            document.field("bytes", bytes);
            
            Map<String, String> mapData = new HashMap<>();
            mapData.put("something", "value for something");
            mapData.put("date", "date");
            ODocument subDoc = new ODocument("Data");
            subDoc.field("_schema", mapData);
            subDoc.field("date", new Date(100000));
            subDoc.field("AddressLine1", "1825 Kramer Lane");
            subDoc.field("City", "Austin");
            subDoc.field("StateProvince", "TX");
            subDoc.field("PostalCode", "78758");
            subDoc.field("MatchScore", random.nextInt(100));
            subDoc.field("FloatValue", random.nextFloat());
            subDoc.field("BooleanValue", random.nextBoolean());
            subDoc.field("BigDecimalValue", new BigDecimal(random.nextFloat()));
            subDoc.field("DateValue", new Date());
            subDoc.field("DateTimeValue", new Date());
            subDoc.field("DoubleValue", random.nextDouble());
            subDoc.field("LongValue", random.nextLong());
            subDoc.field("NullValue", (String)null);

            document.field("data", subDoc);
            database.save(document);
            numWrites++;
            database.commit();
            break;
          } catch (Throwable e) {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Create Iteration=" + numRetries + ", id=" + ID + ", " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES) {
              throw e;
            }
          }
        }
      } finally {
        if (database != null) {
          database.close();
        }
      }
      return ID;
    }

    private String createRecord(int i, String clusterName) throws Throwable {
      ODatabaseDocumentTx database = null;
      String ID = getServerId() + "." + getId() + "-" + UUID.randomUUID().toString();
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        Random random = new Random();
        database.declareIntent(new OIntentMassiveInsert());
        while (numRetries < NUM_RETRIES) {
          database.begin(OTransaction.TXTYPE.NOTX);
          try {
            byte[] bytes = new byte[50 * 1024];
            Arrays.fill(bytes, (byte)32);

            ODocument document = database.newInstance("Person");
            document.field("id", ID);
            document.field("firstName", "Billy" + i);
            document.field("lastName", "Mayes" + i);
            document.field("id-id", ID);
            document.field("id-type", "Platform.Job");
            document.field("birthday", new Date());
            document.field("children", i);
            document.field("test", "test");
            document.field("bytes", bytes);

            Map<String, String> mapData = new HashMap<>();
            mapData.put("something", "value for something");
            mapData.put("date", "date");
            ODocument subDoc = new ODocument("Data");
            subDoc.field("_schema", mapData);
            subDoc.field("date", new Date(100000));
            subDoc.field("AddressLine1", "1825 Kramer Lane");
            subDoc.field("City", "Austin");
            subDoc.field("StateProvince", "TX");
            subDoc.field("PostalCode", "78758");
            subDoc.field("MatchScore", random.nextInt(100));
            subDoc.field("FloatValue", random.nextFloat());
            subDoc.field("BooleanValue", random.nextBoolean());
            subDoc.field("BigDecimalValue", new BigDecimal(random.nextFloat()));
            subDoc.field("DateValue", new Date());
            subDoc.field("DateTimeValue", new Date());
            subDoc.field("DoubleValue", random.nextDouble());
            subDoc.field("LongValue", random.nextLong());
            subDoc.field("NullValue", (String)null);

            document.field("data", subDoc);

            if(clusterName == null)
            {
              document.save();
            }
            else
            {
              document.save(clusterName);
            }
            numWrites++;
            database.commit();
            break;
          } catch (Throwable e) {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Create Iteration=" + numRetries + ", id=" + ID + ", " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES) {
              throw e;
            }
          }
        }
      } finally {
        if (database != null) {
          database.declareIntent(null);
          database.close();
        }
      }
      return ID;
    }

    private String addCluster() throws Throwable
    {
      ODatabaseDocumentTx database = null;
      String clusterName = "person" + "_" + UUID.randomUUID().toString();
      try
      {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        database.command(new OCommandSQL("ALTER CLASS Person ADDCLUSTER " + clusterName)).execute();
      }
      finally
      {
        if (database != null)
        {
          database.close();
        }
      }
      return clusterName;
    }

    private void printClusterNames() throws Throwable
    {
      ODatabaseDocumentTx database = null;
      String clusterName = "person" + "_" + UUID.randomUUID().toString();
      try
      {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        OSchema oSchema = database.getMetadata().getSchema();
        OClass oClass = oSchema.getClass("Person");
        if (oClass != null)
        {
          for (int i : oClass.getClusterIds())
          {
            logger.info("Cluster id [" + i + "] Cluster name [" + database.getClusterNameById(i) + "]");
          }
        }
      }
      finally
      {
        if (database != null)
        {
          database.close();
        }
      }

    }

    private ODocument findById(ODatabaseDocumentTx database, String id) throws Exception {

      OIndexManager indexManager = database.getMetadata().getIndexManager();
      OIndex index = indexManager.getIndex("id_pk");
      ORecordId recordId = (ORecordId) index.get(id);
      if (recordId == null) {
        throw new IllegalStateException("Cannot find record with id=" + id);
      }
      return database.getRecord(recordId);
//       List<ODocument> results = database.query(new OSQLSynchQuery<>("select * from Person where id = ?"), id);
//       if (results.size() == 0) {
//       throw new Exception("Person with ID=" + id + " not found");
//       } else if (results.size() > 1) {
//       throw new Exception("Query for ID=" + id + " returned " + results.size() + ", expected 1 value");
//       }
//       return results.get(0);
    }

    public int getNumWrites() {
      return numWrites;
    }

    public String getId() {
      return id;
    }

    public String getServerId() {
      return serverId;
    }
  }

  class Reader implements Callable<String> {
    private AtomicBoolean running = new AtomicBoolean(true);
    private String        id;
    private String        databaseUrl;

    Reader(String databaseUrl, String id) {
      this.databaseUrl = databaseUrl;
      this.id = id;
    }

    @Override
    public String call() {
      String msg;
      logger.log(Level.INFO, "Starting Reader...");
      ODatabaseDocumentTx database = null;
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        database.close();
        while (running.get()) {
          database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
          List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select count(*) from Person"));
          String absolutePath = new File(".").getAbsolutePath();
          absolutePath = absolutePath.substring(0, absolutePath.length() - 2);
          String pathName = absolutePath.substring(absolutePath.lastIndexOf("\\") + 1, absolutePath.length());
          String output = "Count(" + pathName + "): " + result.get(0) + " counting class: " + database.countClass("Person")
              + " counting cluster: " + database.countClusterElements("Person");
          logger.log(Level.INFO, output);
          database.close();
          //Thread.sleep(1000);
          String ID = createRecord(2);
          Thread.sleep(500);
          updateRecord(ID);
        }
        List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select count(*) from Person"));
        String output = "Final Count: " + result.get(0) + " counting class: " + database.countClass("Person")
            + " counting cluster: " + database.countClusterElements("Person");

        logger.log(Level.INFO, output);
        msg = "Final Count: " + result.get(0);
        System.out.println("Final Count: " + result.get(0));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      finally {
        logger.info("Reader[" + id + "] stopping");
        List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select count(*) from Person"));
        String output = "Final Count: " + result.get(0) + " counting class: " + database.countClass("Person")
            + " counting cluster: " + database.countClusterElements("Person");

        logger.log(Level.INFO, output);
        msg = "Final Count: " + result.get(0);
        System.out.println("Final Count: " + result.get(0));
        database.close();
      }
      return msg;
    }

    private String createRecord(int i) throws Throwable {
      ODatabaseDocumentTx database = null;
      String ID = "ID-" + UUID.randomUUID().toString();
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        Random random = new Random();
        while (numRetries < NUM_RETRIES) {
          database.begin();
          try {

            byte[] bytes = new byte[50 * 1024];
            Arrays.fill(bytes, (byte)32);

            ODocument document = database.newInstance("Person");
            document.field("id", ID);
            document.field("firstName", "Billy" + i);
            document.field("lastName", "Mayes" + i);
            document.field("id-id", ID);
            document.field("id-type", "Platform.Job");
            document.field("birthday", new Date());
            document.field("children", i);
            document.field("test", "test");
            document.field("bytes", bytes);

            Map<String, String> mapData = new HashMap<>();
            mapData.put("something", "value for something");
            mapData.put("date", "date");
            ODocument subDoc = new ODocument("Data");
            subDoc.field("_schema", mapData);
            subDoc.field("date", new Date(100000));
            subDoc.field("AddressLine1", "1825 Kramer Lane");
            subDoc.field("City", "Austin");
            subDoc.field("StateProvince", "TX");
            subDoc.field("PostalCode", "78758");
            subDoc.field("MatchScore", random.nextInt(100));
            subDoc.field("FloatValue", random.nextFloat());
            subDoc.field("BooleanValue", random.nextBoolean());
            subDoc.field("BigDecimalValue", new BigDecimal(random.nextFloat()));
            subDoc.field("DateValue", new Date());
            subDoc.field("DateTimeValue", new Date());
            subDoc.field("DoubleValue", random.nextDouble());
            subDoc.field("LongValue", random.nextLong());
            subDoc.field("NullValue", (String)null);

            document.field("data", subDoc);
            database.save(document);
            database.commit();
            break;
          } catch (Throwable e) {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Create Iteration=" + numRetries + ", id=" + ID + ", " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES) {
              throw e;
            }
          }
        }
      } finally {
        if (database != null) {
          database.close();
        }
      }
      return ID;
    }
    private void updateRecord(String id) throws Throwable {
      ODatabaseDocumentTx database = null;
      try {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        while (numRetries < NUM_RETRIES) {
          try {
            database.begin();
            ODocument document = findById(database, id);
            document.field("test", "test updated");
            database.save(document);
            database.commit();
            break;
          } catch (Throwable e) {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Update Iteration=" + numRetries + ", id=" + id + ", " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES) {
              throw e;
            }
          }
        }
      } finally {
        if (database != null && !database.isClosed()) {
          database.close();
        }
      }
    }
    private ODocument findById(ODatabaseDocumentTx database, String id) throws Exception {

      OIndexManager indexManager = database.getMetadata().getIndexManager();
      OIndex index = indexManager.getIndex("id_pk");
      ORecordId recordId = (ORecordId) index.get(id);
      if (recordId == null) {
        throw new IllegalStateException("Cannot find record with id=" + id);
      }
      return database.getRecord(recordId);
    }

    public void stop() {
      this.running.set(false);
    }
  }

  private static void registerShutdownHook(final List<Reader> readers, final OServer server, final ExecutorService executor) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          Thread.sleep(100);
          for (Reader reader : readers) {
            reader.stop();
          }
          // System.out.println("Wrote " + writer.getNumWrites() + " records");
          executor.shutdown();
          server.shutdown();
          logger.log(Level.INFO, "DONE!");
          // System.out.println("DONE!");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }

  private String getDatabaseURL(int serverId) {
    String ORIENTDB_HOME = new File("servers/" + serverId).getAbsolutePath().replace('\\', '/');
    System.setProperty("ORIENTDB_HOME", ORIENTDB_HOME);
    String databaseUrl = "plocal:" + ORIENTDB_HOME + "/databases/platform";
    logger.log(Level.INFO, "URL=" + databaseUrl);
    return databaseUrl;
  }

}
