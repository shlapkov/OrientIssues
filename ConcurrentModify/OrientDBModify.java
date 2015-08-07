package test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.script.OCommandScript;
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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class OrientDBModify
{
  private static Logger    logger      = Logger.getLogger(OrientDBModify.class.getName());
  private static final int NUM_RETRIES = 5;
  private static final int NUM_READERS = 1;

  public void execute(int serverId, boolean distributed) throws Exception {
    String confDir = System.getProperty("g1.server.conf.dir");
    if (confDir == null) {
      confDir = "target/classes";
      System.setProperty("g1.server.conf.dir", confDir);
    }

    String databaseUrl = getDatabaseURL(serverId);

    logger.log(Level.INFO, "Starting EXECUTE test against database " + databaseUrl);


    System.out.println("\n\nCurrent directory: " + new File(".").getAbsolutePath());

    // start in clustered mode
    OServer server = new OServer();
    if(distributed)
    {
      server.startup(new FileInputStream("src/test/resources/orientdb-dserver-config-" + serverId + ".xml"));
    }
    else
    {
      server.startup(new FileInputStream("src/test/resources/orientdb-server-config-" + serverId + ".xml"));
    }
    server.activate();

    if(distributed)
    {
      server.getPluginByClass(OHazelcastPlugin.class).waitUntilOnline();
    }

    //reindex if needed
    ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
    if( database!=null )
      database.close();

    logger.log(Level.INFO, "");
    logger.log(Level.INFO, "");
    logger.log(Level.INFO, "Hit return to start test");
    logger.log(Level.INFO, "");
    // System.in.read();

    ExecutorService executor = Executors.newCachedThreadPool();

    List<Callable<String>> callables = new ArrayList<>();

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
      System.out.println("Usage: create <serverid>, execute <serverid> <distributed>");

    final int serverId = Integer.parseInt(args[1]);

    OrientDBModify main = new OrientDBModify();
    if (args[0].equals("create")) {
      main.create(serverId);
    } else if (args[0].equals("execute")) {
      boolean distributed =  Boolean.valueOf(args[2]);
      main.execute(serverId, distributed);
    } else {
      System.out.println("Usage: create <serverid>, execute <serverid> <distributed>");
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
        while (running.get()) {
          String id = createRecord(28);
          Thread.sleep(1000);
          batchOperation(id);
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

    private void batchOperation(String id) throws Throwable
    {
      ODatabaseDocumentTx database = null;
      try
      {
        database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        int numRetries = 0;
        while (numRetries < NUM_RETRIES)
        {
          database.begin();
          try
          {
            ODocument document = findById(database, id);
            document.field("test", "test updated");
            database.save(document);

            String sql = "UPDATE Person SET PostalCode = \"78001\" WHERE id = \"" + id + "\"";
            OCommandScript cmdScript = new OCommandScript("sql", sql);
            database.command(cmdScript).execute();

            logger.log(Level.INFO, "Before commit Version = " + document.getVersion() + " test = " + document.field("test") + " PostCode = " + document.field("PostalCode"));
            database.commit();

            document = findById(database, id);
            logger.log(Level.INFO, "After commit Version = " + document.getVersion()  + " test = " + document.field("test") + " PostCode = " + document.field("PostalCode"));
            break;
          }
          catch (Throwable e)
          {
            database.rollback();
            logger.log(Level.SEVERE, "********************************");
            logger.log(Level.SEVERE, "Create Iteration=" + numRetries + " " + e.toString(), e);
            logger.log(Level.SEVERE, "********************************");
            if (numRetries++ == NUM_RETRIES)
            {
              throw e;
            }
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
