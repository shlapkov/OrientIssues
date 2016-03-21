package test;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BulkDeleteTest
{
  private static Logger logger = Logger.getLogger(BulkDeleteTest.class.getName());
  private static final String SERVER_ID = "BulkDeleteTest";
  private static final int NUM_RETRIES = 5;
  private static final int BATCH_SIZE = 5000;
  
  public static void main(String[] args) throws Throwable
  {
    if (args.length < 1) System.out.println("Usage: create, execute [count]");

    BulkDeleteTest main = new BulkDeleteTest();
    if (args[0].equals("create"))
    {
      main.create();
    }
    else if (args[0].equals("execute"))
    {
      int count = Integer.parseInt(args[1]);
      main.execute(count);
    }
    else
    {
      System.out.println("Usage: create <serverid>, execute <serverid> [count], slave <serverid>");
    }
  }


  public void create() throws Exception
  {
    OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
    OFileUtils.deleteRecursively(new File("servers"));

    String confDir = System.getProperty("g1.server.conf.dir");
    if (confDir == null)
    {
      confDir = "target/classes";
      System.setProperty("g1.server.conf.dir", confDir);
    }

    String databaseUrl = getDatabaseURL();

    initializeExceptionRecordTable(databaseUrl);
  }


  public void execute(int count) throws Throwable
  {
    String confDir = System.getProperty("g1.server.conf.dir");
    if (confDir == null)
    {
      confDir = "target/classes";
      System.setProperty("g1.server.conf.dir", confDir);
    }

    String databaseUrl = getDatabaseURL();

    logger.log(Level.INFO, "Starting EXECUTE test against database " + databaseUrl + " count = " + count);

    // start in clustered mode
    OGlobalConfiguration.DISTRIBUTED_CRUD_TASK_SYNCH_TIMEOUT.setValue(10000l);
    OGlobalConfiguration.DISTRIBUTED_ASYNCH_RESPONSES_TIMEOUT.setValue(15000l);
    OGlobalConfiguration.DISTRIBUTED_QUEUE_TIMEOUT.setValue(30000l);

    // start in clustered mode
    OServer server = new OServer();
    FileInputStream config = null;
    try
    {
      config = new FileInputStream("src/test/resources/orientdb-dserver-config-BulkDelete.xml");
      server.startup(config);
      server.activate();
  
      OHazelcastPlugin hzPlugin = server.getPluginByClass(OHazelcastPlugin.class);
      if (hzPlugin.isEnabled())
      {
        hzPlugin.waitUntilOnline();
      }
  
      // Add records
      long startAdd = System.nanoTime();
      logger.info("Adding " + count + " records");
      createExceptionRecords(count, true); 
      long endAdd = System.nanoTime();
      long elapsedSecondsAdd = (endAdd - startAdd) / 1000000000;
      logger.info("Added " + count + " records in " + elapsedSecondsAdd + " seconds");
      
      // Update records
      logger.info("Updating records");
      long startUpdate= System.nanoTime();
      int numUpdated= updateRecords();
      long endUpdate= System.nanoTime();
      long elapsedSecondsUpdate = (endUpdate - startUpdate) / 1000000000;
      logger.info("Updated " + numUpdated + " records in " + elapsedSecondsUpdate + " seconds");
      
      
      // Delete records
      logger.info("Deleting records");
      long startDelete = System.nanoTime();
      int numDeleted = deleteRecords();
      long endDelete = System.nanoTime();
      long elapsedSecondsDelete = (endDelete - startDelete) / 1000000000;
      logger.info("Deleted " + numDeleted + " records in " + elapsedSecondsDelete + " seconds");
      logger.info("Deleting records with limits");
      startDelete = System.nanoTime();
      numDeleted = deleteRecordsWithLimit();
      endDelete = System.nanoTime();
      elapsedSecondsDelete = (endDelete - startDelete) / 1000000000;
      logger.info("Deleted " + numDeleted + " records in " + elapsedSecondsDelete + " seconds");
    }
    finally
    {
      server.shutdown();
      if(config != null)
      {
        config.close();
      }
    }

  }


  private int deleteRecords() throws Throwable {
    ODatabaseDocumentTx database = null;
    try {
      database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(), "admin", "admin");
      int numRetries = 0;
      int numDeleted = 0;
      while (numRetries < NUM_RETRIES) {
        database.begin();
        try {
          OCommandSQL cmd = new OCommandSQL("DELETE from ExceptionRecordVersion where username='admin'");
          database.command(cmd).execute((Object[])null);
          cmd = new OCommandSQL("DELETE from ExceptionRecord where username='admin'");
          numDeleted = database.command(cmd).execute((Object[])null);
          database.commit();
          break;
        } catch (Throwable e) {
          database.rollback();
          logger.log(Level.SEVERE, "********************************");
          logger.log(Level.SEVERE, "Delete Iteration=" + numRetries + ", " + e.toString(), e);
          logger.log(Level.SEVERE, "********************************");
          if (numRetries++ == NUM_RETRIES) {
            throw e;
          }
        }
      }
      return numDeleted;
    } finally {
      if (database != null && !database.isClosed()) {
        database.close();
      }
    }
  }

  private int deleteRecordsWithLimit() throws Throwable {
    ODatabaseDocumentTx database = null;
    try {
      database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(), "admin", "admin");
      int numRetries = 0;
      int numDeleted = 0;
      while (numRetries < NUM_RETRIES) {
        try {
          int deletedBatch = 0;
          do
          {
            database.begin();
            OCommandSQL cmd = new OCommandSQL("DELETE from ExceptionRecord where username='admin' LIMIT " + BATCH_SIZE);
            deletedBatch = database.command(cmd).execute((Object[])null);
            numDeleted += deletedBatch;
            database.commit();
          }
          while (deletedBatch > 0);
          break;
        } catch (Throwable e) {
          database.rollback();
          logger.log(Level.SEVERE, "********************************");
          logger.log(Level.SEVERE, "Delete Iteration=" + numRetries + ", " + e.toString(), e);
          logger.log(Level.SEVERE, "********************************");
          if (numRetries++ == NUM_RETRIES) {
            throw e;
          }
        }
      }
      return numDeleted;
    } finally {
      if (database != null && !database.isClosed()) {
        database.close();
      }
    }
  }

  private void createExceptionRecords(int count, boolean batch) throws Exception {
    ODatabaseDocumentTx database = null;
    try {
      database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(), "admin", "admin");
      database.begin();
      for (int x = 0; x < count; x++)
      {
        String ID = UUID.randomUUID().toString();
        ODocument document = database.newInstance("ExceptionRecord");
        document.field("id", ID);
        document.field("data", DATA);
        document.field("status", "original");
        document.field("approved", false);
        document.field("groupNonException", false);
        document.field("comment", "");
        document.field("jobId", "1");
        document.field("dataflowName", "Error_Handling_V5");
        document.field("username", "admin");
        document.field("timestamp", new Date().getTime());
        document.field("stageLabel", "Exception Monitor");
        document.field("groupColumn", "");
        document.field("lastModifiedBy", "admin");
        document.field("lastModified", new Date());
        database.save(document);
        
        createExceptionRecordVersion(database, ID, "1");
        if (batch && (x % BATCH_SIZE) == 0)
        {
          System.out.println("Committing batch of " + BATCH_SIZE);
          database.commit();
          database.begin();
        }
      }
      database.commit();
    }
    catch (Throwable e)
    {
      database.rollback();
      throw e;
    }
    finally {
      if (database != null) {
        database.close();
      }
    }
  }

  private void createExceptionRecordVersion(ODatabaseDocumentTx database, String id, String version) throws Exception {
    try {
      int numRetries = 0;
      while (numRetries < NUM_RETRIES) {
        try {
          ODocument document = database.newInstance("ExceptionRecordVersion");
          document.field("key-exceptionId", id);
          document.field("key-version", version);
          document.field("jobId", "1");
          document.field("dataflowName", "Error_Handling_V5");
          document.field("exceptionVersion-version", DATA);
          database.save(document);
          
          break;
        } catch (Throwable e) {
          logger.log(Level.SEVERE, "********************************");
          logger.log(Level.SEVERE, "Create Iteration=" + numRetries + ", id=" + id + ", version=" + version + ", " + e.toString(), e);
          logger.log(Level.SEVERE, "********************************");
          if (numRetries++ == NUM_RETRIES) {
            throw e;
          }
        }
      }
    }
    finally
    {
    }
  }
  
  private int updateRecords() throws Throwable {
    ODatabaseDocumentTx database = null;
    try {
      database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(), "admin", "admin");
      int numRetries = 0;
      int numUpdated = 0;
      while (numRetries < NUM_RETRIES) {
        database.begin();
        try {
          OCommandSQL cmd = new OCommandSQL("UPDATE ExceptionRecord set jobId='2' where username='admin'");
          numUpdated = database.command(cmd).execute((Object[])null);
          database.commit();
          break;
        } catch (Throwable e) {
          database.rollback();
          logger.log(Level.SEVERE, "********************************");
          logger.log(Level.SEVERE, "Update Iteration=" + numRetries + ", " + e.toString(), e);
          logger.log(Level.SEVERE, "********************************");
          if (numRetries++ == NUM_RETRIES) {
            throw e;
          }
        }
      }
      return numUpdated;
    } finally {
      if (database != null && !database.isClosed()) {
        database.close();
      }
    }
  }

  private String getDatabaseURL()
  {
    String ORIENTDB_HOME = new File("servers/" + SERVER_ID).getAbsolutePath().replace('\\', '/');
    System.setProperty("ORIENTDB_HOME", ORIENTDB_HOME);
    String databaseUrl = "plocal:" + ORIENTDB_HOME + "/databases/platform";
    return databaseUrl;
  }

  private void initializeExceptionRecordTable(String databaseUrl)
  {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(databaseUrl).create();
    OClass exceptionRecordClass = database.getMetadata().getSchema().createClass("ExceptionRecord");
    OClass exceptionVersionClass = database.getMetadata().getSchema().createClass("ExceptionRecordVersion");
    try
    {

      exceptionRecordClass.createProperty("id", OType.STRING);
      exceptionRecordClass.createProperty("status", OType.STRING);
      exceptionRecordClass.createProperty("approved", OType.BOOLEAN);
      exceptionRecordClass.createProperty("recordType", OType.STRING);
      exceptionRecordClass.createProperty("groupNonException", OType.BOOLEAN);
      exceptionRecordClass.createProperty("comment", OType.STRING);
      exceptionRecordClass.createProperty("jobId", OType.STRING);
      exceptionRecordClass.createProperty("dataflowName", OType.STRING);
      exceptionRecordClass.createProperty("username", OType.STRING);
      exceptionRecordClass.createProperty("timestamp", OType.LONG);
      exceptionRecordClass.createProperty("stageLabel", OType.STRING);
      exceptionRecordClass.createProperty("groupColumn", OType.STRING);
      exceptionRecordClass.createProperty("lastModifiedBy", OType.STRING);
      exceptionRecordClass.createProperty("lastModified", OType.LONG);
      exceptionRecordClass.createProperty("data", OType.STRING);

      exceptionVersionClass.createProperty("key-exceptionId", OType.STRING);
      exceptionVersionClass.createProperty("key-version", OType.STRING);
      exceptionVersionClass.createProperty("jobId", OType.STRING);
      exceptionVersionClass.createProperty("dataflowName", OType.STRING);

      exceptionRecordClass.createIndex("ExceptionRecord_pk", INDEX_TYPE.UNIQUE, "id");
      exceptionRecordClass.createIndex("ExceptionRecord_idx", INDEX_TYPE.NOTUNIQUE, "status", "approved", "recordType", "groupNonException",
              "comment", "jobId", "dataflowName", "username", "timestamp", "stageLabel", "groupColumn", "lastModifiedBy", "lastModified");

      exceptionVersionClass.createIndex("ExceptionVersion_Index", INDEX_TYPE.NOTUNIQUE, "key-exceptionId", "key-version", "jobId", "dataflowname");
    }
    finally
    {
      database.close();
    }
  }

  private static String DATA = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n" + 
          "<exceptionRecord>\r\n" + 
          "    <data>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>ClmntState</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">CA</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>EFFDTE</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"></value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>ERR_CD</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">E0001</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>LOSS_DATE_or_Accident_Date</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">03/02/2013 00:00:00</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>db_POLICY</key>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>CLMNT</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">01</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>Accident_State_cd</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">04</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>Subline</key>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>TPA</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">SED</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>db_EFFDTE</key>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>Type_of_Injury</key>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>ASLOB</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">170</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>TYPE_Addl_Info</key>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>ERR_MSG</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">PolicyNumber 71GPP4967403 incorrect for this Claim</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>ClmntAddr1</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">7753 NEWLINE AVE</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>PolEffDate</key>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>TYPE_Claimant</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">Y</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>ClassCode</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">000000</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>Pad_Policy</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">000000000000000000071GPP4967403</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>POLICY</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">71GPP4967403</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>TYPE_Claim</key>\r\n" + 
          "            <value xsi:type=\"xs:string\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">Y</value>\r\n" + 
          "        </entry>\r\n" + 
          "        <entry>\r\n" + 
          "            <key>Body_Part_Cd</key>\r\n" + 
          "        </entry>\r\n" + 
          "    </data>\r\n" + 
          "    <id>4b401635-d692-4430-a4b3-52681485e991</id>\r\n" + 
          "    <metadata>\r\n" + 
          "        <status>original</status>\r\n" + 
          "        <recordtype>exception</recordtype>\r\n" + 
          "        <comment></comment>\r\n" + 
          "        <jobId>1</jobId>\r\n" + 
          "        <dataflowName>Error_Handling_V5</dataflowName>\r\n" + 
          "        <username>admin</username>\r\n" + 
          "        <timestamp>1380118717674</timestamp>\r\n" + 
          "        <stageLabel>Exception Monitor</stageLabel>\r\n" + 
          "        <groupColumn></groupColumn>\r\n" + 
          "        <isApproved>false</isApproved>\r\n" + 
          "        <matchedConditions>\r\n" + 
          "            <condition>\r\n" + 
          "                <name>E0001</name>\r\n" + 
          "                <metric>PolicyNumber incorrect for this Claim</metric>\r\n" + 
          "                <domain>POLICY_NO</domain>\r\n" + 
          "            </condition>\r\n" + 
          "        </matchedConditions>\r\n" + 
          "        <lastModifiedBy>admin</lastModifiedBy>\r\n" + 
          "        <lastModified>2013-09-25T09:18:37.958-05:00</lastModified>\r\n" + 
          "        <dataTypes/>\r\n" + 
          "    </metadata>\r\n" + 
          "    <readOnlyFields>Status.Description</readOnlyFields>\r\n" + 
          "    <readOnlyFields>YYYY</readOnlyFields>\r\n" + 
          "</exceptionRecord>\r\n" + 
          "";
  
}
