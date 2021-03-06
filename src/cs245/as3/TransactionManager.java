package cs245.as3;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
  private static final byte[] COMMIT_MESSAGE = "Commit".getBytes();
  private LogManager lm;
  private StorageManager sm;
  public static final char DELIMITER_CHAR = ',';
  public static final String DELIMITER_STRING = ",";
  private final long EMPTY_LONG = -1l;

  class WritesetEntry {
    public long key;
    public byte[] value;

    public WritesetEntry(long key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }

  public class LogLineRecord {
    long txID;
    long key;
    byte[] value;

    public LogLineRecord(long txID, long key, byte[] value) {
      this.txID = txID;
      this.key = key;
      this.value = value;
    }

    public long getTxID() {
      return txID;
    }

    public void setTxID(long txID) {
      this.txID = txID;
    }

    public long getKey() {
      return key;
    }

    public void setKey(long key) {
      this.key = key;
    }

    public byte[] getValue() {
      return value;
    }

    public void setValue(byte[] value) {
      this.value = value;
    }
  }

  /**
   * Holds the latest value for each key.
   */
  private HashMap<Long, TaggedValue> latestValues;
  /**
   * Hold on to writesets until commit.
   */
  private HashMap<Long, ArrayList<WritesetEntry>> writesets;

  public TransactionManager() {
    writesets = new HashMap<>();
    //see initAndRecover
    latestValues = null;
  }

  /**
   * Prepare the transaction manager to serve operations.
   * At this time you should detect whether the StorageManager is inconsistent and recover it.
   */
  public void initAndRecover(StorageManager sm, LogManager lm) {
    this.sm = sm;
    this.lm = lm;
    latestValues = sm.readStoredTable();
    checkAndRecover();
  }

  private void checkAndRecover() {
    int offset = 0;
    List<LogLineRecord> recordList = new ArrayList<>();
    Set<Long> commitedRecords = new HashSet<>();
    while (offset < lm.getLogEndOffset()) {
      byte[] logLineLengthByteArray = lm.readLogRecord(offset, Long.BYTES);
      offset += Long.BYTES;
      long length = getLongFromBytes(logLineLengthByteArray);
      byte[] logLineByteArray = lm.readLogRecord(offset, (int) length);
      LogLineRecord logLineRecord = deconstructLogLineToRecord(logLineByteArray);
      handleRecords(logLineRecord, recordList, commitedRecords);
      offset += length;
    }
    rewriteMissingRecords(recordList, commitedRecords);
  }

  private void handleRecords(LogLineRecord logLineRecord, List<LogLineRecord> recordList, Set<Long> commitedRecords) {
    recordList.add(logLineRecord);
    if (isCommitMessage(logLineRecord)) {
      commitedRecords.add(logLineRecord.txID);
    }
  }

  private boolean isCommitMessage(LogLineRecord logLineRecord) {
    return logLineRecord.key == -1 && logLineRecord.getValue().toString().equals(COMMIT_MESSAGE);
  }

  private void rewriteMissingRecords(List<LogLineRecord> recordList, Set<Long> commitedRecords) {
    for (LogLineRecord record : recordList) {
      if (!commitedRecords.contains(record.getTxID()) && !isCommitMessage(record)) {
        //write it to the storage manager
        long tag = 0;
        latestValues.put(record.key, new TaggedValue(tag, record.value));
        sm.queueWrite(record.key,0,record.value);
      }
    }
  }

  /**
   * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
   */
  public void start(long txID) {

    // TODO: Not implemented for non-durable transactions, you should implement this
  }

  /**
   * Returns the latest committed value for a key by any transaction.
   */
  public byte[] read(long txID, long key) {
    TaggedValue taggedValue = latestValues.get(key);
    return taggedValue == null ? null : taggedValue.value;
  }

  /**
   * Indicates a write to the database. Note that such writes should not be visible to read()
   * calls until the transaction making the write commits. For simplicity, we will not make reads
   * to this same key from txID itself after we make a write to the key.
   */
  public void write(long txID, long key, byte[] value) {
    ArrayList<WritesetEntry> writeset = writesets.get(txID);
    if (writeset == null) {
      writeset = new ArrayList<>();
      writesets.put(txID, writeset);
    }
    writeset.add(new WritesetEntry(key, value));
  }

  private void addCommitMessage(long txID, ArrayList<WritesetEntry> writeset) {
    writeset.add(new WritesetEntry(EMPTY_LONG, COMMIT_MESSAGE));
  }

  private void writeToLogManager(long txID, ArrayList<WritesetEntry> writeset) {
    List<byte[]> logLines =
        writeset.stream().map(w -> createLogLineWithRecord(txID, w.key, w.value)).collect(Collectors.toList());
    logLines.forEach(b -> {
      byte[] logLineLength = getByteFromLong(b.length);
      lm.appendLogRecord(logLineLength);
      lm.appendLogRecord(b);
    });
  }

  private byte[] getByteFromLong(long length) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(length);
    return buffer.array();
  }

  public long getLongFromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(bytes);
    buffer.flip();//need flip
    return buffer.getLong();
  }

  /**
   * Commits a transaction, and makes its writes visible to subsequent read operations.\
   */
  public void commit(long txID) {
    ArrayList<WritesetEntry> writeset = writesets.get(txID);
    if (writeset != null) {
      addCommitMessage(txID, writeset);
      writeToLogManager(txID, writeset);
      for (WritesetEntry x : writeset) {
        //tag is unused in this implementation:
        long tag = 0;
        if (x.key >= 0) {
          latestValues.put(x.key, new TaggedValue(tag, x.value));
        }
      }
      writesets.remove(txID);
    }
  }

  /**
   * Aborts a transaction.
   */
  public void abort(long txID) {
    writesets.remove(txID);
  }

  /**
   * The storage manager will call back into this procedure every time a queued write becomes persistent.
   * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
   */
  public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
    sm.queueWrite(key, persisted_tag, persisted_value);
  }

  public byte[] createLogLineWithRecord(long txID, long key, byte[] value) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream dOS = new DataOutputStream(stream);
    try {
      byte[] txIDArray = getByteFromLong(txID);
      dOS.write(txIDArray);
      byte[] keyArray = getByteFromLong(key);
      dOS.write(keyArray);
      dOS.write(value);
      dOS.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stream.toByteArray();
  }

  public LogLineRecord deconstructLogLineToRecord(byte[] bytes) {

    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    long txID = byteBuffer.getLong();
    long key = byteBuffer.getLong();
    byte[] value = new byte[byteBuffer.remaining()];
    byteBuffer.get(value);
    LogLineRecord record = new LogLineRecord(txID, key, value);
    return record;
  }
}
