package cs245.as3;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

  }

  private void writeToLogManager(long txID, ArrayList<WritesetEntry> writeset) {
    List<byte[]> logLines =
        writeset.stream().map(w -> createLogLine(txID, w.key, w.value)).collect(Collectors.toList());
    logLines.forEach(b -> lm.appendLogRecord(b));
  }

  /**
   * Commits a transaction, and makes its writes visible to subsequent read operations.\
   */
  public void commit(long txID) {
    ArrayList<WritesetEntry> writeset = writesets.get(txID);
    addCommitMessage(txID,writeset);
    writeToLogManager(txID, writeset);
    if (writeset != null) {
      for (WritesetEntry x : writeset) {
        //tag is unused in this implementation:
        long tag = 0;
        latestValues.put(x.key, new TaggedValue(tag, x.value));
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
  }

  public byte[] createLogLine(long txID, long key, byte[] value) {
    StringBuilder sb = new StringBuilder();
    sb.append(Long.toString(txID));
    sb.append(DELIMITER_CHAR);
    sb.append(Long.toString(key));
    sb.append(DELIMITER_CHAR);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream dOS = new DataOutputStream(stream);
    try {
      dOS.write(sb.toString().getBytes());
      dOS.write(value);
      dOS.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stream.toByteArray();
  }

  public List<String> deconstructLogLine(byte[] bytes) {
    String logLine = new String(bytes);
    String[] split = logLine.split(DELIMITER_STRING);
    List<String> list = Arrays.asList(split);
    return list;
  }

//  public String createLogLine(List<String> logLineList) {
//    StringBuilder stringBuilder = new StringBuilder();
//    logLineList.stream().forEach(l -> {
//      stringBuilder.append(l);
//      stringBuilder.append(DELIMITER_STRING);
//    });
//    return stringBuilder.toString();
//  }

//  public List<String> deconstructLogLine(String s) {
//    String[] split = s.split(DELIMITER_STRING);
//    List<String> list = Arrays.asList(split);
//    return list;
//  }

//  public byte[] stringToByte(String string) {
//    return string.getBytes();
//  }
//
//  public String bytesToString(byte[] bytes) {
//    return new String(bytes);
//  }
}
