package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactionPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

/**
 * Dynamic Partition implementation of StoreFileManager. Not thread safe - relies on external locking (in
 * HStore). Collections that this class returns are immutable or unique to the call, so they should
 * be safe. DP store organizes the SSTables of the region into non-overlapping DPs. Each DP contains
 * a set of SSTables. When scan or get happens, it only has to read the SSTables from the
 * corresponding DPs. See DPCompationPolicy on how the DPs are determined; this class
 * doesn't care. This class should work together with DPCompactionPolicy and DPCompactor.
 * With regard to how they work, we make at least the following (reasonable) assumptions: -
 * Compaction produces one SSTable per new DP (if any); that is easy to change. - Compaction has
 * one contiguous set of DPs both in and out.
 */
public class DPStoreFileManager implements StoreFileManager, DPCompactionPolicy.DPInformationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreFileManager.class);
  /**
   * The file metadata fields that contain the DP information.
   */
  public static final byte[] STRIPE_START_KEY = Bytes.toBytes("STRIPE_START_KEY");
  public static final byte[] STRIPE_END_KEY = Bytes.toBytes("STRIPE_END_KEY");
  private final static Bytes.RowEndKeyComparator MAP_COMPARATOR = new Bytes.RowEndKeyComparator();
  /**
   * The key value used for range boundary, indicating that the boundary is open (i.e. +-inf).
   */
  public final static byte[] OPEN_KEY = HConstants.EMPTY_BYTE_ARRAY;
  final static byte[] INVALID_KEY = null;

  /**
   * The state class. Used solely to replace results atomically during compactions and avoid
   * complicated error handling.
   */
  private static class State {
    /**
     * The end rows of each DP. The last DP end is always open-ended, so it's not stored
     * here. It is invariant that the start row of the DP is the end row of the previous one
     * (and is an open boundary for the first one).
     */
    public byte[][] dpEndRows = new byte[0][];
    /**
     * Files by DP. Each element of the list corresponds to dpEndRow element with the same
     * index, except the last one. Inside each list, the files are in reverse order by seqNum. Note
     * that the length of this is one higher than that of dpEndKeys.
     */
    public ArrayList<ImmutableList<HStoreFile>> dpFiles = new ArrayList<>();
    /** Cached list of all files in the structure, to return from some calls */
    public ImmutableList<HStoreFile> allFilesCached = ImmutableList.of();
    private ImmutableList<HStoreFile> allCompactedFilesCached = ImmutableList.of();
  }
  private State state = null;
  /** Cached file metadata (or overrides as the case may be) */
  private HashMap<HStoreFile, byte[]> fileStarts = new HashMap<>();
  private HashMap<HStoreFile, byte[]> fileEnds = new HashMap<>();
  /**
   * Normally invalid key is null, but in the map null is the result for "no key"; so use the
   * following constant value in these maps instead. Note that this is a constant and we use it to
   * compare by reference when we read from the map.
   */
  private static final byte[] INVALID_KEY_IN_MAP = new byte[0];

  private final CellComparator cellComparator;

  private final int blockingFileCount;

  public DPStoreFileManager(CellComparator kvComparator, Configuration conf) {
    this.cellComparator = kvComparator;
    this.blockingFileCount =
      conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
  }
  @Override public void loadFiles(List<HStoreFile> storeFiles) {

  }

  @Override public void insertNewFiles(Collection<HStoreFile> sfs) {

  }

  @Override public void addCompactionResults(Collection<HStoreFile> compactedFiles,
    Collection<HStoreFile> results) {

  }

  @Override public void removeCompactedFiles(Collection<HStoreFile> compactedFiles) {

  }

  @Override public ImmutableCollection<HStoreFile> clearFiles() {
    return null;
  }

  @Override public Collection<HStoreFile> clearCompactedFiles() {
    return null;
  }

  @Override public Collection<HStoreFile> getStorefiles() {
    return state.allFilesCached;
  }

  @Override
  public byte[] getStartRow(int partitionIndex) {
    return new byte[0];
  }

  @Override
  public byte[] getEndRow(int partitionIndex) {
    return new byte[0];
  }

  @Override
  public List<HStoreFile> getLevel0Files() {
    return null;
  }

  @Override
  public List<byte[]> getDPBoundaries() {
    return null;
  }

  @Override
  public ArrayList<ImmutableList<HStoreFile>> getDPs() {
    return null;
  }

  @Override
  public int getDPCount() {
    return 0;
  }

  @Override public Collection<HStoreFile> getCompactedfiles() {
    return null;
  }

  @Override public int getStorefileCount() {
    return 0;
  }

  @Override public int getCompactedFilesCount() {
    return 0;
  }

  @Override public Collection<HStoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow) {
    return null;
  }

  @Override public Iterator<HStoreFile> getCandidateFilesForRowKeyBefore(KeyValue targetKey) {
    return null;
  }

  @Override public Iterator<HStoreFile> updateCandidateFilesForRowKeyBefore(
    Iterator<HStoreFile> candidateFiles, KeyValue targetKey, Cell candidate) {
    return null;
  }

  @Override public Optional<byte[]> getSplitPoint() throws IOException {
    return Optional.empty();
  }

  @Override public int getStoreCompactionPriority() {
    return 0;
  }

  @Override
  public Collection<HStoreFile> getUnneededFiles(long maxTs, List<HStoreFile> filesCompacting) {
    return null;
  }

  @Override public double getCompactionPressure() {
    return 0;
  }

  @Override public Comparator<HStoreFile> getStoreFileComparator() {
    return null;
  }
}
