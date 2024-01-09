package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.apache.hadoop.util.StringUtils;
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
  public static final byte[] DP_START_KEY = Bytes.toBytes("DP_START_KEY");
  public static final byte[] DP_END_KEY = Bytes.toBytes("DP_END_KEY");
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
    /** Level 0. The files are in reverse order by seqNum. */
    public ImmutableList<HStoreFile> level0Files = ImmutableList.of();
    /**
     * Cached list of all files in the structure, to return from some calls.
     */
    public ImmutableList<HStoreFile> allFilesCached = ImmutableList.of();
    private ImmutableList<HStoreFile> allCompactedFilesCached = ImmutableList.of();
  }
  private State state = null;
  /**
   * Cached file metadata (or overrides as the case may be) .
   */
  private HashMap<HStoreFile, byte[]> fileStarts = new HashMap<>();
  private HashMap<HStoreFile, byte[]> fileEnds = new HashMap<>();
  /**
   * Normally invalid key is null, but in the map null is the result for "no key"; so use the
   * following constant value in these maps instead. Note that this is a constant and we use it to
   * compare by reference when we read from the map.
   */
  private static final byte[] INVALID_KEY_IN_MAP = new byte[0];

  private final CellComparator cellComparator;
  private DPStoreConfig config;

  private final int blockingFileCount;

  public DPStoreFileManager(CellComparator kvComparator, Configuration conf,
    DPStoreConfig config) {
    this.cellComparator = kvComparator;
    this.config = config;
    this.blockingFileCount =
      conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
  }
  @Override
  public void loadFiles(List<HStoreFile> storeFiles) {
    loadUnclassifiedStoreFiles(storeFiles);
  }

  /**
   * Loads initial store files that were picked up from some physical location pertaining to this
   * store (presumably). Unlike adding files after compaction, assumes empty initial sets, and is
   * forgiving with regard to partition constraints - at worst, many/all files will go to level 0.
   * @param storeFiles Store files to add.
   */
  private void loadUnclassifiedStoreFiles(List<HStoreFile> storeFiles) {
    TreeMap<byte[], ArrayList<HStoreFile>> candidateStripes = new TreeMap<>(MAP_COMPARATOR);
    ArrayList<HStoreFile> level0Files = new ArrayList<>();
    // Separate the files into tentative stripes; then validate. Currently, we rely on metadata.
    // If needed, we could dynamically determine the stripes in future.
    for (HStoreFile sf : storeFiles) {
      byte[] startRow = startOf(sf), endRow = endOf(sf);
      // Validate the range and put the files into place.
      if (isInvalid(startRow) || isInvalid(endRow)) {
        insertFileIntoDPartition(level0Files, sf); // No metadata - goes to L0.
        ensureLevel0Metadata(sf);
      } else if (!isOpen(startRow) && !isOpen(endRow) && nonOpenRowCompare(startRow, endRow) >= 0) {
        LOG.error("Unexpected metadata - start row [" + Bytes.toString(startRow) + "], end row ["
          + Bytes.toString(endRow) + "] in file [" + sf.getPath() + "], pushing to L0");
        insertFileIntoDPartition(level0Files, sf); // Bad metadata - goes to L0 also.
        ensureLevel0Metadata(sf);
      } else {
        ArrayList<HStoreFile> stripe = candidateStripes.get(endRow);
        if (stripe == null) {
          stripe = new ArrayList<>();
          candidateStripes.put(endRow, stripe);
        }
        insertFileIntoDPartition(stripe, sf);
      }
    }

    boolean hasOverlaps = false;
    byte[] expectedStartRow = null; // first stripe can start wherever
    Iterator<Map.Entry<byte[], ArrayList<HStoreFile>>> entryIter =
      candidateStripes.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<byte[], ArrayList<HStoreFile>> entry = entryIter.next();
      ArrayList<HStoreFile> files = entry.getValue();
      // Validate the file start rows, and remove the bad ones to level 0.
      for (int i = 0; i < files.size(); ++i) {
        HStoreFile sf = files.get(i);
        byte[] startRow = startOf(sf);
        if (expectedStartRow == null) {
          expectedStartRow = startRow; // ensure that first stripe is still consistent
        } else if (!rowEquals(expectedStartRow, startRow)) {
          hasOverlaps = true;
          LOG.warn("Store file doesn't fit into the tentative stripes - expected to start at ["
            + Bytes.toString(expectedStartRow) + "], but starts at [" + Bytes.toString(startRow)
            + "], to L0 it goes");
          HStoreFile badSf = files.remove(i);
          insertFileIntoDPartition(level0Files, badSf);
          ensureLevel0Metadata(badSf);
          --i;
        }
      }
      // Check if any files from the candidate stripe are valid. If so, add a stripe.
      byte[] endRow = entry.getKey();
      if (!files.isEmpty()) {
        expectedStartRow = endRow; // Next stripe must start exactly at that key.
      } else {
        entryIter.remove();
      }
    }

    // In the end, there must be open ends on two sides. If not, and there were no errors i.e.
    // files are consistent, they might be coming from a split. We will treat the boundaries
    // as open keys anyway, and log the message.
    // If there were errors, we'll play it safe and dump everything into L0.
    if (!candidateStripes.isEmpty()) {
      HStoreFile firstFile = candidateStripes.firstEntry().getValue().get(0);
      boolean isOpen = isOpen(startOf(firstFile)) && isOpen(candidateStripes.lastKey());
      if (!isOpen) {
        LOG.warn("The range of the loaded files does not cover full key space: from ["
          + Bytes.toString(startOf(firstFile)) + "], to ["
          + Bytes.toString(candidateStripes.lastKey()) + "]");
        if (!hasOverlaps) {
          ensureEdgeStripeMetadata(candidateStripes.firstEntry().getValue(), true);
          ensureEdgeStripeMetadata(candidateStripes.lastEntry().getValue(), false);
        } else {
          LOG.warn("Inconsistent files, everything goes to L0.");
          for (ArrayList<HStoreFile> files : candidateStripes.values()) {
            for (HStoreFile sf : files) {
              insertFileIntoDPartition(level0Files, sf);
              ensureLevel0Metadata(sf);
            }
          }
          candidateStripes.clear();
        }
      }
    }

    // Copy the results into the fields.
    State state = new State();
    state.level0Files = ImmutableList.copyOf(level0Files);
    state.dpFiles = new ArrayList<>(candidateStripes.size());
    state.dpEndRows = new byte[Math.max(0, candidateStripes.size() - 1)][];
    ArrayList<HStoreFile> newAllFiles = new ArrayList<>(level0Files);
    int i = candidateStripes.size() - 1;
    for (Map.Entry<byte[], ArrayList<HStoreFile>> entry : candidateStripes.entrySet()) {
      state.dpFiles.add(ImmutableList.copyOf(entry.getValue()));
      newAllFiles.addAll(entry.getValue());
      if (i > 0) {
        state.dpEndRows[state.dpFiles.size() - 1] = entry.getKey();
      }
      --i;
    }
    state.allFilesCached = ImmutableList.copyOf(newAllFiles);
    this.state = state;
    debugDumpState("Files loaded");
  }

  private void debugDumpState(String string) {
    if (!LOG.isDebugEnabled()) return;
    StringBuilder sb = new StringBuilder();
    sb.append("\n" + string + "; current stripe state is as such:");
    sb.append("\n level 0 with ").append(state.level0Files.size())
      .append(" files: " + StringUtils.TraditionalBinaryPrefix
        .long2String(StripeCompactionPolicy.getTotalFileSize(state.level0Files), "", 1) + ";");
    for (int i = 0; i < state.dpFiles.size(); ++i) {
      String endRow = (i == state.dpEndRows.length)
        ? "(end)"
        : "[" + Bytes.toString(state.dpEndRows[i]) + "]";
      sb.append("\n stripe ending in ").append(endRow).append(" with ")
        .append(state.dpFiles.get(i).size())
        .append(" files: " + StringUtils.TraditionalBinaryPrefix.long2String(
          StripeCompactionPolicy.getTotalFileSize(state.dpFiles.get(i)), "", 1) + ";");
    }
    sb.append("\n").append(state.dpFiles.size()).append(" stripes total.");
    sb.append("\n").append(getStorefileCount()).append(" files total.");
    LOG.debug(sb.toString());
  }

  private void ensureEdgeStripeMetadata(ArrayList<HStoreFile> stripe, boolean isFirst) {
    HashMap<HStoreFile, byte[]> targetMap = isFirst ? fileStarts : fileEnds;
    for (HStoreFile sf : stripe) {
      targetMap.put(sf, OPEN_KEY);
    }
  }

  /**
   * Compare two keys for equality.
   */
  private boolean rowEquals(byte[] k1, byte[] k2) {
    return Bytes.equals(k1, 0, k1.length, k2, 0, k2.length);
  }

  private byte[] startOf(HStoreFile sf) {
    byte[] result = fileStarts.get(sf);

    // result and INVALID_KEY_IN_MAP are compared _only_ by reference on purpose here as the latter
    // serves only as a marker and is not to be confused with other empty byte arrays.
    // See Javadoc of INVALID_KEY_IN_MAP for more information
    return (result == null) ? sf.getMetadataValue(DP_START_KEY)
      : result == INVALID_KEY_IN_MAP ? INVALID_KEY
      : result;
  }

  private byte[] endOf(HStoreFile sf) {
    byte[] result = fileEnds.get(sf);

    // result and INVALID_KEY_IN_MAP are compared _only_ by reference on purpose here as the latter
    // serves only as a marker and is not to be confused with other empty byte arrays.
    // See Javadoc of INVALID_KEY_IN_MAP for more information
    return (result == null) ? sf.getMetadataValue(DP_END_KEY)
      : result == INVALID_KEY_IN_MAP ? INVALID_KEY
      : result;
  }

  /**
   * Checks whether the key is invalid (e.g. from an L0 file, or non-partition-compacted files).
   */
  private static final boolean isInvalid(byte[] key) {
    // No need to use Arrays.equals because INVALID_KEY is null
    return key == INVALID_KEY;
  }

  /**
   * Inserts a file in the correct place (by seqnum) in a partition copy.
   * @param dPartition dPartition copy to insert into.
   * @param sf     File to insert.
   */
  private static void insertFileIntoDPartition(ArrayList<HStoreFile> dPartition, HStoreFile sf) {
    // The only operation for which sorting of the files matters is KeyBefore. Therefore,
    // we will store the file in reverse order by seqNum from the outset.
    for (int insertBefore = 0;; ++insertBefore) {
      if (
        insertBefore == dPartition.size()
          || (StoreFileComparators.SEQ_ID.compare(sf, dPartition.get(insertBefore)) >= 0)
      ) {
        dPartition.add(insertBefore, sf);
        break;
      }
    }
  }

  private void ensureLevel0Metadata(HStoreFile sf) {
    if (!isInvalid(startOf(sf))) this.fileStarts.put(sf, INVALID_KEY_IN_MAP);
    if (!isInvalid(endOf(sf))) this.fileEnds.put(sf, INVALID_KEY_IN_MAP);
  }

  /**
   * Checks whether the key indicates an open interval boundary (i.e. infinity).
   */
  private static boolean isOpen(byte[] key) {
    return key != null && key.length == 0;
  }

  private static final boolean isOpen(Cell key) {
    return key != null && key.getRowLength() == 0;
  }

  /**
   * Compare two keys. Keys must not be open (isOpen(row) == false).
   */
  private final int nonOpenRowCompare(byte[] k1, byte[] k2) {
    assert !isOpen(k1) && !isOpen(k2);
    return Bytes.compareTo(k1, k2);
  }

  private final int nonOpenRowCompare(Cell k1, byte[] k2) {
    assert !isOpen(k1) && !isOpen(k2);
    return cellComparator.compareRows(k1, k2, 0, k2.length);
  }

  @Override
  public void insertNewFiles(Collection<HStoreFile> sfs) {
    CompactionOrFlushMergeCopy cmc = new CompactionOrFlushMergeCopy(true);
    cmc.mergeResults(Collections.emptyList(), sfs);
    debugDumpState("Added new files");
  }

  @Override
  public void addCompactionResults(Collection<HStoreFile> compactedFiles,
    Collection<HStoreFile> results) {
    // See class comment for the assumptions we make here.
    LOG.debug("Attempting to merge compaction results: " + compactedFiles.size()
      + " files replaced by " + results.size());
    // In order to be able to fail in the middle of the operation, we'll operate on lazy
    // copies and apply the result at the end.
    CompactionOrFlushMergeCopy cmc = new CompactionOrFlushMergeCopy(false);
    cmc.mergeResults(compactedFiles, results);
    markCompactedAway(compactedFiles);
    debugDumpState("Merged compaction results");
  }

  // Mark the files as compactedAway once the storefiles and compactedfiles list is finalised
  // Let a background thread close the actual reader on these compacted files and also
  // ensure to evict the blocks from block cache so that they are no longer in
  // cache
  private void markCompactedAway(Collection<HStoreFile> compactedFiles) {
    for (HStoreFile file : compactedFiles) {
      file.markCompactedAway();
    }
  }

  @Override
  public void removeCompactedFiles(Collection<HStoreFile> compactedFiles) {
    // See class comment for the assumptions we make here.
    LOG.debug("Attempting to delete compaction results: " + compactedFiles.size());
    // In order to be able to fail in the middle of the operation, we'll operate on lazy
    // copies and apply the result at the end.
    CompactionOrFlushMergeCopy cmc = new CompactionOrFlushMergeCopy(false);
    cmc.deleteResults(compactedFiles);
    debugDumpState("Deleted compaction results");
  }

  /**
   * Non-static helper class for merging compaction or flush results. Since we want to merge them
   * atomically (more or less), it operates on lazy copies, then creates a new state object and puts
   * it in place.
   */
  private class CompactionOrFlushMergeCopy {
    private ArrayList<List<HStoreFile>> dPFiles = null;
    private ArrayList<HStoreFile> level0Files = null;
    private ArrayList<byte[]> dPEndRows = null;

    private Collection<HStoreFile> compactedFiles = null;
    private Collection<HStoreFile> results = null;

    private List<HStoreFile> l0Results = new ArrayList<>();
    private final boolean isFlush;

    public CompactionOrFlushMergeCopy(boolean isFlush) {
      // Create a lazy mutable copy (other fields are so lazy they start out as nulls).
      this.dPFiles = new ArrayList<>(DPStoreFileManager.this.state.dpFiles);
      this.isFlush = isFlush;
    }

    private void mergeResults(Collection<HStoreFile> compactedFiles,
      Collection<HStoreFile> results) {
      assert this.compactedFiles == null && this.results == null;
      this.compactedFiles = compactedFiles;
      this.results = results;
      // Do logical processing.
      if (!isFlush) {
        removeCompactedFiles();
      }
      TreeMap<byte[], HStoreFile> newStripes = processResults();
      if (newStripes != null) {
        processNewCandidateStripes(newStripes);
      }
      // Create new state and update parent.
      DPStoreFileManager.State state = createNewState(false);
      DPStoreFileManager.this.state = state;
      updateMetadataMaps();
    }

    private void deleteResults(Collection<HStoreFile> compactedFiles) {
      this.compactedFiles = compactedFiles;
      // Create new state and update parent.
      DPStoreFileManager.State state = createNewState(true);
      DPStoreFileManager.this.state = state;
      updateMetadataMaps();
    }

    private DPStoreFileManager.State createNewState(boolean delCompactedFiles) {
      DPStoreFileManager.State oldState = DPStoreFileManager.this.state;
      // dPartition count should be the same unless the end rows changed.
      assert oldState.dpFiles.size() == this.dPFiles.size() || this.dPEndRows != null;
      DPStoreFileManager.State newState = new DPStoreFileManager.State();
      newState.level0Files =
        (this.level0Files == null) ? oldState.level0Files : ImmutableList.copyOf(this.level0Files);
      newState.dpEndRows = (this.dPEndRows == null)
        ? oldState.dpEndRows
        : this.dPEndRows.toArray(new byte[this.dPEndRows.size()][]);
      newState.dpFiles = new ArrayList<>(this.dPFiles.size());
      for (List<HStoreFile> newStripe : this.dPFiles) {
        newState.dpFiles.add(newStripe instanceof ImmutableList<?>
          ? (ImmutableList<HStoreFile>) newStripe
          : ImmutableList.copyOf(newStripe));
      }

      List<HStoreFile> newAllFiles = new ArrayList<>(oldState.allFilesCached);
      List<HStoreFile> newAllCompactedFiles = new ArrayList<>(oldState.allCompactedFilesCached);
      if (!isFlush) {
        newAllFiles.removeAll(compactedFiles);
        if (delCompactedFiles) {
          newAllCompactedFiles.removeAll(compactedFiles);
        } else {
          newAllCompactedFiles.addAll(compactedFiles);
        }
      }
      if (results != null) {
        newAllFiles.addAll(results);
      }
      newState.allFilesCached = ImmutableList.copyOf(newAllFiles);
      newState.allCompactedFilesCached = ImmutableList.copyOf(newAllCompactedFiles);
      return newState;
    }

    private void updateMetadataMaps() {
      DPStoreFileManager parent = DPStoreFileManager.this;
      if (!isFlush) {
        for (HStoreFile sf : this.compactedFiles) {
          parent.fileStarts.remove(sf);
          parent.fileEnds.remove(sf);
        }
      }
      if (this.l0Results != null) {
        for (HStoreFile sf : this.l0Results) {
          parent.ensureLevel0Metadata(sf);
        }
      }
    }

    /**
     * @param index Index of the dPartition we need.
     * @return A lazy dPartition copy from current dPartitions.
     */
    private final ArrayList<HStoreFile> getStripeCopy(int index) {
      List<HStoreFile> stripeCopy = this.dPFiles.get(index);
      ArrayList<HStoreFile> result = null;
      if (stripeCopy instanceof ImmutableList<?>) {
        result = new ArrayList<>(stripeCopy);
        this.dPFiles.set(index, result);
      } else {
        result = (ArrayList<HStoreFile>) stripeCopy;
      }
      return result;
    }

    /** Returns A lazy L0 copy from current state. */
    private final ArrayList<HStoreFile> getLevel0Copy() {
      if (this.level0Files == null) {
        this.level0Files = new ArrayList<>(DPStoreFileManager.this.state.level0Files);
      }
      return this.level0Files;
    }

    /**
     * Process new files, and add them either to the structure of existing stripes, or to the list
     * of new candidate stripes.
     * @return New candidate stripes.
     */
    private TreeMap<byte[], HStoreFile> processResults() {
      TreeMap<byte[], HStoreFile> newStripes = null;
      for (HStoreFile sf : this.results) {
        byte[] startRow = startOf(sf), endRow = endOf(sf);
        if (isInvalid(endRow) || isInvalid(startRow)) {
          if (!isFlush) {
            LOG.warn("The newly compacted file doesn't have stripes set: " + sf.getPath());
          }
          insertFileIntoDPartition(getLevel0Copy(), sf);
          this.l0Results.add(sf);
          continue;
        }
        if (!this.dPFiles.isEmpty()) {
          int stripeIndex = findStripeIndexByEndRow(endRow);
          if ((stripeIndex >= 0) && rowEquals(getStartRow(stripeIndex), startRow)) {
            // Simple/common case - add file to an existing stripe.
            insertFileIntoDPartition(getStripeCopy(stripeIndex), sf);
            continue;
          }
        }

        // Make a new candidate stripe.
        if (newStripes == null) {
          newStripes = new TreeMap<>(MAP_COMPARATOR);
        }
        HStoreFile oldSf = newStripes.put(endRow, sf);
        if (oldSf != null) {
          throw new IllegalStateException(
            "Compactor has produced multiple files for the stripe ending in ["
              + Bytes.toString(endRow) + "], found " + sf.getPath() + " and " + oldSf.getPath());
        }
      }
      return newStripes;
    }

    /**
     * Remove compacted files.
     */
    private void removeCompactedFiles() {
      for (HStoreFile oldFile : this.compactedFiles) {
        byte[] oldEndRow = endOf(oldFile);
        List<HStoreFile> source;
        if (isInvalid(oldEndRow)) {
          source = getLevel0Copy();
        } else {
          int stripeIndex = findStripeIndexByEndRow(oldEndRow);
          if (stripeIndex < 0) {
            throw new IllegalStateException(
              "An allegedly compacted file [" + oldFile + "] does not belong"
                + " to a known stripe (end row - [" + Bytes.toString(oldEndRow) + "])");
          }
          source = getStripeCopy(stripeIndex);
        }
        if (!source.remove(oldFile)) {
          LOG.warn("An allegedly compacted file [{}] was not found", oldFile);
        }
      }
    }

    /**
     * See {@link #addCompactionResults(Collection, Collection)} - updates the stripe list with new
     * candidate stripes/removes old stripes; produces new set of stripe end rows.
     * @param newStripes New stripes - files by end row.
     */
    private void processNewCandidateStripes(TreeMap<byte[], HStoreFile> newStripes) {
      // Validate that the removed and added aggregate ranges still make for a full key space.
      boolean hasStripes = !this.dPFiles.isEmpty();
      this.dPEndRows =
        new ArrayList<>(Arrays.asList(DPStoreFileManager.this.state.dpEndRows));
      int removeFrom = 0;
      byte[] firstStartRow = startOf(newStripes.firstEntry().getValue());
      byte[] lastEndRow = newStripes.lastKey();
      if (!hasStripes && (!isOpen(firstStartRow) || !isOpen(lastEndRow))) {
        throw new IllegalStateException("Newly created stripes do not cover the entire key space.");
      }

      boolean canAddNewStripes = true;
      Collection<HStoreFile> filesForL0 = null;
      if (hasStripes) {
        // Determine which stripes will need to be removed because they conflict with new stripes.
        // The new boundaries should match old stripe boundaries, so we should get exact matches.
        if (isOpen(firstStartRow)) {
          removeFrom = 0;
        } else {
          removeFrom = findStripeIndexByEndRow(firstStartRow);
          if (removeFrom < 0) {
            throw new IllegalStateException("Compaction is trying to add a bad range.");
          }
          ++removeFrom;
        }
        int removeTo = findStripeIndexByEndRow(lastEndRow);
        if (removeTo < 0) {
          throw new IllegalStateException("Compaction is trying to add a bad range.");
        }
        // See if there are files in the stripes we are trying to replace.
        ArrayList<HStoreFile> conflictingFiles = new ArrayList<>();
        for (int removeIndex = removeTo; removeIndex >= removeFrom; --removeIndex) {
          conflictingFiles.addAll(this.dPFiles.get(removeIndex));
        }
        if (!conflictingFiles.isEmpty()) {
          // This can be caused by two things - concurrent flush into stripes, or a bug.
          // Unfortunately, we cannot tell them apart without looking at timing or something
          // like that. We will assume we are dealing with a flush and dump it into L0.
          if (isFlush) {
            long newSize = StripeCompactionPolicy.getTotalFileSize(newStripes.values());
            LOG.warn("Stripes were created by a flush, but results of size " + newSize
              + " cannot be added because the stripes have changed");
            canAddNewStripes = false;
            filesForL0 = newStripes.values();
          } else {
            long oldSize = StripeCompactionPolicy.getTotalFileSize(conflictingFiles);
            LOG.info(conflictingFiles.size() + " conflicting files (likely created by a flush) "
              + " of size " + oldSize + " are moved to L0 due to concurrent stripe change");
            filesForL0 = conflictingFiles;
          }
          if (filesForL0 != null) {
            for (HStoreFile sf : filesForL0) {
              insertFileIntoDPartition(getLevel0Copy(), sf);
            }
            l0Results.addAll(filesForL0);
          }
        }

        if (canAddNewStripes) {
          // Remove old empty stripes.
          int originalCount = this.dPFiles.size();
          for (int removeIndex = removeTo; removeIndex >= removeFrom; --removeIndex) {
            if (removeIndex != originalCount - 1) {
              this.dPEndRows.remove(removeIndex);
            }
            this.dPFiles.remove(removeIndex);
          }
        }
      }

      if (!canAddNewStripes) {
        return; // Files were already put into L0.
      }

      // Now, insert new stripes. The total ranges match, so we can insert where we removed.
      byte[] previousEndRow = null;
      int insertAt = removeFrom;
      for (Map.Entry<byte[], HStoreFile> newStripe : newStripes.entrySet()) {
        if (previousEndRow != null) {
          // Validate that the ranges are contiguous.
          assert !isOpen(previousEndRow);
          byte[] startRow = startOf(newStripe.getValue());
          if (!rowEquals(previousEndRow, startRow)) {
            throw new IllegalStateException("The new stripes produced by "
              + (isFlush ? "flush" : "compaction") + " are not contiguous");
          }
        }
        // Add the new stripe.
        ArrayList<HStoreFile> tmp = new ArrayList<>();
        tmp.add(newStripe.getValue());
        dPFiles.add(insertAt, tmp);
        previousEndRow = newStripe.getKey();
        if (!isOpen(previousEndRow)) {
          dPEndRows.add(insertAt, previousEndRow);
        }
        ++insertAt;
      }
    }
  }

  /**
   * Finds the stripe index by end row.
   */
  private int findStripeIndexByEndRow(byte[] endRow) {
    assert !isInvalid(endRow);
    if (isOpen(endRow)) return state.dpEndRows.length;
    return Arrays.binarySearch(state.dpEndRows, endRow, Bytes.BYTES_COMPARATOR);
  }

  @Override
  public ImmutableCollection<HStoreFile> clearFiles() {
    ImmutableCollection<HStoreFile> result = this.state.allFilesCached;
    this.state = new State();
    this.fileStarts.clear();
    this.fileEnds.clear();
    return result;
  }

  @Override
  public Collection<HStoreFile> clearCompactedFiles() {
    final ImmutableList<HStoreFile> result = this.state.allCompactedFilesCached;
    this.state = new State();
    return result;
  }

  @Override public Collection<HStoreFile> getStorefiles() {
    return this.state.allFilesCached;
  }

  @Override
  public byte[] getStartRow(int dpIndex) {
    return (dpIndex == 0 ? OPEN_KEY : this.state.dpEndRows[dpIndex - 1]);
  }

  @Override
  public byte[] getEndRow(int dpIndex) {
    return dpIndex == this.state.dpEndRows.length ? OPEN_KEY : this.state.dpEndRows[dpIndex];
  }

  @Override
  public List<byte[]> getDPBoundaries() {
    if (this.state.dpFiles.isEmpty()) {
      return Collections.emptyList();
    }
    ArrayList<byte[]> result = new ArrayList<>(this.state.dpEndRows.length + 2);
    result.add(OPEN_KEY);
    Collections.addAll(result, this.state.dpEndRows);
    result.add(OPEN_KEY);
    return result;
  }

  @Override
  public ArrayList<ImmutableList<HStoreFile>> getDPs() {
    return this.state.dpFiles;
  }

  @Override
  public int getDPCount() {
    return this.state.dpFiles.size();
  }

  @Override
  public Collection<HStoreFile> getCompactedfiles() {
    return this.state.allCompactedFilesCached;
  }

  @Override
  public int getStorefileCount() {
    return this.state.allFilesCached.size();
  }

  @Override
  public int getCompactedFilesCount() {
    return this.state.allCompactedFilesCached.size();
  }

  @Override
  public Collection<HStoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow) {
    if (state.dpFiles.isEmpty()) {
      return Collections.emptyList();
    }

    int firstPartition = findDPartitionForRow(startRow, true);
    int lastPartition = findDPartitionForRow(stopRow, false);
    assert firstPartition <= lastPartition;
    if (firstPartition == lastPartition) {
      return state.dpFiles.get(lastPartition); // There's just one stripe we need.
    }
    if (firstPartition == 0 && lastPartition == (state.dpFiles.size() - 1)) {
      return state.allFilesCached; // We need to read all files.
    }

    ConcatenatedLists<HStoreFile> result = new ConcatenatedLists<>();
    result.addAllSublists(state.dpFiles.subList(firstPartition, lastPartition + 1));
    return result;
  }

  /**
   * Finds the dPartition index for the dPartition containing a row provided externally for get/scan.
   */
  private int findDPartitionForRow(byte[] row, boolean isStart) {
    if (isStart && Arrays.equals(row, HConstants.EMPTY_START_ROW)) return 0;
    if (!isStart && Arrays.equals(row, HConstants.EMPTY_END_ROW)) {
      return state.dpFiles.size() - 1;
    }
    // If there's an exact match below, a partition ends at "row". Partition right boundary is
    // exclusive, so that means the row is in the next partition; thus, we need to add one to index.
    // If there's no match, the return value of binarySearch is (-(insertion point) - 1), where
    // insertion point is the index of the next greater element, or list size if none. The
    // insertion point happens to be exactly what we need, so we need to add one to the result.
    return Math.abs(Arrays.binarySearch(state.dpEndRows, row, Bytes.BYTES_COMPARATOR) + 1);
  }

  @Override
  public Iterator<HStoreFile> getCandidateFilesForRowKeyBefore(KeyValue targetKey) {
    KeyBeforeConcatenatedLists result = new KeyBeforeConcatenatedLists();
    // Order matters for this call.
    result.addSublist(state.level0Files);
    if (!state.dpFiles.isEmpty()) {
      int lastStripeIndex = findDPartitionForRow(CellUtil.cloneRow(targetKey), false);
      for (int stripeIndex = lastStripeIndex; stripeIndex >= 0; --stripeIndex) {
        result.addSublist(state.dpFiles.get(stripeIndex));
      }
    }
    return result.iterator();
  }

  /**
   * An extension of ConcatenatedLists that has several peculiar properties. First, one can cut the
   * tail of the logical list by removing last several sub-lists. Second, items can be removed thru
   * iterator. Third, if the sub-lists are immutable, they are replaced with mutable copies when
   * needed. On average KeyBefore operation will contain half the dPartitions as potential candidates,
   * but will quickly cut down on them as it finds something in the more likely ones; thus, the
   * above allow us to avoid unnecessary copying of a bunch of lists.
   */
  private static class KeyBeforeConcatenatedLists extends ConcatenatedLists<HStoreFile> {
    @Override
    public java.util.Iterator<HStoreFile> iterator() {
      return new KeyBeforeConcatenatedLists.Iterator();
    }

    public class Iterator extends ConcatenatedLists<HStoreFile>.Iterator {
      public ArrayList<List<HStoreFile>> getComponents() {
        return components;
      }

      public void removeComponents(int startIndex) {
        List<List<HStoreFile>> subList = components.subList(startIndex, components.size());
        for (List<HStoreFile> entry : subList) {
          size -= entry.size();
        }
        assert size >= 0;
        subList.clear();
      }

      @Override
      public void remove() {
        if (!this.nextWasCalled) {
          throw new IllegalStateException("No element to remove");
        }
        this.nextWasCalled = false;
        List<HStoreFile> src = components.get(currentComponent);
        if (src instanceof ImmutableList<?>) {
          src = new ArrayList<>(src);
          components.set(currentComponent, src);
        }
        src.remove(indexWithinComponent);
        --size;
        --indexWithinComponent;
        if (src.isEmpty()) {
          components.remove(currentComponent); // indexWithinComponent is already -1 here.
        }
      }
    }
  }

  @Override
  public Iterator<HStoreFile> updateCandidateFilesForRowKeyBefore(
    Iterator<HStoreFile> candidateFiles, KeyValue targetKey, Cell candidate) {
    KeyBeforeConcatenatedLists.Iterator original = (KeyBeforeConcatenatedLists.Iterator) candidateFiles;
    assert original != null;
    ArrayList<List<HStoreFile>> components = original.getComponents();
    for (int firstIrrelevant = 0; firstIrrelevant < components.size(); ++firstIrrelevant) {
      HStoreFile sf = components.get(firstIrrelevant).get(0);
      byte[] endKey = endOf(sf);
      // Entries are ordered as such: L0, then stripes in reverse order. We never remove
      // level 0; we remove the dPartition, and all subsequent ones, as soon as we find the
      // first one that cannot possibly have better candidates.
      if (!isInvalid(endKey) && !isOpen(endKey) && (nonOpenRowCompare(targetKey, endKey) >= 0)) {
        original.removeComponents(firstIrrelevant);
        break;
      }
    }
    return original;
  }

  @Override
  public Optional<byte[]> getSplitPoint() throws IOException {
    if (this.getStorefileCount() == 0) {
      return Optional.empty();
    }
    if (state.dpFiles.size() <= 1) {
      return getSplitPointFromAllFiles();
    }
    int leftIndex = -1, rightIndex = state.dpFiles.size();
    long leftSize = 0, rightSize = 0;
    long lastLeftSize = 0, lastRightSize = 0;
    while (rightIndex - 1 != leftIndex) {
      if (leftSize >= rightSize) {
        --rightIndex;
        lastRightSize = getDPartitionFilesSize(rightIndex);
        rightSize += lastRightSize;
      } else {
        ++leftIndex;
        lastLeftSize = getDPartitionFilesSize(leftIndex);
        leftSize += lastLeftSize;
      }
    }
    if (leftSize == 0 || rightSize == 0) {
      String errMsg = String.format(
        "Cannot split on a boundary - left index %d size %d, " + "right index %d size %d",
        leftIndex, leftSize, rightIndex, rightSize);
      debugDumpState(errMsg);
      LOG.warn(errMsg);
      return getSplitPointFromAllFiles();
    }
    double ratio = (double) rightSize / leftSize;
    if (ratio < 1) {
      ratio = 1 / ratio;
    }
    if (config.getMaxSplitImbalance() > ratio) {
      return Optional.of(state.dpEndRows[leftIndex]);
    }

    // If the difference between the sides is too large, we could get the proportional key on
    // the a stripe to equalize the difference, but there's no proportional key method at the
    // moment, and it's not extremely important.
    // See if we can achieve better ratio if we split the bigger side in half.
    boolean isRightLarger = rightSize >= leftSize;
    double newRatio = isRightLarger
      ? getMidStripeSplitRatio(leftSize, rightSize, lastRightSize)
      : getMidStripeSplitRatio(rightSize, leftSize, lastLeftSize);
    if (newRatio < 1) {
      newRatio = 1 / newRatio;
    }
    if (newRatio >= ratio) {
      return Optional.of(state.dpEndRows[leftIndex]);
    }
    LOG.debug("Splitting the stripe - ratio w/o split " + ratio + ", ratio with split " + newRatio
      + " configured ratio " + config.getMaxSplitImbalance());
    // OK, we may get better ratio, get it.
    return StoreUtils.getSplitPoint(state.dpFiles.get(isRightLarger ? rightIndex : leftIndex),
      cellComparator);
  }

  private double getMidStripeSplitRatio(long smallerSize, long largerSize, long lastLargerSize) {
    return (double) (largerSize - lastLargerSize / 2f) / (smallerSize + lastLargerSize / 2f);
  }

  private Optional<byte[]> getSplitPointFromAllFiles() throws IOException {
    ConcatenatedLists<HStoreFile> sfs = new ConcatenatedLists<>();
    sfs.addSublist(state.level0Files);
    sfs.addAllSublists(state.dpFiles);
    return StoreUtils.getSplitPoint(sfs, cellComparator);
  }

  /**
   * Gets the total size of all files in the dPartition.
   * @param dPartitionIndex dPartition index.
   * @return Size.
   */
  private long getDPartitionFilesSize(int dPartitionIndex) {
    long result = 0;
    for (HStoreFile sf : state.dpFiles.get(dPartitionIndex)) {
      result += sf.getReader().length();
    }
    return result;
  }

  @Override
  public int getStoreCompactionPriority() {
    // If there's only L0, do what the default store does.
    // If we are in critical priority, do the same - we don't want to trump all stores all
    // the time due to how many files we have.
    int fc = getStorefileCount();
    if (state.dpFiles.isEmpty() || (this.blockingFileCount <= fc)) {
      return this.blockingFileCount - fc;
    }
    // If we are in good shape, we don't want to be trumped by all other stores due to how
    // many files we have, so do an approximate mapping to normal priority range; L0 counts
    // for all stripes.
    int l0 = state.level0Files.size(), sc = state.dpFiles.size();
    int priority = (int) Math.ceil(((double) (this.blockingFileCount - fc + l0) / sc) - l0);
    return (priority <= HStore.PRIORITY_USER) ? (HStore.PRIORITY_USER + 1) : priority;
  }

  @Override
  public Collection<HStoreFile> getUnneededFiles(long maxTs, List<HStoreFile> filesCompacting) {
    // 1) We can never get rid of the last file which has the maximum seqid in a partition.
    // 2) Files that are not the latest can't become one due to (1), so the rest are fair game.
    State state = this.state;
    Collection<HStoreFile> expiredStoreFiles = null;
    for (ImmutableList<HStoreFile> dPartition : state.dpFiles) {
      expiredStoreFiles = findExpiredFiles(dPartition, maxTs, filesCompacting, expiredStoreFiles);
    }
    return findExpiredFiles(state.level0Files, maxTs, filesCompacting, expiredStoreFiles);
  }

  private Collection<HStoreFile> findExpiredFiles(ImmutableList<HStoreFile> dPartition, long maxTs,
    List<HStoreFile> filesCompacting, Collection<HStoreFile> expiredStoreFiles) {
    // Order by seqnum is reversed.
    for (int i = 1; i < dPartition.size(); ++i) {
      HStoreFile sf = dPartition.get(i);
      synchronized (sf) {
        long fileTs = sf.getReader().getMaxTimestamp();
        if (fileTs < maxTs && !filesCompacting.contains(sf)) {
          LOG.info("Found an expired store file: " + sf.getPath() + " whose maxTimestamp is "
            + fileTs + ", which is below " + maxTs);
          if (expiredStoreFiles == null) {
            expiredStoreFiles = new ArrayList<>();
          }
          expiredStoreFiles.add(sf);
        }
      }
    }
    return expiredStoreFiles;
  }

  @Override
  public double getCompactionPressure() {
    State stateLocal = this.state;
    if (stateLocal.allFilesCached.size() > blockingFileCount) {
      // just a hit to tell others that we have reached the blocking file count.
      return 2.0;
    }
    if (stateLocal.dpFiles.isEmpty()) {
      return 0.0;
    }
    int blockingFilePerStripe = blockingFileCount / stateLocal.dpFiles.size();
    // do not calculate L0 separately because data will be moved to stripe quickly and in most cases
    // we flush data to stripe directly.
    int delta = stateLocal.level0Files.isEmpty() ? 0 : 1;
    double max = 0.0;
    for (ImmutableList<HStoreFile> dPFile : stateLocal.dpFiles) {
      int dPFileCount = dPFile.size();
      double normCount = (double) (dPFileCount + delta - config.getStripeCompactMinFiles())
        / (blockingFilePerStripe - config.getStripeCompactMinFiles());
      if (normCount >= 1.0) {
        // This could happen if stripe is not split evenly. Do not return values that larger than
        // 1.0 because we have not reached the blocking file count actually.
        return 1.0;
      }
      if (normCount > max) {
        max = normCount;
      }
    }
    return max;
  }

  @Override
  public Comparator<HStoreFile> getStoreFileComparator() {
    return StoreFileComparators.SEQ_ID;
  }
}
