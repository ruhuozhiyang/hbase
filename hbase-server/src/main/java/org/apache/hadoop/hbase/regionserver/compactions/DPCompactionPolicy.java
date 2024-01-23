package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.DPAreaOfTS;
import org.apache.hadoop.hbase.regionserver.DPInformationProvider;
import org.apache.hadoop.hbase.regionserver.DPStoreConfig;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@InterfaceAudience.Private
public class DPCompactionPolicy extends CompactionPolicy {
  private final static Logger LOG = LoggerFactory.getLogger(DPCompactionPolicy.class);
  private final ExploringCompactionPolicy compactionPolicyInDP;
  private final DPStoreConfig dpStoreConfig;

  public DPCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo,
    DPStoreConfig dpStoreConfig) {
    super(conf, storeConfigInfo);
    this.dpStoreConfig = dpStoreConfig;
    this.compactionPolicyInDP = new ExploringCompactionPolicy(conf, storeConfigInfo);
  }

  public List<HStoreFile> preSelectFilesForCoprocessor(DPInformationProvider di,
    List<HStoreFile> filesCompacting) {
    final ArrayList<HStoreFile> storeFiles = new ArrayList<>(di.getStorefiles());
    storeFiles.removeAll(filesCompacting);
    return storeFiles;
  }

  public DPCompactionRequest createEmptyRequest(DPInformationProvider di,
    CompactionRequestImpl request) {
    return new DPCompactionRequest(request, di.getDPBoundaries());
  }

  public boolean needsCompactions(DPInformationProvider di, List<HStoreFile> filesCompacting) {
    return filesCompacting.isEmpty() && (StoreUtils.hasReferences(di.getStorefiles())
      || needsSingleDPartitionCompaction(di));
  }

  /**
   * @param di StoreFileManager.
   * @return Whether any dPartition potentially needs compaction.
   */
  protected boolean needsSingleDPartitionCompaction(DPInformationProvider di) {
    int minFiles = this.dpStoreConfig.getDPartitionCompactMinFiles();
    for (List<HStoreFile> dPartition : di.getDPs()) {
      if (dPartition.size() >= minFiles) {
        LOG.info("Need to Single DPartition Compaction, Files Num:{}.", dPartition.size());
        return true;
      }
    }
    return false;
  }

  public DPCompactionRequest selectCompaction(DPInformationProvider di,
    List<HStoreFile> filesCompacting, boolean isOffPeak) throws IOException {
    if (!filesCompacting.isEmpty()) {
      LOG.info("Not selecting compaction: " + filesCompacting.size() + " files compacting");
      return null;
    }
    Collection<HStoreFile> allFiles = di.getStorefiles();
    if (StoreUtils.hasReferences(allFiles)) {
      LOG.debug("There are references in the store; compacting all files");
      DPCompactionRequest request =
        new DPCompactionRequest(allFiles, di.getDPBoundaries());
      request.getRequest().setAfterSplit(true);
      return request;
    }
    List<HStoreFile> l0Files = di.getLevel0Files();
    boolean shouldCompactL0 = this.dpStoreConfig.getLevel0MinFiles() <= l0Files.size();
    if (shouldCompactL0) {
      LOG.debug("Selecting L0 compaction with " + l0Files.size() + " files");
      DPCompactionRequest result = selectSingleDPCompaction(di, true, isOffPeak);
      if (result != null) {
        return result;
      }
    }
    return selectSingleDPCompaction(di, false, isOffPeak);
  }

  protected DPCompactionRequest selectSingleDPCompaction(DPInformationProvider di, boolean includeL0,
    boolean isOffPeak) {
    ArrayList<ImmutableList<HStoreFile>> dPartitions = di.getDPs();
    int bqIndex = -1;
    List<HStoreFile> bqSelection = null;
    int dPCount = dPartitions.size();
    long bqTotalSize = -1;
    for (int i = 0; i < dPCount; ++i) {
      List<HStoreFile> selection = selectSimpleCompaction(dPartitions.get(i), isOffPeak, false);
      if (selection.isEmpty()) continue;
      long size = 0;
      for (HStoreFile sf : selection) {
        size += sf.getReader().length();
      }
      if (
        bqSelection == null || selection.size() > bqSelection.size()
          || (selection.size() == bqSelection.size() && size < bqTotalSize)
      ) {
        bqSelection = selection;
        bqIndex = i;
        bqTotalSize = size;
      }
    }
    if (bqSelection == null) {
      LOG.debug("No good compaction is possible in any DPartition.");
      return null;
    }
    List<HStoreFile> dPFilesToCompact = new ArrayList<>(bqSelection);
    List<HStoreFile> l0Files = di.getLevel0Files();
    ConcatenatedLists<HStoreFile> sfs = new ConcatenatedLists<>();
    sfs.addSublist(dPFilesToCompact);
    if (includeL0) {
      sfs.addSublist(l0Files);
    }
    DPCompactionRequest req = new DPCompactionRequest(sfs, di.getDPBoundaries());
    if (dPFilesToCompact.size() == dPartitions.get(bqIndex).size() && includeL0) {
      req.setMajorRange(di.getStartRow(bqIndex), di.getEndRow(bqIndex));
    }
    req.getRequest().setOffPeak(isOffPeak);
    return req;
  }

  private List<HStoreFile> selectSimpleCompaction(List<HStoreFile> sfs, boolean isOffPeak,
    boolean forceCompact) {
    int minFilesLocal = this.dpStoreConfig.getDPartitionCompactMinFiles();
    int maxFilesLocal = Math.max(this.dpStoreConfig.getDPartitionCompactMaxFiles(), minFilesLocal);
    List<HStoreFile> selected =
      this.compactionPolicyInDP.applyCompactionPolicy(sfs, false, isOffPeak, minFilesLocal, maxFilesLocal);
    if (forceCompact && (selected == null || selected.isEmpty()) && !sfs.isEmpty()) {
      return this.compactionPolicyInDP.selectCompactFiles(sfs, maxFilesLocal, isOffPeak);
    }
    return selected;
  }

  @Override public boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
    throws IOException {
    return false;
  }

  @Override public boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  public static class DPCompactionRequest {
    private final List<byte[]> dPBoundaries;
    protected byte[] majorRangeFromRow = null, majorRangeToRow = null;
    private CompactionRequestImpl request;

    public DPCompactionRequest(CompactionRequestImpl request, List<byte[]> dPBoundaries) {
      this.request = request;
      this.dPBoundaries = dPBoundaries;
    }

    public DPCompactionRequest(Collection<HStoreFile> files, List<byte[]> dPBoundaries) {
      this(new CompactionRequestImpl(files), dPBoundaries);
    }

    /**
     * Executes the request against compactor.
     * Essentially, just calls correct overload of compact method.
     * To simulate more dynamic dispatch.
     * @param compactor Compactor.
     * @return result of the compacting process.
     */
    public List<Path> execute(DPCompactor compactor, ThroughputController throughputController,
      User user, DPAreaOfTS areaOfTransitStore) throws IOException {
      return compactor.compact(this.request, this.dPBoundaries, this.majorRangeFromRow,
        this.majorRangeToRow, throughputController, user, areaOfTransitStore);
    }

    public CompactionRequestImpl getRequest() {
      return this.request;
    }

    public void setRequest(CompactionRequestImpl request) {
      assert request != null;
      this.request = request;
      this.majorRangeFromRow = this.majorRangeToRow = null;
    }

    /**
     * Sets compaction "major range".
     * Major range is the key range for which all the files are included,
     * so they can be treated like major-compacted files.
     * @param startRow Left boundary, inclusive.
     * @param endRow   Right boundary, exclusive.
     */
    public void setMajorRange(byte[] startRow, byte[] endRow) {
      this.majorRangeFromRow = startRow;
      this.majorRangeToRow = endRow;
    }
  }
}
