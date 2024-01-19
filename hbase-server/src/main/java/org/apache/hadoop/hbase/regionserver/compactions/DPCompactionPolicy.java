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
  private SWCompactionPolicy dPPolicy;
  private DPStoreConfig dpStoreConfig;

  public DPCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo,
    DPStoreConfig dpStoreConfig) {
    super(conf, storeConfigInfo);
    this.dpStoreConfig = dpStoreConfig;
    this.dPPolicy = new SWCompactionPolicy(conf, storeConfigInfo);
  }

  public List<HStoreFile> preSelectFilesForCoprocessor(DPInformationProvider di,
    List<HStoreFile> filesCompacting) {
    final ArrayList<HStoreFile> storefiles = new ArrayList<>(di.getStorefiles());
    storefiles.removeAll(filesCompacting);
    return storefiles;
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
    List<HStoreFile> filesCompacting, boolean isOffpeak) throws IOException {
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
      DPCompactionRequest result = selectSingleDPCompaction(di, true, isOffpeak);
      if (result != null) {
        return result;
      }
    }

    return selectSingleDPCompaction(di, false, isOffpeak);
  }

  protected DPCompactionRequest selectSingleDPCompaction(DPInformationProvider di, boolean includeL0,
    boolean isOffpeak) {
    ArrayList<ImmutableList<HStoreFile>> dPartitions = di.getDPs();
    int bqIndex = -1;
    List<HStoreFile> bqSelection = null;
    int dPCount = dPartitions.size();
    long bqTotalSize = -1;
    for (int i = 0; i < dPCount; ++i) {
      List<HStoreFile> selection = selectSimpleCompaction(dPartitions.get(i), isOffpeak, false);
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

    List<HStoreFile> dPfilesToCompact = new ArrayList<>(bqSelection);
    List<HStoreFile> l0Files = di.getLevel0Files();
    ConcatenatedLists<HStoreFile> sfs = new ConcatenatedLists<>();
    sfs.addSublist(dPfilesToCompact);
    if (includeL0) {
      sfs.addSublist(l0Files);
    }
    DPCompactionRequest req = new DPCompactionRequest(sfs, di.getDPBoundaries());

    if (dPfilesToCompact.size() == dPartitions.get(bqIndex).size() && includeL0) {
      req.setMajorRange(di.getStartRow(bqIndex), di.getEndRow(bqIndex));
    }
    req.getRequest().setOffPeak(isOffpeak);
    return req;
  }

  private List<HStoreFile> selectSimpleCompaction(List<HStoreFile> sfs, boolean isOffpeak,
    boolean forceCompact) {
    int minFilesLocal = this.dpStoreConfig.getDPartitionCompactMinFiles();
    List<HStoreFile> selected =
      this.dPPolicy.applyCompactionPolicy(sfs, false, isOffpeak, minFilesLocal);
    if (forceCompact && (selected == null || selected.isEmpty()) && !sfs.isEmpty()) {
      return this.dPPolicy.selectCompactFiles(sfs, isOffpeak);
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

  /**
   * The further wrapper of Dynamic-partition compaction request.
   */
  public static class DPCompactionRequest {
    private CompactionRequestImpl request;
    private final List<byte[]> dPBoundaries;
    protected byte[] majorRangeFromRow = null, majorRangeToRow = null;

    public DPCompactionRequest(CompactionRequestImpl request, List<byte[]> dPBoundaries) {
      this.request = request;
      this.dPBoundaries = dPBoundaries;
    }

    public DPCompactionRequest(Collection<HStoreFile> files, List<byte[]> targetBoundaries) {
      this(new CompactionRequestImpl(files), targetBoundaries);
    }

    /**
     * Executes the request against compactor (essentially, just calls correct overload of compact
     * method), to simulate more dynamic dispatch.
     * @param compactor Compactor.
     * @return result of compact(...)
     */
    public List<Path> execute(DPCompactor compactor, ThroughputController throughputController,
      User user, DPAreaOfTS ats) throws IOException {
      return compactor.compact(this.request, this.dPBoundaries, this.majorRangeFromRow,
        this.majorRangeToRow, throughputController, user, ats);
    }

    public List<Path> execute(DPCompactor compactor, ThroughputController throughputController,
      DPAreaOfTS ats) throws IOException {
      return execute(compactor, throughputController, null, ats);
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
     * Sets compaction "major range". Major range is the key range for which all the files are
     * included, so they can be treated like major-compacted files.
     * @param startRow Left boundary, inclusive.
     * @param endRow   Right boundary, exclusive.
     */
    public void setMajorRange(byte[] startRow, byte[] endRow) {
      this.majorRangeFromRow = startRow;
      this.majorRangeToRow = endRow;
    }
  }
}
