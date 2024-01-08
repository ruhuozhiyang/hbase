package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.DPStoreFlusher;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.StripeStoreFlusher;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
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

  public DPCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  public boolean needsCompactions(DPInformationProvider pi, List<HStoreFile> filesCompacting) {
    // Approximation on whether we need compaction.
    return filesCompacting.isEmpty() && (StoreUtils.hasReferences(pi.getStorefiles()));
  }

  public DPCompactionRequest selectCompaction(DPInformationProvider pi,
    List<HStoreFile> filesCompacting, boolean isOffpeak) throws IOException {
    return null;
  }

  @Override public boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
    throws IOException {
    return false;
  }

  @Override public boolean throttleCompaction(long compactionSize) {
    return compactionSize > comConf.getThrottlePoint();
  }

  public DPStoreFlusher.DPFlushRequest selectFlush(CellComparator comparator,
    DPCompactionPolicy.DPInformationProvider di) {
    if (di.getDPCount() == 0) {
      // No stripes - get stripes by cluster analysis, derive KVs per stripe.
      return new DPStoreFlusher.DPFlushRequest(comparator);
    }
    // There are stripes - do according to the boundaries.
    return new DPStoreFlusher.BoundaryDPFlushRequest(comparator, di.getDPBoundaries());
  }

  /** Dynamic partition compaction request wrapper. */
  public static class DPCompactionRequest {
    private CompactionRequestImpl request;
    private final List<byte[]> targetBoundaries;
    protected byte[] majorRangeFromRow = null, majorRangeToRow = null;

    public DPCompactionRequest(CompactionRequestImpl request,
      List<byte[]> targetBoundaries) {
      this.request = request;
      this.targetBoundaries = targetBoundaries;
    }

    public DPCompactionRequest(Collection<HStoreFile> files,
      List<byte[]> targetBoundaries) {
      this(new CompactionRequestImpl(files), targetBoundaries);
    }

    public List<Path> execute(DPCompactor compactor, ThroughputController throughputController)
      throws IOException {
      return execute(compactor, throughputController, null);
    }

    /**
     * Executes the request against compactor (essentially, just calls correct overload of compact
     * method), to simulate more dynamic dispatch.
     * @param compactor Compactor.
     * @return result of compact(...)
     */
    public List<Path> execute(DPCompactor compactor,
      ThroughputController throughputController, User user) throws IOException {
      return compactor.compact(this.request, this.targetBoundaries, this.majorRangeFromRow,
        this.majorRangeToRow, throughputController, user);
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

  /** The information about partitions that the policy needs to do its stuff */
  public static interface DPInformationProvider {
    Collection<HStoreFile> getStorefiles();

    /**
     * Gets the start row for a given dynamic partition.
     * @param dpIndex dp index.
     * @return Start row. May be an open key.
     */
    byte[] getStartRow(int dpIndex);

    /**
     * Gets the end row for a given dynamic partition.
     * @param dpIndex dp index.
     * @return End row. May be an open key.
     */
    byte[] getEndRow(int dpIndex);

    /** Returns Level 0 files. */
    List<HStoreFile> getLevel0Files();

    /** Returns All dp boundaries; including the open ones on both ends. */
    List<byte[]> getDPBoundaries();

    /** Returns The dynamic partitions. */
    ArrayList<ImmutableList<HStoreFile>> getDPs();

    /** Returns dynamic partition count. */
    int getDPCount();
  }
}
