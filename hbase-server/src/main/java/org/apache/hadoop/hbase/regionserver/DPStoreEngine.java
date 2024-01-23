package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactor;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DPStoreEngine extends
  StoreEngine<DPStoreFlusher, DPCompactionPolicy, DPCompactor, DPStoreFileManager>{
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreEngine.class);
  private DPStoreConfig config;
  private DPAreaOfTS areaOfTempSFiles;

  @Override
  public boolean needsCompaction(List filesCompacting) {
    return this.compactionPolicy.needsCompactions(this.storeFileManager, filesCompacting);
  }

  @Override
  public CompactionContext createCompaction() throws IOException {
    DPCompaction dpCompaction = new DPCompaction();
    dpCompaction.setAts(this.areaOfTempSFiles);
    return dpCompaction;
  }

  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator cellComparator)
    throws IOException {
    this.config = new DPStoreConfig(conf);
    this.areaOfTempSFiles = new DPAreaOfTS(store, conf);
    this.storeFileManager = new DPStoreFileManager(cellComparator, conf, this.config);
    this.storeFlusher = new DPStoreFlusher(conf, store, this.storeFileManager, this.areaOfTempSFiles);
    this.compactionPolicy = new DPCompactionPolicy(conf, store, this.config);
    this.compactor = new DPCompactor(conf, store);
  }

  private class DPCompaction extends CompactionContext {
    private DPCompactionPolicy.DPCompactionRequest dPRequest = null;
    private DPAreaOfTS areaOfTransitStore = null;

    public void setAts(DPAreaOfTS areaOfTransitStore) {
      this.areaOfTransitStore = areaOfTransitStore;
    }

    @Override
    public List<HStoreFile> preSelect(List<HStoreFile> filesCompacting) {
      return compactionPolicy.preSelectFilesForCoprocessor(storeFileManager, filesCompacting);
    }

    @Override
    public boolean select(List<HStoreFile> filesCompacting, boolean isUserCompaction,
      boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      this.dPRequest =
        compactionPolicy.selectCompaction(storeFileManager, filesCompacting, mayUseOffPeak);
      this.request = (this.dPRequest == null)
        ? new CompactionRequestImpl(new ArrayList<>())
        : this.dPRequest.getRequest();
      return this.dPRequest != null;
    }

    @Override public void forceSelect(CompactionRequestImpl request) {
      super.forceSelect(request);
      if (this.dPRequest != null) {
        this.dPRequest.setRequest(this.request);
      } else {
        LOG.warn("DPStore is forced to take an arbitrary file list and compact it.");
        this.dPRequest = compactionPolicy.createEmptyRequest(storeFileManager, this.request);
      }
    }

    @Override
    public List<Path> compact(ThroughputController throughputController, User user)
      throws IOException {
      Preconditions.checkArgument(this.dPRequest != null, "Cannot compact without selection");
      return this.dPRequest.execute(compactor, throughputController, user, this.areaOfTransitStore);
    }
  }
}
