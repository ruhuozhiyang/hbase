package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactor;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DPStoreEngine extends
  StoreEngine<DPStoreFlusher, DPCompactionPolicy, DPCompactor, DPStoreFileManager>{
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreEngine.class);
  private DPStoreConfig config;
  @Override
  public boolean needsCompaction(List filesCompacting) {
    return this.compactionPolicy.needsCompactions(this.storeFileManager, filesCompacting);
  }

  @Override
  public CompactionContext createCompaction() throws IOException {
    return new DPCompaction();
  }

  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator cellComparator)
    throws IOException {
    this.config = new DPStoreConfig(conf, store);
  }

  private class DPCompaction extends CompactionContext {

    @Override
    public List<HStoreFile> preSelect(List<HStoreFile> filesCompacting) {
      return null;
    }

    @Override
    public boolean select(List<HStoreFile> filesCompacting, boolean isUserCompaction,
      boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      return false;
    }

    @Override
    public List<Path> compact(ThroughputController throughputController, User user)
      throws IOException {
      return null;
    }
  }
}
