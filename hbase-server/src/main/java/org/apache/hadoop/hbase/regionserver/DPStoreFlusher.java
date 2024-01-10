package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.DPCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@InterfaceAudience.Private
public class DPStoreFlusher extends StoreFlusher{
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreFlusher.class);
  private final Object flushLock = new Object();
  private final DPCompactionPolicy policy;
  private final DPCompactionPolicy.DPInformationProvider stripes;
  public DPStoreFlusher(Configuration conf, HStore store, DPCompactionPolicy policy,
    DPStoreFileManager stripes) {
    super(conf, store);
    this.policy = policy;
    this.stripes = stripes;
  }

  private DPMultiFileWriter.WriterFactory createWriterFactory(MemStoreSnapshot snapshot,
    Consumer<Path> writerCreationTracker) {
    return new DPMultiFileWriter.WriterFactory() {
      @Override
      public StoreFileWriter createWriter() throws IOException {
        // XXX: it used to always pass true for includesTag, re-consider?
        return DPStoreFlusher.this.createWriter(snapshot, true, writerCreationTracker);
      }
    };
  }

  @Override public List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
    MonitoredTask status, ThroughputController throughputController, FlushLifeCycleTracker tracker,
    Consumer<Path> writerCreationTracker) throws IOException {
    List<Path> result = new ArrayList<>();
    int cellsCount = snapshot.getCellsCount();
    if (cellsCount == 0) {
      // don't flush if there are no entries
      return result;
    }

    InternalScanner scanner = createScanner(snapshot.getScanners(), tracker);

    // Let policy select flush method.
    DPStoreFlusher.DPFlushRequest req =
      this.policy.selectFlush(scanner, store.getComparator(), this.stripes);

    boolean success = false;
    DPMultiFileWriter mw = null;
    try {
      mw = req.createWriter(); // Writer according to the policy.
      DPMultiFileWriter.WriterFactory factory =
        createWriterFactory(snapshot, writerCreationTracker);
      StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
      mw.init(storeScanner, factory);

      synchronized (flushLock) {
        performFlush(scanner, mw, throughputController);
        result = mw.commitWriters(cacheFlushSeqNum, false);
        success = true;
      }
    } finally {
      if (!success && (mw != null)) {
        for (Path leftoverFile : mw.abortWriters()) {
          try {
            store.getFileSystem().delete(leftoverFile, false);
          } catch (Exception e) {
            LOG.error("Failed to delete a file after failed flush: " + e);
          }
        }
      }
      try {
        scanner.close();
      } catch (IOException ex) {
        LOG.warn("Failed to close flush scanner, ignoring", ex);
      }
    }
    return result;
  }

  public static class DPFlushRequest {
    protected final CellComparator comparator;
    protected InternalScanner scanner;

    public DPFlushRequest(CellComparator comparator) {
      this.comparator = comparator;
    }

    public DPFlushRequest(CellComparator comparator, InternalScanner scanner) {
      this.comparator = comparator;
      this.scanner = scanner;
    }

    public DPMultiFileWriter createWriter() throws IOException {
      List<byte[]> kvs = new ArrayList<>();
      DPClusterAnalysis dpCA = new DPClusterAnalysis();
      dpCA.loadData(kvs);
      dpCA.setKernel(1);

      final List<byte[]> dpBoundaries = getDPBoundariesByJLFX();
      return new DPMultiFileWriter.BoundaryMultiWriter(comparator, dpBoundaries, null,
        null);
    }
  }

  /** Dynamic partition flush request wrapper based on boundaries. */
  public static class BoundaryDPFlushRequest extends DPStoreFlusher.DPFlushRequest {
    private final List<byte[]> targetBoundaries;

    /** @param targetBoundaries New files should be written with these boundaries. */
    public BoundaryDPFlushRequest(CellComparator comparator, List<byte[]> targetBoundaries) {
      super(comparator);
      this.targetBoundaries = targetBoundaries;
    }

    @Override
    public DPMultiFileWriter createWriter() throws IOException {
      return new DPMultiFileWriter.BoundaryMultiWriter(comparator, targetBoundaries, null,
        null);
    }
  }
}
