package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@InterfaceAudience.Private
public class DPStoreFlusher extends StoreFlusher{
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreFlusher.class);
  private final Object flushLock = new Object();
  private final DPInformationProvider dPInformation;

  public DPStoreFlusher(Configuration conf, HStore store, DPStoreFileManager dPInformation) {
    super(conf, store);
    this.dPInformation = dPInformation;
  }

  private DPBoundaryMultiFileWriter.WriterFactory createWriterFactory(MemStoreSnapshot snapshot,
    Consumer<Path> writerCreationTracker) {
    return new DPBoundaryMultiFileWriter.WriterFactory() {
      @Override
      public StoreFileWriter createWriter() throws IOException {
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
      return result;
    }

    InternalScanner scanner = createScanner(snapshot.getScanners(), tracker);
    InternalScanner scannerForCA = createScanner(snapshot.getScanners(), tracker);

    DPFlushRequest req = selectFlush(scannerForCA, store.getComparator(), this.dPInformation);

    boolean success = false;
    DPBoundaryMultiFileWriter mw = null;
    try {
      mw = req.createWriter(); // Writer according to the policy.
      DPBoundaryMultiFileWriter.WriterFactory factory =
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

  public DPFlushRequest selectFlush(InternalScanner scanner, CellComparator cellComparator,
    DPInformationProvider dip) {
    if (dip.getDPCount() == 0) {
      return new GenBoundaryAndDPFlushRequest(cellComparator, scanner);
    }
    return new BoundaryDPFlushRequest(cellComparator, dip.getDPBoundaries());
  }

  public static abstract class DPFlushRequest {
    protected final CellComparator cellComparator;

    public DPFlushRequest(CellComparator cellComparator) {
      this.cellComparator = cellComparator;
    }

    public abstract DPBoundaryMultiFileWriter createWriter() throws IOException;
  }

  public static class GenBoundaryAndDPFlushRequest extends DPFlushRequest {
    private InternalScanner scanner;

    public GenBoundaryAndDPFlushRequest(CellComparator cellComparator, InternalScanner scanner) {
      super(cellComparator);
      this.scanner = scanner;
    }

    @Override
    public DPBoundaryMultiFileWriter createWriter() throws IOException {
      List<byte[]> dpBoundaries = doCA2GetDPBoundaries();
      LOG.info("Get dpBoundaries:{} by DP Cluster Analysis.", dpBoundaries.toString());
      return new DPBoundaryMultiFileWriter(cellComparator, dpBoundaries,
        null, null);
    }

    private List<byte[]> doCA2GetDPBoundaries() throws IOException {
      List<Cell> kvs = new ArrayList<>();
      List<byte[]> rowKeys = new ArrayList<>();

      boolean hasMore;
      int flagForDebug = 0;
      do {
        hasMore = scanner.next(kvs);
        if (!kvs.isEmpty()) {
          for (Cell cell : kvs) {
            byte[] rowArray = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            if (flagForDebug < 3) {
              LOG.info("Key String:{}", Bytes.toString(rowArray));
            }
            rowKeys.add(rowArray);
          }
          kvs.clear();
        }
        ++flagForDebug;
      } while (hasMore);

      DPClusterAnalysis dpCA = new DPClusterAnalysis();
      dpCA.loadData(rowKeys);
      dpCA.setKernels();
      dpCA.kMeans();
      return dpCA.getDpBoundaries();
    }
  }

  /**
   * Dynamic partition flush request wrapper based on boundaries.
   */
  public static class BoundaryDPFlushRequest extends DPFlushRequest {
    private final List<byte[]> targetBoundaries;

    /**
     * @param cellComparator used to compare cells.
     * @param targetBoundaries New files should be written with these boundaries.
     */
    public BoundaryDPFlushRequest(CellComparator cellComparator, List<byte[]> targetBoundaries) {
      super(cellComparator);
      this.targetBoundaries = targetBoundaries;
    }

    @Override
    public DPBoundaryMultiFileWriter createWriter() throws IOException {
      DPClusterAnalysis.boundariesExpansion(targetBoundaries);
      LOG.info("Expansion dpBoundaries:{}.", targetBoundaries);
      return new DPBoundaryMultiFileWriter(cellComparator, targetBoundaries,
        null, null);
    }
  }
}
