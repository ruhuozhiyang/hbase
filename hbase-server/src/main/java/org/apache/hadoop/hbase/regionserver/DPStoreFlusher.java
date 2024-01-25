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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.Consumer;

@InterfaceAudience.Private
public class DPStoreFlusher extends StoreFlusher{
  private static final Logger LOG = LoggerFactory.getLogger(DPStoreFlusher.class);
  private final Object flushLock = new Object();
  private final DPStoreFileManager dPFileManager;
  private final DPTransitStoreArea areaOfTransitStore;

  public DPStoreFlusher(Configuration conf, HStore store, DPStoreFileManager dPFileManager,
    DPTransitStoreArea areaOfTransitStore) {
    super(conf, store);
    this.dPFileManager = dPFileManager;
    this.areaOfTransitStore = areaOfTransitStore;
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

    DPFlushRequest req = selectFlush(scannerForCA, store.getComparator(), this.dPFileManager);

    boolean success = false;
    DPBoundaryMultiFileWriter mw = null;
    try {
      mw = req.createWriter(this.areaOfTransitStore); // Writer according to the policy.
      DPBoundaryMultiFileWriter.WriterFactory factory =
        createWriterFactory(snapshot, writerCreationTracker);
      StoreScanner storeScanner = (scanner instanceof StoreScanner) ? (StoreScanner) scanner : null;
      mw.init(storeScanner, factory);
      mw.initWriterForL0();

      synchronized (flushLock) {
        performFlush(scanner, mw, throughputController);
        if (req instanceof UpdateBoundaryAndDPFlushRequest) {
          ((UpdateBoundaryAndDPFlushRequest) req).flushTransitStoreArea(areaOfTransitStore, mw);
        }
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
        scannerForCA.close();
      } catch (IOException ex) {
        LOG.warn("Failed to close flush scanner, ignoring", ex);
      }
    }
    return result;
  }

  public DPFlushRequest selectFlush(InternalScanner scannerForCA, CellComparator cellComparator,
    DPStoreFileManager dPFileManager) {
    return dPFileManager.getDPCount() > 0
            ? new UpdateBoundaryAndDPFlushRequest(cellComparator, dPFileManager)
            : new GenBoundaryAndDPFlushRequest(cellComparator, scannerForCA);
  }

  public static abstract class DPFlushRequest {
    protected final CellComparator cellComparator;

    public DPFlushRequest(CellComparator cellComparator) {
      this.cellComparator = cellComparator;
    }

    public abstract DPBoundaryMultiFileWriter createWriter(DPTransitStoreArea areaOfTransitStore) throws IOException;
  }

  public static class GenBoundaryAndDPFlushRequest extends DPFlushRequest {
    private InternalScanner scannerForCA;

    public GenBoundaryAndDPFlushRequest(CellComparator cellComparator, InternalScanner scannerForCA) {
      super(cellComparator);
      this.scannerForCA = scannerForCA;
    }

    @Override
    public DPBoundaryMultiFileWriter createWriter(DPTransitStoreArea areaOfTransitStore) throws IOException {
      List<byte[]> newDpBoundaries = doCA2GenDPBoundaries();
      LOG.info("[Gen DPBoundaries] Gen dPBoundaries:{}, size:[{}].",
              newDpBoundaries.toString(), newDpBoundaries.size());
      return new DPBoundaryMultiFileWriter(cellComparator, newDpBoundaries,
        null, null, areaOfTransitStore);
    }

    private List<byte[]> doCA2GenDPBoundaries() throws IOException {
      List<Cell> bufferForScan = new ArrayList<>();
      List<byte[]> rowKeys = new ArrayList<>();

      boolean hasMore;
      int flagForDebug = 0;
      do {
        hasMore = scannerForCA.next(bufferForScan);
        if (!bufferForScan.isEmpty()) {
          for (Cell cell : bufferForScan) {
            byte[] rowKey = new byte[cell.getRowLength()];
            System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowKey, 0, cell.getRowLength());
            if (flagForDebug < 3) {
              LOG.info("[Gen DPBoundaries], Key Example[{}]:[{}].", flagForDebug + 1, Bytes.toString(rowKey));
            }
            rowKeys.add(rowKey);
          }
          bufferForScan.clear();
        }
        ++flagForDebug;
      } while (hasMore);

      DPClusterAnalysis dpCA = new DPClusterAnalysis();
      dpCA.loadRowKeys(rowKeys);
      dpCA.initKernels();
      dpCA.kMeans();
      dpCA.pruneToSetDPBoundaries();
      return dpCA.getDpBoundaries();
    }
  }

  /**
   * Dynamic-partition flush request wrapper based on boundaries.
   */
  public static class UpdateBoundaryAndDPFlushRequest extends DPFlushRequest {
    private DPStoreFileManager dPFileManager;

    /**
     * @param cellComparator used to compare cells.
     * @param dPFileManager New files should be written with these boundaries in dPFileManager.
     */
    public UpdateBoundaryAndDPFlushRequest(CellComparator cellComparator, DPStoreFileManager dPFileManager) {
      super(cellComparator);
      this.dPFileManager = dPFileManager;
    }

    public void flushTransitStoreArea(DPTransitStoreArea areaOfTransitStore,
      DPBoundaryMultiFileWriter mw) throws IOException {
      NavigableMap<Cell, Cell> sortedCellsInATS = areaOfTransitStore.getTransitStoreAreaSnapshot();
      if (sortedCellsInATS != null) {
        final Iterator<Cell> sortedCells = sortedCellsInATS.values().iterator();
        while (sortedCells.hasNext()) {
          mw.append(sortedCells.next());
        }
      }
      areaOfTransitStore.resetTransitStoreAreaSnapshot();
    }

    @Override
    public DPBoundaryMultiFileWriter createWriter(DPTransitStoreArea areaOfTransitStore) throws IOException {
      List<byte[]> newDpBoundaries;
      StringBuilder message = new StringBuilder();
      if (areaOfTransitStore.getCellCount() > 0) {
        message.append("[Update DPBoundaries],");
        LOG.info("[Update DPBoundaries],");
        newDpBoundaries = doCA2UpdateDPBoundaries(areaOfTransitStore, this.dPFileManager.getDPBoundaries());
        message.append(DPClusterAnalysis.serializeToString("Update DPBoundaries To:", newDpBoundaries));
      } else {
        newDpBoundaries = new ArrayList<>(this.dPFileManager.getDPBoundaries());
        message.append("The CellCount of areaOfTransitStore is 0, and Skip Updating DPBoundaries.");
      }
      LOG.info(message.toString());
      return new DPBoundaryMultiFileWriter(cellComparator, newDpBoundaries,
              null, null, areaOfTransitStore);
    }

    private List<byte[]> doCA2UpdateDPBoundaries(DPTransitStoreArea areaOfTransitStore, List<byte[]> oldBoundaries) {
      List<byte[]> rowKeys = new ArrayList<>();
      int countForDebug = 0;
      for (Cell cell : areaOfTransitStore.getAllCellsAndSnapShotAndReset()) {
        if (cell.getRowLength() > 0 && cell.getRowLength() < 24) {
          byte[] rowArray = new byte[cell.getRowLength()];
          System.arraycopy(cell.getRowArray(), cell.getRowOffset(), rowArray, 0, cell.getRowLength());
          if (countForDebug < 3) {
            LOG.info("[Update DPBoundaries], Key Example[{}]:[{}].", countForDebug + 1, Bytes.toString(rowArray));
          }
          rowKeys.add(rowArray);
          ++countForDebug;
        }
      }
      DPClusterAnalysis dpCA = new DPClusterAnalysis();
      dpCA.loadRowKeys(rowKeys);
      dpCA.initKernels();
      dpCA.kMeans();
      dpCA.setOldDPBoundaries(oldBoundaries);
      dpCA.pruneToSetDPBoundaries();
      return dpCA.getDpBoundaries();
    }
  }
}
