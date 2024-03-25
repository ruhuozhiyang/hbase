package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@InterfaceAudience.Private
public class DPTransitStoreArea {
  private volatile MutableSegment transitStoreArea;
  private List<Cell> transitStoreAreaSnapshot;
  private HStore store;
  private Configuration conf;
  private final ReentrantReadWriteLock atsLock = new ReentrantReadWriteLock();

  public DPTransitStoreArea(HStore store, Configuration conf) {
    this.store = store;
    this.conf = conf;
    this.transitStoreArea = SegmentFactory.instance().createMutableSegment(this.conf,
      this.store.getComparator(), null);
    this.transitStoreAreaSnapshot = new ArrayList<>();
  }

  public int getCellCount() {
    this.atsLock.readLock().lock();
    int cellCount = this.transitStoreArea.getCellsCount();
    this.atsLock.readLock().unlock();
    return cellCount;
  }

  public List<Cell> getTransitStoreAreaSnapshot() {
    this.atsLock.readLock().lock();
    final List<Cell> tSAS = new ArrayList<>(transitStoreAreaSnapshot);
    this.atsLock.readLock().unlock();
    return tSAS;
  }

  public void resetTransitStoreAreaSnapshot() {
    this.atsLock.writeLock().lock();
    this.transitStoreAreaSnapshot.clear();
    this.atsLock.writeLock().unlock();
  }

  public void add(Cell cell) {
    this.atsLock.readLock().lock();
    this.transitStoreArea.add(cell, false, null, false);
    this.atsLock.readLock().unlock();
  }

  public List<Cell> getAllCellsAndSnapShotAndReset() {
    List<Cell> result = new ArrayList<>();
    this.atsLock.writeLock().lock();
    for (Cell cell : this.transitStoreArea.getCellSet()) {
      result.add(cell);
    }
    this.transitStoreAreaSnapshot.addAll(result);
    this.transitStoreArea = SegmentFactory.instance().createMutableSegment(this.conf,
      store.getComparator(), null);
    this.atsLock.writeLock().unlock();
    return result;
  }
}
