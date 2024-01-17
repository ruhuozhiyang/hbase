package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

@InterfaceAudience.Private
public class DPBoundaryMultiFileWriter extends AbstractMultiFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(DPBoundaryMultiFileWriter.class);
  protected final CellComparator cellComparator;
  protected List<StoreFileWriter> existingWriters;
  protected List<byte[]> boundaries;

  private StoreFileWriter writerForL0 = null;
  private StoreFileWriter currentWriter;
  private byte[] currentWriterEndKey;
  private Cell lastCell;
  private long cellsInCurrentWriter = 0;
  private int majorRangeFromIndex = -1, majorRangeToIndex = -1;
  private boolean hasAnyDPartitionWriter = false;

  public DPBoundaryMultiFileWriter(CellComparator cellComparator, List<byte[]> targetBoundaries,
    byte[] majorRangeFrom, byte[] majorRangeTo) throws IOException {
    this.cellComparator = cellComparator;
    this.boundaries = targetBoundaries;

    this.existingWriters = new ArrayList<>((this.boundaries.size() / 2) + 1);

    // "major" range (range for which all files are included) boundaries, if any,
    // must match some target boundaries, let's find them.
    assert (majorRangeFrom == null) == (majorRangeTo == null);
    if (majorRangeFrom != null) {
      this.majorRangeFromIndex = Arrays.equals(majorRangeFrom, DPStoreFileManager.OPEN_KEY)
        ? 0
        : Collections.binarySearch(this.boundaries, majorRangeFrom, Bytes.BYTES_COMPARATOR) / 2;
      this.majorRangeToIndex = Arrays.equals(majorRangeTo, DPStoreFileManager.OPEN_KEY)
        ? ((this.boundaries.size() / 2) - 1)
        : (Collections.binarySearch(this.boundaries, majorRangeTo, Bytes.BYTES_COMPARATOR) / 2);
      if (this.majorRangeFromIndex < 0 || this.majorRangeToIndex < 0) {
        throw new IOException("Major range does not match writer boundaries: ["
          + Bytes.toString(majorRangeFrom) + "] [" + Bytes.toString(majorRangeTo) + "]; from "
          + this.majorRangeFromIndex + " to " + this.majorRangeToIndex);
      }
    }
  }

  @Override
  public void append(Cell cell) throws IOException {
    if (this.writerForL0 == null) {
      this.writerForL0 = writerFactory.createWriter();
      this.existingWriters.add(this.writerForL0);
    }
    if (!checkWhetherInDPartitions(this.boundaries, cell)) {
      this.writerForL0.append(cell);
      return;
    }
    prepareWriterFor(cell);
    this.currentWriter.append(cell);
    this.lastCell = cell;
    ++this.cellsInCurrentWriter;
  }

  private boolean checkWhetherInDPartitions(List<byte[]> boundaries, Cell cell) {
    byte[] rowArray = Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    int i = Collections.binarySearch(boundaries, rowArray, Bytes.BYTES_COMPARATOR);
    if (i >= 0) {
      return i % 2 == 0 ? true : false;
    } else {
      return (Math.abs(i + 1) % 2) == 0 ? false : true;
    }
  }

  private void prepareWriterFor(Cell cell) throws IOException {
    if (currentWriter != null && !isCellAfterCurrentWriter(cell)) {
      return;
    }
    stopUsingCurrentWriter();
    while (isCellAfterCurrentWriter(cell)) {
      checkCanCreateWriter();
      createEmptyWriter();
    }
    checkCanCreateWriter();
    hasAnyDPartitionWriter = true;
    currentWriter = writerFactory.createWriter();
    existingWriters.add(currentWriter);
  }

  private boolean isCellAfterCurrentWriter(Cell cell) {
    return !Arrays.equals(currentWriterEndKey, DPStoreFileManager.OPEN_KEY)
      && (cellComparator.compareRows(cell, currentWriterEndKey, 0, currentWriterEndKey.length) >= 0);
  }

  private void stopUsingCurrentWriter() {
    if (this.currentWriter != null) {
      LOG.info("Stopping to use a writer after [" + Bytes.toString(this.currentWriterEndKey)
        + "] row; wrote out " + this.cellsInCurrentWriter + " kvs");
      cellsInCurrentWriter = 0;
    }
    currentWriter = null;
    currentWriterEndKey = ((existingWriters.size() - 1) == (boundaries.size() / 2))
      ? null
      : boundaries.get((2 * existingWriters.size()) - 1);
  }

  private void checkCanCreateWriter() throws IOException {
    int maxWriterCount = boundaries.size() / 2;
    assert (existingWriters.size() - 1) <= maxWriterCount;
    if ((existingWriters.size() - 1) >= maxWriterCount) {
      throw new IOException("Cannot create any more writers (created " + (existingWriters.size() - 1)
        + " out of " + maxWriterCount + " - row might be out of range of all valid writers");
    }
  }

  /**
   * Called if there are no cells for some dPartition.
   *
   * We need to have something in the writer list for this dPartition, so that writer-boundary list
   * indices correspond to each other.
   *
   * We can insert null in the writer list for that purpose, except in the following cases where we
   * actually need a file:
   * 1) If we are in range for which we are compacting all the files, we need to create an empty file
   * to preserve dPartition metadata.
   * 2) If we have not produced any file at all for this compactions, and this is the last chance
   * (the last dPartition), we need to preserve last seqNum (see also HBASE-6059).
   */
  private void createEmptyWriter() throws IOException {
    int index = existingWriters.size() - 1;
    boolean isInMajorRange = (index >= majorRangeFromIndex) && (index < majorRangeToIndex);
    boolean isLastWriter = !hasAnyDPartitionWriter && (index == ((boundaries.size() / 2) - 1));
    boolean needEmptyFile = isInMajorRange || isLastWriter;
    existingWriters.add(needEmptyFile ? writerFactory.createWriter() : null);
    hasAnyDPartitionWriter |= needEmptyFile;
    currentWriterEndKey = ((existingWriters.size() - 1) == (boundaries.size() / 2))
      ? null
      : boundaries.get((2 * existingWriters.size()) - 1);
  }

  @Override
  protected Collection<StoreFileWriter> writers() {
    return existingWriters;
  }

  @Override
  protected final void preCommitWriters() throws IOException {
    assert this.existingWriters != null;
    preCommitWritersInternal();
    assert (this.boundaries.size() / 2) == (this.existingWriters.size() - 1);
  }

  protected void preCommitWritersInternal() throws IOException {
    stopUsingCurrentWriter();
    while ((existingWriters.size() - 1) < (boundaries.size() / 2)) {
      createEmptyWriter();
    }
    if (lastCell != null) {
      sanityCheckRight(boundaries.get(boundaries.size() - 1), lastCell);
    }
  }

  @Override
  protected void preCloseWriter(StoreFileWriter writer) throws IOException {
    LOG.info("Write DPartition metadata for " + writer.getPath().toString());
    int index = existingWriters.indexOf(writer);
    if (index > 0) {
      index -= 1;
      writer.appendFileInfo(DPStoreFileManager.DP_START_KEY, boundaries.get(2 * index));
      writer.appendFileInfo(DPStoreFileManager.DP_END_KEY, boundaries.get(2 * index + 1));
    }
  }

  /**
   * Subclasses can call this method to make sure the last KV is within multi-writer range.
   * @param right The right boundary of the writer.
   */
  protected void sanityCheckRight(byte[] right, Cell cell) throws IOException {
    if (
      !Arrays.equals(DPStoreFileManager.OPEN_KEY, right)
        && cellComparator.compareRows(cell, right, 0, right.length) >= 0
    ) {
      String error = "The last row is higher or equal than the right boundary of ["
        + Bytes.toString(right) + "]: ["
        + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "]";
      LOG.error(error);
      throw new IOException(error);
    }
  }
}
