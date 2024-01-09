package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@InterfaceAudience.Private
public abstract class DPMultiFileWriter extends AbstractMultiFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(DPMultiFileWriter.class);
  protected final CellComparator comparator;
  protected List<StoreFileWriter> existingWriters;
  protected List<byte[]> boundaries;

  /** Whether to write dp metadata */
  private boolean doWriteDPMetadata = true;

  public DPMultiFileWriter(CellComparator comparator) {
    this.comparator = comparator;
  }

  public void setNoDPMetadata() {
    this.doWriteDPMetadata = false;
  }

  @Override
  protected Collection<StoreFileWriter> writers() {
    return existingWriters;
  }

  protected abstract void preCommitWritersInternal() throws IOException;

  @Override
  protected final void preCommitWriters() throws IOException {
    // do some sanity check here.
    assert this.existingWriters != null;
    preCommitWritersInternal();
    assert this.boundaries.size() == (this.existingWriters.size() + 1);
  }

  @Override
  protected void preCloseWriter(StoreFileWriter writer) throws IOException {
    if (doWriteDPMetadata) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Write stripe metadata for " + writer.getPath().toString());
      }
      int index = existingWriters.indexOf(writer);
      writer.appendFileInfo(DPStoreFileManager.DP_START_KEY, boundaries.get(index));
      writer.appendFileInfo(DPStoreFileManager.DP_END_KEY, boundaries.get(index + 1));
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip writing stripe metadata for " + writer.getPath().toString());
      }
    }
  }

  /**
   * Subclasses can call this method to make sure the first KV is within multi-writer range.
   * @param left The left boundary of the writer.
   * @param cell The cell whose row has to be checked.
   */
  protected void sanityCheckLeft(byte[] left, Cell cell) throws IOException {
    if (
      !Arrays.equals(DPStoreFileManager.OPEN_KEY, left)
        && comparator.compareRows(cell, left, 0, left.length) < 0
    ) {
      String error =
        "The first row is lower than the left boundary of [" + Bytes.toString(left) + "]: ["
          + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "]";
      LOG.error(error);
      throw new IOException(error);
    }
  }

  /**
   * Subclasses can call this method to make sure the last KV is within multi-writer range.
   * @param right The right boundary of the writer.
   */
  protected void sanityCheckRight(byte[] right, Cell cell) throws IOException {
    if (
      !Arrays.equals(DPStoreFileManager.OPEN_KEY, right)
        && comparator.compareRows(cell, right, 0, right.length) >= 0
    ) {
      String error = "The last row is higher or equal than the right boundary of ["
        + Bytes.toString(right) + "]: ["
        + Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()) + "]";
      LOG.error(error);
      throw new IOException(error);
    }
  }

  public static class BoundaryMultiWriter extends DPMultiFileWriter {
    private StoreFileWriter currentWriter;
    private byte[] currentWriterEndKey;

    private Cell lastCell;
    private long cellsInCurrentWriter = 0;
    private int majorRangeFromIndex = -1, majorRangeToIndex = -1;
    private boolean hasAnyWriter = false;

    /**
     * @param targetBoundaries The boundaries on which writers/files are separated.
     * @param majorRangeFrom   Major range is the range for which at least one file should be
     *                         written (because all files are included in compaction).
     *                         majorRangeFrom is the left boundary.
     * @param majorRangeTo     The right boundary of majorRange (see majorRangeFrom).
     */
    public BoundaryMultiWriter(CellComparator comparator, List<byte[]> targetBoundaries,
      byte[] majorRangeFrom, byte[] majorRangeTo) throws IOException {
      super(comparator);
      this.boundaries = targetBoundaries;
      this.existingWriters = new ArrayList<>(this.boundaries.size() - 1);
      // "major" range (range for which all files are included) boundaries, if any,
      // must match some target boundaries, let's find them.
      assert (majorRangeFrom == null) == (majorRangeTo == null);
      if (majorRangeFrom != null) {
        majorRangeFromIndex = Arrays.equals(majorRangeFrom, DPStoreFileManager.OPEN_KEY)
          ? 0
          : Collections.binarySearch(boundaries, majorRangeFrom, Bytes.BYTES_COMPARATOR);
        majorRangeToIndex = Arrays.equals(majorRangeTo, DPStoreFileManager.OPEN_KEY)
          ? boundaries.size()
          : Collections.binarySearch(boundaries, majorRangeTo, Bytes.BYTES_COMPARATOR);
        if (this.majorRangeFromIndex < 0 || this.majorRangeToIndex < 0) {
          throw new IOException("Major range does not match writer boundaries: ["
            + Bytes.toString(majorRangeFrom) + "] [" + Bytes.toString(majorRangeTo) + "]; from "
            + majorRangeFromIndex + " to " + majorRangeToIndex);
        }
      }
    }

    @Override
    public void append(Cell cell) throws IOException {
      if (currentWriter == null && existingWriters.isEmpty()) {
        // First append ever, do a sanity check.
        sanityCheckLeft(this.boundaries.get(0), cell);
      }
      prepareWriterFor(cell);
      currentWriter.append(cell);
      lastCell = cell; // for the sanity check
      ++cellsInCurrentWriter;
    }

    private boolean isCellAfterCurrentWriter(Cell cell) {
      return !Arrays.equals(currentWriterEndKey, DPStoreFileManager.OPEN_KEY)
        && (comparator.compareRows(cell, currentWriterEndKey, 0, currentWriterEndKey.length) >= 0);
    }

    @Override
    protected void preCommitWritersInternal() throws IOException {
      stopUsingCurrentWriter();
      while (existingWriters.size() < boundaries.size() - 1) {
        createEmptyWriter();
      }
      if (lastCell != null) {
        sanityCheckRight(boundaries.get(boundaries.size() - 1), lastCell);
      }
    }

    private void prepareWriterFor(Cell cell) throws IOException {
      if (currentWriter != null && !isCellAfterCurrentWriter(cell)) return; // Use same writer.

      stopUsingCurrentWriter();
      // See if KV will be past the writer we are about to create; need to add another one.
      while (isCellAfterCurrentWriter(cell)) {
        checkCanCreateWriter();
        createEmptyWriter();
      }
      checkCanCreateWriter();
      hasAnyWriter = true;
      currentWriter = writerFactory.createWriter();
      existingWriters.add(currentWriter);
    }

    /**
     * Called if there are no cells for some stripe. We need to have something in the writer list
     * for this stripe, so that writer-boundary list indices correspond to each other. We can insert
     * null in the writer list for that purpose, except in the following cases where we actually
     * need a file: 1) If we are in range for which we are compacting all the files, we need to
     * create an empty file to preserve stripe metadata. 2) If we have not produced any file at all
     * for this compactions, and this is the last chance (the last stripe), we need to preserve last
     * seqNum (see also HBASE-6059).
     */
    private void createEmptyWriter() throws IOException {
      int index = existingWriters.size();
      boolean isInMajorRange = (index >= majorRangeFromIndex) && (index < majorRangeToIndex);
      // Stripe boundary count = stripe count + 1, so last stripe index is (#boundaries minus 2)
      boolean isLastWriter = !hasAnyWriter && (index == (boundaries.size() - 2));
      boolean needEmptyFile = isInMajorRange || isLastWriter;
      existingWriters.add(needEmptyFile ? writerFactory.createWriter() : null);
      hasAnyWriter |= needEmptyFile;
      currentWriterEndKey = (existingWriters.size() + 1 == boundaries.size())
        ? null
        : boundaries.get(existingWriters.size() + 1);
    }

    private void checkCanCreateWriter() throws IOException {
      int maxWriterCount = boundaries.size() - 1;
      assert existingWriters.size() <= maxWriterCount;
      if (existingWriters.size() >= maxWriterCount) {
        throw new IOException("Cannot create any more writers (created " + existingWriters.size()
          + " out of " + maxWriterCount + " - row might be out of range of all valid writers");
      }
    }

    private void stopUsingCurrentWriter() {
      if (currentWriter != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stopping to use a writer after [" + Bytes.toString(currentWriterEndKey)
            + "] row; wrote out " + cellsInCurrentWriter + " kvs");
        }
        cellsInCurrentWriter = 0;
      }
      currentWriter = null;
      currentWriterEndKey = (existingWriters.size() + 1 == boundaries.size())
        ? null
        : boundaries.get(existingWriters.size() + 1);
    }
  }
}
