package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.stream.Collectors;

@InterfaceAudience.Private
public class DPClusterAnalysis {
  private static final Logger LOG = LoggerFactory.getLogger(DPClusterAnalysis.class);
  private List<byte[]> rowKeys = new ArrayList<>();
  private List<byte[]> kernels = new DPArrayList<>();
  private List<byte[]> oldDPBoundaries = null;
  private List<byte[]> newDPBoundaries = null;
  private final static int compareIndex = 4;

  public void loadRowKeys(List<byte[]> rowKeys) {
    this.rowKeys = rowKeys;
  }

  public void setOldDPBoundaries(List<byte[]> oldDPBoundaries) {
    this.oldDPBoundaries = new ArrayList<>(oldDPBoundaries);
  }

  private class DPArrayList<E> extends ArrayList<E> {
    @Override
    public int indexOf(Object o) {
      if (o instanceof byte[]) {
        Object[] elementData = toArray();
        if (o == null) {
          for (int i = 0; i < elementData.length; i++) {
            if (elementData[i]==null) {
              return i;
            }
          }
        } else {
          for (int i = 0; i < elementData.length; i++) {
            if (new String((byte[])o).equalsIgnoreCase(new String((byte[])elementData[i]))) {
              return i;
            }
          }
        }
      }
      return -1;
    }

    @Override public String toString() {
      Iterator<E> it = iterator();
      if (! it.hasNext())
        return "[]";
      StringBuilder sb = new StringBuilder();
      sb.append('[');
      for (;;) {
        E e = it.next();
        sb.append((e instanceof byte[]) ? new String((byte[]) e) : e);
        if (! it.hasNext())
          return sb.append(']').toString();
        sb.append(',').append(' ');
      }
    }
  }

  public void initKernels() {
    int numOfKernels = 2;
    if (rowKeys.size() < numOfKernels) {
      throw new RuntimeException("Row Keys are too few to Start CA.");
    }
    final Random randomGen = new Random();
    for (int i = 0; i < numOfKernels; i++) {
      int tmp = randomGen.nextInt(rowKeys.size());
      while (kernels.contains(rowKeys.get(tmp))) {
        tmp = randomGen.nextInt(rowKeys.size());
      }
      kernels.add(rowKeys.get(tmp));
    }
    LOG.info(serializeToString("[Gen/Update DPBoundaries] Initial Kernels:", this.kernels));
  }

  public void kMeans() {
    int numOfIteration = 0;
    while (true) {
      boolean stable = false;
      List<byte[]> newKernels = doIteration2UpdateKernels(this.kernels);
      final List<String> newKernelsString =
        newKernels.stream().map(ele -> new String(ele)).collect(Collectors.toList());
      final List<String> preKernelString =
        this.kernels.stream().map(ele -> new String(ele)).collect(Collectors.toList());
      if (newKernelsString.containsAll(preKernelString)) {
        stable = true;
      }
      if (stable || numOfIteration >= 50) {
        if (kernels.size() == 2) {
          LOG.info("After K-Means, Get Kernels:[{}, {}]", new String(kernels.get(0)), new String(
            kernels.get(1)));
        }
        break;
      } else {
        this.kernels = newKernels;
        ++numOfIteration;
      }
    }
  }

  public void pruneToSetDPBoundaries() {
    if (this.kernels.size() < 2) {
      StringBuilder message = new StringBuilder();
      message.append("The Num of Kernels Gotten through Cluster-Analysis is smaller than 2,");
      if (this.oldDPBoundaries != null) {
        this.newDPBoundaries = new ArrayList<>(this.oldDPBoundaries);
        message.append("Use the old DPBoundaries.");
      } else if (this.kernels.size() == 1) {
        byte[] originKernel = this.kernels.get(0);
        byte[] leftBoundary = new byte[originKernel.length];
        System.arraycopy(originKernel, 0, leftBoundary, 0, originKernel.length);
        int nLeftBoundaryEle = (leftBoundary[compareIndex] & 0xFF) - 1;
        leftBoundary[compareIndex] = (byte) (nLeftBoundaryEle < 48 ? 48 : nLeftBoundaryEle);

        byte[] rightBoundary = new byte[originKernel.length];
        System.arraycopy(originKernel, 0, rightBoundary, 0, originKernel.length);
        int nRightBoundaryEle = (rightBoundary[compareIndex] & 0xFF) + 1;
        rightBoundary[compareIndex] = (byte) (nRightBoundaryEle > 57 ? 57 : nRightBoundaryEle);
        message.append("Kernels Num is 1, and prune to Set DPBoundaries.");
      }
      LOG.warn(message.toString());
      return;
    }
    final byte[] kernel1 = this.kernels.get(0);
    final byte[] kernel2 = this.kernels.get(1);
    byte[] originKernel = Bytes.compareTo(kernel1, kernel2) < 0 ? kernel1 : kernel2;
    final int disTemp = Math.abs(Bytes.compareTo(kernel1, kernel2)) / 2;
    int kernelsDis = disTemp == 0 ? 1 : disTemp;

    byte[] l = new byte[originKernel.length];
    System.arraycopy(originKernel, 0, l, 0, originKernel.length);
    int nl = (l[compareIndex] & 0xFF) - kernelsDis;
    l[compareIndex] = (byte) (nl < 48 ? 48 : nl);

    byte[] r = new byte[originKernel.length];
    System.arraycopy(originKernel, 0, r, 0, originKernel.length);
    int nr = (r[compareIndex] & 0xFF) + kernelsDis;
    r[compareIndex] = (byte) (nr > 57 ? 57 : nr);

    if (oldDPBoundaries != null) {
      LOG.info(serializeToString("[Update DPBoundaries] Previous DPBoundaries:", oldDPBoundaries));
      final int startIndex = Collections.binarySearch(oldDPBoundaries, l, Bytes.BYTES_COMPARATOR);
      final int endIndex = Collections.binarySearch(oldDPBoundaries, r, Bytes.BYTES_COMPARATOR);
      LOG.info("[Update DPBoundaries], StartIndex of [{}] is [{}].", new String(l), startIndex);
      LOG.info("[Update DPBoundaries], EndIndex of [{}] is [{}].", new String(r), endIndex);
      this.newDPBoundaries = new ArrayList<>(this.oldDPBoundaries);
      if (startIndex >= 0 && endIndex >= 0) {
        return;
      }
      int startInsertPoint = Math.abs(startIndex + 1);
      int endInsertPoint = Math.abs(endIndex + 1);
      if ((startInsertPoint == endInsertPoint) && (startInsertPoint % 2 == 0)) {
        if (endInsertPoint == 0) {
          final byte[] boundary0 = this.newDPBoundaries.get(0);
          if ((boundary0[compareIndex] & 0xFF) <= 50) {
            byte[] boundary00 = new byte[boundary0.length];
            System.arraycopy(boundary0, 0, boundary00, 0, r.length);
            boundary00[boundary00.length - 1] = (byte) ((boundary00[boundary00.length - 1] & 0xFF) - 1);
            this.newDPBoundaries.add(endInsertPoint, boundary00);
            this.newDPBoundaries.add(startInsertPoint, DPStoreFileManager.OPEN_KEY);
            return;
          }
        }
        if (startInsertPoint == this.newDPBoundaries.size()) {
          final byte[] boundary0 = this.newDPBoundaries.get(this.newDPBoundaries.size() - 1);
          if ((boundary0[compareIndex] & 0xFF) >= 56) {
            byte[] boundary00 = new byte[boundary0.length];
            System.arraycopy(boundary0, 0, boundary00, 0, r.length);
            boundary00[boundary00.length - 1] = (byte) ((boundary00[boundary00.length - 1] & 0xFF) + 1);
            this.newDPBoundaries.add(endInsertPoint, DPStoreFileManager.OPEN_KEY);
            this.newDPBoundaries.add(startInsertPoint, boundary00);
            return;
          }
        }
        if (endIndex >= 0) {
          byte[] rr = new byte[r.length];
          System.arraycopy(r, 0, rr, 0, r.length);
          rr[rr.length - 1] = (byte) ((rr[rr.length - 1] & 0xFF) - 1);
          this.newDPBoundaries.add(endInsertPoint, rr);
        } else {
          this.newDPBoundaries.add(endInsertPoint, r);
        }
        if (startIndex >= 0) {
          byte[] ll = new byte[l.length];
          System.arraycopy(l, 0, ll, 0, l.length);
          ll[ll.length - 1] = (byte) ((ll[ll.length - 1] & 0xFF) + 1);
          this.newDPBoundaries.add(startInsertPoint, ll);
        } else {
          this.newDPBoundaries.add(startInsertPoint, l);
        }
      } else {
        List<Pair<Integer, byte[][]>> boundariesAdd = new ArrayList<>();
        for (int i = startInsertPoint; i < endInsertPoint; ++i) {
          if (startInsertPoint % 2 == 0) {
            byte[] rig = this.oldDPBoundaries.get(startInsertPoint);
            if (get2KernelDisIndexBased(l, rig, 4) < 2) {
              this.newDPBoundaries.set(startInsertPoint, l);
            } else {
              byte[] rrig = new byte[rig.length];
              System.arraycopy(rig, 0, rrig, 0, rig.length);
              rrig[rrig.length - 1] = (byte) ((rrig[rrig.length - 1] & 0xFF) - 1);
              if (Bytes.compareTo(l, rrig) < 0) {
                byte[][] boundaries = new byte[2][];
                boundaries[0] = l;
                boundaries[1] = rrig;
                boundariesAdd.add(new Pair<>(startInsertPoint, boundaries));
              }
            }
          }
          if (i % 2 == 1) {
            byte[] lef = this.oldDPBoundaries.get(i);
            byte[] rig = i < oldDPBoundaries.size() - 1
              ? this.oldDPBoundaries.get(i + 1)
              : null;
            if (rig != null && get2KernelDisIndexBased(lef, rig, 4) < 2) {
              byte[] llef = new byte[lef.length];
              System.arraycopy(lef, 0, llef, 0, lef.length);
              llef[llef.length - 1] = (byte) ((llef[llef.length - 1] & 0xFF) + 1);
              this.newDPBoundaries.set(i + 1, llef);
            } else {
              byte[] resL, resR;
              resL = new byte[lef.length];
              System.arraycopy(lef, 0, resL, 0, lef.length);
              resL[resL.length - 1] = (byte) ((resL[resL.length - 1] & 0xFF) + 1);
              if ((i == endInsertPoint - 1)) {
                if (get2KernelDisIndexBased(resL, r, 4) < 2) {
                  return;
                }
                resR = r;
              } else {
                resR = new byte[rig.length];
                System.arraycopy(rig, 0, resR, 0, rig.length);
                resR[resR.length - 1] = (byte) ((resR[resR.length - 1] & 0xFF) - 1);
              }
              if (Bytes.compareTo(resL, resR) < 0) {
                byte[][] boundaries = new byte[2][];
                boundaries[0] = resL;
                boundaries[1] = resR;
                boundariesAdd.add(new Pair<>(i + 1, boundaries));
              }
            }
          }
        }
        for (int i = 0; i < boundariesAdd.size(); i++) {
          final Pair<Integer, byte[][]> integerPair = boundariesAdd.get(i);
          this.newDPBoundaries.add(integerPair.getFirst() + (2 * i), integerPair.getSecond()[1]);
          this.newDPBoundaries.add(integerPair.getFirst() + (2 * i), integerPair.getSecond()[0]);
        }
      }
    } else {
      this.newDPBoundaries = new DPArrayList<>();
      this.newDPBoundaries.add(l);
      this.newDPBoundaries.add(r);
    }
  }

  private int get2KernelDisIndexBased(byte[] kernelA, byte[] kernelB, int index) {
    return Math.abs((kernelA[index] & 0xFF) - (kernelB[index] & 0xFF));
  }

  public List<byte[]> getDpBoundaries() {
    return this.newDPBoundaries;
  }

  private List<byte[]> doIteration2UpdateKernels(List<byte[]> kernels) {
    List<byte[]> newKernels = new ArrayList<>(kernels.size());
    List<byte[]> cluster1 = new ArrayList<>();
    List<byte[]> cluster2 = new ArrayList<>();
    int max1 = Integer.MIN_VALUE;
    int max2 = Integer.MIN_VALUE;
    for (int j = 0; j < rowKeys.size(); j++) {
      byte[] rowKey = rowKeys.get(j);
      int pos = classifyToKernel(rowKey, kernels);
      if (pos == 0) {
        if (rowKey.length > max1) {
          max1 = rowKey.length;
        }
        cluster1.add(rowKey);
      } else {
        if (rowKey.length > max2) {
          max2 = rowKey.length;
        }
        cluster2.add(rowKey);
      }
    }
    final byte[] rowKeysMean1 = getRowKeysMean(cluster1, max1);
    final byte[] rowKeysMean2= getRowKeysMean(cluster2, max2);
    if (rowKeysMean1 != null) {
      newKernels.add(rowKeysMean1);
    }
    if (rowKeysMean2 != null) {
      newKernels.add(rowKeysMean2);
    }
    return newKernels;
  }

  private int classifyToKernel(byte[] rowKey, List<byte[]> kernels) {
    int index = 0;
    float smallDis = Integer.MAX_VALUE;
    for (int i = 0; i < kernels.size(); i++) {
      int dis = get2RowKeysDistance(rowKey, kernels.get(i));
      if (dis < smallDis) {
        smallDis = dis;
        index = i;
      }
    }
    return index;
  }

  private int get2RowKeysDistance(byte[] a, byte[] b) {
    return Math.abs(Bytes.compareTo(a, b));
  }

  private byte[] getRowKeysMean(List<byte[]> rowKeys, int max) {
    if (rowKeys.size() == 0) return null;
    int[] rowKeysMeanInt = new int[max];
    byte[] rowKeysMeanByte = new byte[max];
    for (int i = 0; i < rowKeys.size(); i++) {
      for (int j = 0; j < max; j++) {
        if (j >= rowKeys.get(i).length) {
          rowKeysMeanInt[j] = rowKeysMeanInt[j] + 48;
        } else {
          rowKeysMeanInt[j] = rowKeysMeanInt[j] + (rowKeys.get(i)[j] & 0xFF);
        }
      }
    }
    for (int i = 0; i < rowKeysMeanInt.length; i++) {
      rowKeysMeanByte[i] = (byte) (rowKeysMeanInt[i] / rowKeys.size());
    }
    return rowKeysMeanByte;
  }

  public static String serializeToString(String message, List<byte[]> eles) {
    StringBuilder res = new StringBuilder();
    res.append(message);
    Iterator<byte[]> it = eles.iterator();
    if (!it.hasNext())
      return res.append("[]").toString();
    res.append("[");
    for (;;) {
      byte[] e = it.next();
      res.append(e == null ? "NULL" : new String(e));
      if (!it.hasNext())
        return res.append("]").toString();
      res.append(",");
    }
  }

  private static String genStringForTest(int length) {
    String numAndCharsSource = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    String numSource = "0123456789";
    int len = numSource.length();
    StringBuffer sb = new StringBuffer();
    for(int i = 0; i < length; i++){
      sb.append(numSource.charAt((int) Math.round(Math.random()*(len-1))));
    }
    return sb.toString();
  }

  @Deprecated
  private void printClusterInfoForTest(List<byte[]> cluster, int num) {
    Iterator<byte[]> i = cluster.iterator();
    byte[] min = i.next();
    while (i.hasNext()) {
      byte[] next = i.next();
      if (Bytes.compareTo(next, min) < 0)
        min = next;
    }
    Iterator<byte[]> j = cluster.iterator();
    byte[] max = j.next();
    while (j.hasNext()) {
      byte[] next = j.next();
      if (Bytes.compareTo(next, max) > 0)
        max = next;
    }
    System.out.println("Cluster" + num + " Range:[" + new String(min) + "," + new String(max) + "]");
    System.out.println("Cluster" + num + " Elements:" + cluster.stream().map(ele -> new String(ele)).collect(Collectors.toList()));
  }

  @Deprecated
  public static void boundariesExpansion(List<byte[]> targetBoundaries) {
    for (int i = 0; i < targetBoundaries.size(); i++) {
      if (i % 2 == 0) {
        if (i == 0) {
          final byte[] start = targetBoundaries.get(i);
          if (start == DPStoreFileManager.OPEN_KEY) {
            continue;
          }
          final byte[] end = targetBoundaries.get(i + 1);
          byte[] newStart;
          int ns = (start[compareIndex] & 0xFF) - 1;
          if (ns < 48) {
            newStart = DPStoreFileManager.OPEN_KEY;
            targetBoundaries.set(i, newStart);
            continue;
          } else {
            newStart = new byte[start.length];
            System.arraycopy(start, 0, newStart, 0, start.length);
            newStart[compareIndex] = (byte) ns;
          }
          if ((end[compareIndex] & 0xFF) - (newStart[compareIndex] & 0xFF) > 2) {
            continue;
          }
          targetBoundaries.set(i, newStart);
        } else {
          final byte[] preEnd = targetBoundaries.get(i - 1);
          final byte[] start = targetBoundaries.get(i);
          final byte[] end = targetBoundaries.get(i + 1);
          byte[] newStart = new byte[start.length];
          System.arraycopy(start, 0, newStart, 0, start.length);
          newStart[compareIndex] = (byte) ((start[compareIndex] & 0xFF) - 1);
          if ((newStart[compareIndex] & 0xFF) < 48 || Bytes.compareTo(newStart, preEnd) < 0) {
            byte[] nNewStart = new byte[preEnd.length];
            System.arraycopy(preEnd, 0, nNewStart, 0, preEnd.length);
            nNewStart[nNewStart.length - 1] = (byte) ((preEnd[preEnd.length - 1] & 0xFF) + 1);
            targetBoundaries.set(i, nNewStart);
          }
          if ((end[compareIndex] & 0xFF) - (newStart[compareIndex] & 0xFF) > 2) {
            continue;
          }
          targetBoundaries.set(i, newStart);
        }
      }
    }
  }

  public static void main(String[] args) {
//    List<byte[]> data = new ArrayList<>();
//    Random random = new Random();
//    for (int i = 0; i < 1000; i++) {
//      String a = genStringForTest(random.nextInt(13) + 1);
//      data.add(Bytes.toBytes(a));
////      data.add(Bytes.toBytes(String.valueOf(i)));
//    }
////    for (int i = 5000; i < 7000; i++) {
////      data.add(Bytes.toBytes(String.valueOf(i)));
////    }
////    for (int i = 8000; i < 10000; i++) {
////      data.add(Bytes.toBytes(String.valueOf(i)));
////    }
//    DPClusterAnalysis dpCA = new DPClusterAnalysis();
//    dpCA.loadRowKeys(data);
//    dpCA.initKernels();
//    dpCA.kMeans();
//    dpCA.pruneToSetDPBoundaries();
//    System.out.println(serializeToString("CA Boundary:", dpCA.getDpBoundaries()));
//    LOG.info(serializeToString("CA Boundary:", dpCA.getDpBoundaries()));

//    ArrayList<Integer> test = new ArrayList<>(2);
//    test.add(1);
//    System.out.println(test.get(0) + "|" + test.get(1)); // Exception

    ArrayList<Integer> test = new ArrayList<>();
    test.add(1);
    test.add(5);
    test.add(7);
    test.add(9);
    test.add(1, 2);
    test.add(1,3);
    System.out.println(test); // [1, 3, 2, 5, 7, 9]
    System.out.println(2);

//    byte[] test = null;
//    byte[] INVALID_KEY = null;
//    System.out.println(test == INVALID_KEY); //true

//    List<Integer> test = new ArrayList<>();
//    test.add(1);
//    test.add(2);
//    test.add(3);
//    final List<Integer> collect = test.stream().map(ele -> ele + 1).collect(Collectors.toList());
//    System.out.println(collect);

//    int a = 8;
//    int b = 8772;
//    int c = 9814;
//    byte[] aBytes = Bytes.toBytes(String.valueOf(a));
//    byte[] bBytes = Bytes.toBytes(String.valueOf(b));
//    byte[] cBytes = Bytes.toBytes(String.valueOf(c));
//    System.out.println(Bytes.compareTo(aBytes, bBytes));
//    System.out.println(Bytes.compareTo(aBytes, cBytes));

//    byte[] a = "a".getBytes();
//    byte[] b = "b".getBytes();
//    byte c = (byte) (((a[0] & 0xFF) + (b[0] & 0xFF)) / 2);
//    System.out.println(a.length);
//    System.out.println(c);

//    int a = 9939;
//    System.out.println(Bytes.toBytes(String.valueOf(a))[2] & 0xFF); //51

//    System.out.println(Math.abs(Bytes.compareTo(Bytes.toBytes(a), Bytes.toBytes(b))));
//    System.out.println(getSimilarityRatio("aaa", "abc"));
//    System.out.println(getSimilarityRatio("bbb", "abc"));
  }
}
