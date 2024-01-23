package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

@InterfaceAudience.Private
public class DPClusterAnalysis {
  private static final Logger LOG = LoggerFactory.getLogger(DPClusterAnalysis.class);
  private List<byte[]> inData = new ArrayList<>();
  private List<byte[]> initKernel = new DPArrayList<>();
  private List<byte[]> oldDPBoundaries = null;
  private List<byte[]> newDPBoundaries = null;
  private final static int compareIndex = 4;

  public void loadData(List<byte[]> data) {
    this.inData = data;
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

  public boolean initKernels() {
    int num = 2;
    if (inData.size() < num) {
      return false;
    }
    final Random randomGen = new Random();
    for (int i = 0; i < num; i++) {
      int tmp = randomGen.nextInt(inData.size());
      while (initKernel.contains(inData.get(tmp))) {
        tmp = randomGen.nextInt(inData.size());
      }
      initKernel.add(inData.get(tmp));
    }
    serializeKernelOrBoundary2Log(this.initKernel, "[Gen/Update DPBoundaries] Initial Kernels:");
    return true;
  }

  public void kMeans() {
    while (true) {
      int numOfIteration = 0;
      boolean stable = false;
      List<byte[]> newKernels = doIteration2UpdateKernels(this.initKernel);
      final List<String> newKernelsString =
        newKernels.stream().map(ele -> new String(ele)).collect(Collectors.toList());
      final List<String> preKernelString =
        this.initKernel.stream().map(ele -> new String(ele)).collect(Collectors.toList());
      if (newKernelsString.containsAll(preKernelString)) {
        stable = true;
      }
      if (stable || numOfIteration >= 50) {
        if (initKernel.size() == 2) {
          LOG.info("After K-Means, Get Kernels:[{}, {}]", new String(initKernel.get(0)), new String(initKernel.get(1)));
        }
        break;
      } else {
        this.initKernel = newKernels;
        ++numOfIteration;
      }
    }
  }

  public void prune2GetDPBoundaries() {
    if (this.initKernel.size() < 2) {
      StringBuilder message = new StringBuilder();
      message.append("The Num of Kernels Gotten through Cluster-Analysis is smaller than 2,");
      if (this.oldDPBoundaries != null) {
        this.newDPBoundaries = new ArrayList<>(this.oldDPBoundaries);
        message.append("Use the old DPBoundaries.");
      } else if (this.initKernel.size() == 1){
        byte[] originKernel = this.initKernel.get(0);
        byte[] leftBoundary = new byte[originKernel.length];
        System.arraycopy(originKernel, 0, leftBoundary, 0, originKernel.length);
        int nLeftBoundaryEle = (leftBoundary[compareIndex] & 0xFF) - 1;
        leftBoundary[compareIndex] = (byte) (nLeftBoundaryEle < 48 ? 48 : nLeftBoundaryEle);

        byte[] rightBoundary = new byte[originKernel.length];
        System.arraycopy(originKernel, 0, rightBoundary, 0, originKernel.length);
        int nRightBoundaryEle = (rightBoundary[compareIndex] & 0xFF) + 1;
        rightBoundary[compareIndex] = (byte) (nRightBoundaryEle > 57 ? 57 : nRightBoundaryEle);
        message.append("Kernels Num is 1, and prune to Get DPBoundaries.");
      }
      LOG.warn(message.toString());
      return;
    }
    final byte[] kernel1 = this.initKernel.get(0);
    final byte[] kernel2 = this.initKernel.get(1);
    byte[] o = Bytes.compareTo(kernel1, kernel2) < 0 ? kernel1 : kernel2;
    final int disTmp = Math.abs(Bytes.compareTo(kernel1, kernel2)) / 2;
    int kernelsDis = disTmp == 0 ? 1 : disTmp;

    byte[] l = new byte[o.length];
    System.arraycopy(o, 0, l, 0, o.length);
    int nl = (l[compareIndex] & 0xFF) - kernelsDis;
    l[compareIndex] = (byte) (nl < 48 ? 48 : nl);

    byte[] r = new byte[o.length];
    System.arraycopy(o, 0, r, 0, o.length);
    int nr = (r[compareIndex] & 0xFF) + kernelsDis;
    r[compareIndex] = (byte) (nr > 57 ? 57 : nr);

    if (oldDPBoundaries != null) {
      serializeKernelOrBoundary2Log(oldDPBoundaries, "[Update DPBoundaries] Previous DPBoundaries:");
      ArrayList<byte[]> newDPBoundaryPair = new ArrayList<>();
      newDPBoundaryPair.add(l);
      newDPBoundaryPair.add(r);
      serializeKernelOrBoundary2Log(newDPBoundaryPair, "[Update DPBoundaries] New DPBoundary Pair For Pruning:");
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
        int signal = 0;
        for (int i = startInsertPoint; i < endInsertPoint; ++i) {
          if (startInsertPoint % 2 == 0) {
            byte[] rig = this.oldDPBoundaries.get(startInsertPoint);
            byte[] rrig = new byte[rig.length];
            System.arraycopy(rig, 0, rrig, 0, rig.length);
            rrig[rrig.length - 1] = (byte) ((rrig[rrig.length - 1] & 0xFF) - 1);
            if (Bytes.compareTo(l, rrig) < 0) {
              this.newDPBoundaries.add(startInsertPoint, rrig);
              this.newDPBoundaries.add(startInsertPoint, l);
              signal = 1;
            }
          }
          if (i % 2 == 1) {
            byte[] resL, resR;
            byte[] lef = this.oldDPBoundaries.get(i);
            resL = new byte[lef.length];
            System.arraycopy(lef, 0, resL, 0, lef.length);
            resL[resL.length - 1] = (byte) ((resL[resL.length - 1] & 0xFF) + 1);
            if ((i == endInsertPoint - 1)) {
              resR = r;
            } else {
              byte[] rig = this.oldDPBoundaries.get(i + 1);
              resR = new byte[rig.length];
              System.arraycopy(rig, 0, resR, 0, rig.length);
              resR[resR.length - 1] = (byte) ((resR[resR.length - 1] & 0xFF) - 1);
            }
            if (Bytes.compareTo(resL, resR) < 0) {
              int insertIndex = signal == 1 ? i + 3 : i + 1;
              this.newDPBoundaries.add(insertIndex, resR);
              this.newDPBoundaries.add(insertIndex, resL);
              signal = 1;
            }
          }
        }
      }
    } else {
      this.newDPBoundaries = new DPArrayList<>();
      this.newDPBoundaries.add(l);
      this.newDPBoundaries.add(r);
    }
  }

  public List<byte[]> getDpBoundaries() {
    return this.newDPBoundaries;
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

  private static void serializeKernelOrBoundary2Log(List<byte[]> kernelOrBoundary, String message) {
    StringBuilder res = new StringBuilder();
    res.append(message).append("[");
    for (int i = 0; i < kernelOrBoundary.size(); i++) {
      res.append(kernelOrBoundary.get(i) == null ? "NULL" : new String(kernelOrBoundary.get(i)));
      if (i < kernelOrBoundary.size() - 1) {
        res.append(",");
      }
    }
    res.append("]");
    LOG.info(res.toString());
  }

  private List<byte[]> doIteration2UpdateKernels(List<byte[]> kernels) {
    List<byte[]> newKernels = new ArrayList<>(kernels.size());
    List<byte[]> clusters1 = new ArrayList<>();
    List<byte[]> clusters2 = new ArrayList<>();
    int max1 = Integer.MIN_VALUE;
    int max2 = Integer.MIN_VALUE;
    for (int j = 0; j < inData.size(); j++) {
      byte[] rowKey = inData.get(j);
      int pos = target2Kernel(rowKey, kernels);
      if (pos == 0) {
        if (rowKey.length > max1) {
          max1 = rowKey.length;
        }
        clusters1.add(rowKey);
      } else {
        if (rowKey.length > max2) {
          max2 = rowKey.length;
        }
        clusters2.add(rowKey);
      }
    }
    final byte[] rowKeysMean1 = getRowKeysMean(clusters1, max1);
    final byte[] rowKeysMean2= getRowKeysMean(clusters2, max2);
    if (rowKeysMean1 != null) {
      newKernels.add(rowKeysMean1);
    }
    if (rowKeysMean2 != null) {
      newKernels.add(rowKeysMean2);
    }
    return newKernels;
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

  private int target2Kernel(byte[] rowKey, List<byte[]> kernels) {
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

  public static void main(String[] args) {
    List<byte[]> data = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      String a = genStringForTest(random.nextInt(13) + 1);
      data.add(Bytes.toBytes(a));
      data.add(Bytes.toBytes(String.valueOf(i)));
    }
    for (int i = 5000; i < 7000; i++) {
      data.add(Bytes.toBytes(String.valueOf(i)));
    }
    for (int i = 8000; i < 10000; i++) {
      data.add(Bytes.toBytes(String.valueOf(i)));
    }
    DPClusterAnalysis dpCA = new DPClusterAnalysis();
    dpCA.loadData(data);
    dpCA.initKernels();
    dpCA.kMeans();
    dpCA.prune2GetDPBoundaries();
    serializeKernelOrBoundary2Log(dpCA.getDpBoundaries(), "CA Boundary:");

//    ArrayList<Integer> test = new ArrayList<>(2);
//    test.add(1);
//    System.out.println(test.get(0) + "|" + test.get(1)); // Exception

//    ArrayList<Integer> test = new ArrayList<>();
//    test.add(1);
//    test.add(5);
//    test.add(7);
//    test.add(9);
//    test.add(1, 2);
//    test.add(1,3);
//    System.out.println(test); // [1, 3, 2, 5, 7, 9]

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
