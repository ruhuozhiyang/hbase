package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class DPClusterAnalysis {
  private List<byte[]> inData = new ArrayList<>();
  private List<byte[]> initKernel = new ArrayList<>();
  private List<byte[]> dpBoundaries = new ArrayList<>();

  public void loadData(List<byte[]> data) {
    this.inData = data;
  }

  public boolean setKernel() {
    int num = 2;
    if (inData.size() == 0 || inData.size() < num) {
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
    return true;
  }

  public void kMeans() {
    System.out.println("initial kernel:" + new String(initKernel.get(0)) + " " + new String(initKernel.get(1)));
    while (true) {
      boolean stable = false;
      List<byte[]> newKernels = iteration(initKernel);
      final List<String> newKernelsString =
        newKernels.stream().map(ele -> new String(ele)).collect(Collectors.toList());
      final List<String> initKernelString =
        initKernel.stream().map(ele -> new String(ele)).collect(Collectors.toList());
      deBug(newKernels);
      if (newKernelsString.containsAll(initKernelString)) {
        stable = true;
      }
      if (stable) {
        break;
      } else {
        initKernel = newKernels;
      }
    }
  }

  public List<byte[]> getDpBoundaries() {
    List<byte[]> db = new ArrayList<>();
    int d = Math.abs(Bytes.compareTo(this.initKernel.get(0), this.initKernel.get(1))) / 2;
    byte[] r = this.initKernel.get(0);
    r[0] = (byte) ((r[0] & 0xFF) + d);
    db.add(r);
    byte[] l = this.initKernel.get(0);
    l[0] = (byte) ((l[0] & 0xFF) - d);
    db.add(l);
    return db;
  }

  private void deBug(List<byte[]> newKernels) {
    StringBuilder res = new StringBuilder();
    res.append("[");
//    for (int i = 0; i < initKernel.size(); i++) {
////      res.append(initKernel.get(i) == null ? "NULL" : initKernel.get(i));
//      res.append(initKernel.get(i) == null ? "NULL" : new String(initKernel.get(i)));
//      if (i < initKernel.size() - 1) {
//        res.append(",");
//      }
//    }
    res.append("]?=[");
    for (int i = 0; i < newKernels.size(); i++) {
//      res.append(newKernels.get(i) == null ? "NULL" : newKernels.get(i));
      res.append(newKernels.get(i) == null ? "NULL" : new String(newKernels.get(i)));
      if (i < newKernels.size() - 1) {
        res.append(",");
      }
    }
    res.append("]");
    System.out.println(res);
  }

  private List<byte[]> iteration(List<byte[]> kernels) {
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
//    Iterator<byte[]> i = clusters1.iterator();
//    byte[] min = i.next();
//    while (i.hasNext()) {
//      byte[] next = i.next();
//      if (Bytes.compareTo(next, min) < 0)
//        min = next;
//    }
//    Iterator<byte[]> j = clusters1.iterator();
//    byte[] max = j.next();
//    while (j.hasNext()) {
//      byte[] next = j.next();
//      if (Bytes.compareTo(next, max) > 0)
//        max = next;
//    }
//    System.out.println(min);
//    System.out.println("[" + new String(min) + "," + new String(max) + "]");
//    System.out.println("cluster1:" + clusters1.stream().map(ele -> new String(ele)).collect(Collectors.toList()));
//    System.out.println("cluster2:" + clusters2.stream().map(ele -> new String(ele)).collect(Collectors.toList()));
    return newKernels;
  }

  private int target2Kernel(byte[] rowKey, List<byte[]> kernels) {
    int index = 0;
    float smallDis = Integer.MAX_VALUE;
    for (int i = 0; i < kernels.size(); i++) {
//      final float dis = getSimilarityRatio(new String(rowKey), new String(kernels.get(i)));
      final int dis = get2RowKeysDistance(rowKey, kernels.get(i));
      if (dis < smallDis) {
        smallDis = dis;
        index = i;
      }
    }
    return index;
  }

  private int get2RowKeysDistance(byte[] a, byte[] b) {
//    return Bytes.compareTo(a, b);
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
//      System.out.println(rowKeysMeanByte[i] & 0xFF);
    }
    return rowKeysMeanByte;
  }

  private static String j(int length) {
//    String KeyString = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    String KeyString = "0123456789";
    int len = KeyString.length();
    StringBuffer sb = new StringBuffer();
    for(int i = 0; i < length; i++){
      sb.append(KeyString.charAt((int) Math.round(Math.random()*(len-1))));
    }
    return sb.toString();
  }

  private float getSimilarityRatio(String str, String target) {
    int d[][];
    int n = str.length();
    int m = target.length();
    int i, j, temp;
    char ch1, ch2;
    if (n == 0 || m == 0) {
      return 100.0F;
    }
    d = new int[n + 1][m + 1];
    for (i = 0; i <= n; i++) {
      d[i][0] = i;
    }
    for (j = 0; j <= m; j++) {
      d[0][j] = j;
    }
    for (i = 1; i <= n; i++) {
      ch1 = str.charAt(i - 1);
      for (j = 1; j <= m; j++) {
        ch2 = target.charAt(j - 1);
        if (ch1 == ch2 || ch1 == ch2 + 32 || ch1 + 32 == ch2) {
          temp = 0;
        } else {
          temp = 1;
        }
        d[i][j] = Math.min(Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + temp);
      }
    }
    return (1 - (float) d[n][m] / Math.max(str.length(), target.length())) * 100F;
  }

  public static void main(String[] args) {
    List<byte[]> data = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 1000; i++) {
      String a = j(random.nextInt(13));
//      System.out.println(a);
      data.add(Bytes.toBytes(a));
//      data.add(Bytes.toBytes(String.valueOf(i)));
    }
//    for (int i = 5000; i < 7000; i++) {
//      data.add(Bytes.toBytes(String.valueOf(i)));
//    }
//    for (int i = 8000; i < 10000; i++) {
//      data.add(Bytes.toBytes(String.valueOf(i)));
//    }
    DPClusterAnalysis  dpCA = new DPClusterAnalysis();
    dpCA.loadData(data);
    dpCA.setKernel();
    dpCA.kMeans();
    System.out.println(new String(dpCA.getDpBoundaries().get(0)));
    System.out.println(new String(dpCA.getDpBoundaries().get(1)));

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
