package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DPClusterAnalysis {
  private List<byte[]> inData = new ArrayList<>();
  private List<byte[]> initKernel = new ArrayList<>();
  private List<byte[]> dpBoundaries = new ArrayList<>();

  public void loadData(List<byte[]> data) {
    this.inData = data;
  }

  public boolean setKernel(int num) {
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
    while (true) {
      boolean con = true;
      List<byte[]> t = iteration(initKernel);
      for (int i = 0; i < t.size(); i++) {
        if (t.get(i) != initKernel.get(i))
          con = false;
      }
      if (con)
        break;
      else
        initKernel = t;
    }
  }

  public List<byte[]> getDpBoundaries() {
    return this.dpBoundaries;
  }

  private List<byte[]> iteration(List<byte[]> kernels) {
    List<byte[]> newKernels = new ArrayList<>(kernels.size());
    byte[][][] clusters = new byte[kernels.size()][][];
    for (int j = 0; j < inData.size(); j++) {
      byte[] rowKey = inData.get(j);
      int pos = target2Kernel(rowKey, kernels);
      clusters[pos][clusters[pos].length] = rowKey;
    }
    for (int i = 0; i < clusters.length; i++) {
      newKernels.add(getRowKeysMean(clusters[i]));
    }
    return newKernels;
  }

  private int target2Kernel(byte[] rowKey, List<byte[]> kernels) {
    int index = 0;
    int smallDis = Integer.MAX_VALUE;
    for (int i = 0; i < kernels.size(); i++) {
      final int dis = get2RowKeysDistance(rowKey, kernels.get(i));
      if (dis < smallDis) {
        smallDis = dis;
        index = i;
      }
    }
    return index;
  }

  private int get2RowKeysDistance(byte[] a, byte[] b) {
    return 0;
  }

  private byte[] getRowKeysMean(byte[][] rowKeys) {
    return null;
  }
}
