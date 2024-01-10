package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DPClusterAnalysis {
  private List<byte[]> inData = new ArrayList<>();
  private List<byte[]> initKernel = new ArrayList<>();

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

  public byte[] target2Kernel(byte[] rowKey, List<byte[]> kernels) {
    int index = 0;
    int smallDis = Integer.MAX_VALUE;
    for (int i = 0; i < kernels.size(); i++) {
      final int pointsDistance = get2PointsDistance(rowKey, kernels.get(i));
      if (pointsDistance < smallDis) {
        smallDis = pointsDistance;
        index = i;
      }
    }
    return kernels.get(index);
  }

  private int get2PointsDistance(byte[] a, byte[] b) {
    return 0;
  }


  public List<byte[]> oneStep(List<byte[]> kernels) {
    List<byte[]> newKernels = new ArrayList<>(kernels.size());
    for (int j = 0; j < inData.size(); j++) {
      final byte[] rowKey = inData.get(j);
      target2Kernel(rowKey, kernels);
    }


    byte[] suma = new byte[0];
    int al = 0;
    byte[] sumb;
    int bl = 0;

    for (int i = 0; i < inData.size(); i++) {
      final byte[] bytes = inData.get(i);
      if (t[4] == 1.0) {
        suma = suma + bytes;
        al++;
      } else if (t[4] == 2.0) {
        sumb = sumb + bytes;
        al++;
        bl++;
      }
    }

    newKernels.add(suma/ al);
    newKernels.add(sumb / bl);
    return newKernels;
  }

  public void KMeans() {
    while (true) {
      boolean con = true;
      List<byte[]> t = oneStep(initKernel);
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

  public static void show_category() {//打印出所有的样本的属性和所属类别
    for (int i = 0; i < data.size(); i++) {
      System.out.print((i + 1) + "  ");
      for (int j = 0; j < 5; j++) {
        System.out.print(data.get(i)[j] + "  ");
      }
      System.out.println();
    }
  }
}
