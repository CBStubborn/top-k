package me.allenlyu.experiments.utils;

import cern.jet.random.Distributions;
import cern.jet.random.engine.RandomEngine;

/**
 * ZipfDistribution
 *
 * @author Allen Jin
 * @date 2017/04/19
 */
public class ZipfDistribution {
    private int N; // number
    private int s; // screw

    public ZipfDistribution(int size, int skew) {
        this.N = size;
        this.s = skew;
    }

    public double H(int n, int s) { // Harmonic number
        if (n == 1) {
            return 1.0 / Math.pow(n, s);
        } else {
            return (1.0 / Math.pow(n, s)) + H(n - 1, s);
        }
    }

    public double f(int k) {
        return (1 / Math.pow(k, this.s)) / H(this.N, this.s);
    }

    public double cdf(int k) {
        return H(k, this.s) / H(this.N, this.s);
    }

//    public static void main(String[] args) {
//        int n = 10000;
//        double s = 2;
//        for (int i = 0; i < n; i++) {
//            int val = Distributions.nextZipfInt(s, RandomEngine.makeDefault());
//            System.out.println(val);
//        }
//
//    }
}