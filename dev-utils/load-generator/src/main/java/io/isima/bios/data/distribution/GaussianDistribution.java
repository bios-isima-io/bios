/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.isima.bios.data.distribution;

import static java.lang.Math.PI;
import static java.lang.Math.cos;
import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.lang.Math.random;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

import java.util.Locale;
import java.util.function.DoubleSupplier;
import java.util.stream.DoubleStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GaussianDistribution implements DoubleSupplier {
  private static Logger logger = LoggerFactory.getLogger(GaussianDistribution.class);

  private double mu, sigma;
  private double[] state = new double[2];
  private int index = state.length;

  GaussianDistribution(double m, double s) {
    mu = m;
    sigma = s;
  }

  static double[] meanStdDev(double[] numbers) {
    if (numbers.length == 0) {
      return new double[]{0.0, 0.0};
    }

    double sx = 0.0, sxx = 0.0;
    long n = 0;
    for (double x : numbers) {
      sx += x;
      sxx += pow(x, 2);
      n++;
    }

    return new double[]{sx / n, pow((n * sxx - pow(sx, 2)), 0.5) / n};
  }

  static String replicate(int n, String s) {
    return range(0, n + 1).mapToObj(i -> s).collect(joining());
  }

  static long[] getRequestSizeArray(double[] numbers, int duration, int peakRequestSize) {
    final int maxWidth = peakRequestSize;
    long[] bins = new long[duration];
    long[] requestArray = new long[duration];

    for (double x : numbers) {
      bins[(int) (x * bins.length)]++;
    }


    double maxFreq = stream(bins).max().getAsLong();
    logger.debug(String.format("Max Freq = %f", maxFreq));

    for (int i = 0; i < bins.length; i++) {
      requestArray[i] = (int) (bins[i] / maxFreq * maxWidth);
      logger.debug(String.format("%s%n", replicate((int) (bins[i] / maxFreq * maxWidth), "#")));
    }
    return requestArray;
  }

  @Override
  public double getAsDouble() {
    index++;
    if (index >= state.length) {
      double r = sqrt(-2 * log(random())) * sigma;
      double x = 2 * PI * random();
      state = new double[]{mu + r * sin(x), mu + r * cos(x)};
      index = 0;
    }
    return state[index];
  }

  public static long[] getRequestArray(int duration, int peakRequestSize) {
    Locale.setDefault(Locale.US);
    double[] data = DoubleStream.generate(new GaussianDistribution(0.0, 0.5)).limit(100_000)
            .toArray();
    double[] res = meanStdDev(data);
    return getRequestSizeArray(stream(data).map(a -> max(0.0, min(0.9999, a / 3 + 0.5))).toArray(),
            duration, peakRequestSize);
  }

}
