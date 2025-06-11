'use strict';

export const PositiveIndicator = {
  UNKNOWN: 0,
  NEUTRAL: 1,
  HIGH: 2,
  LOW: 3,
}

export const UnitPosition = {
  UNKNOWN: 0,
  PREFIX: 1,
  SUFFIX: 2,
}

export const AttributeCategory = {
  UNKNOWN: 0,
  ADDITIVE: 1,
  GAUGE: 2,
  RATIO: 3,
  DIMENSION: 11,
  DESCRIPTION: 12,
}

export const InformationType = {
  UNKNOWN: 0,
  DISTINCT_COUNT: 1,
  SAMPLE_COUNTS: 2,
  FREQUENT_ITEMS: 3,
  COUNT: 21,
  SUM: 22,
  AVERAGE: 23,
  STATISTICS: 24,
  QUANTILES: 31,
  HIGH_PERCENTILES: 32,
  LOW_PERCENTILES: 33,
}

export const DataPoints = {
  UNKNOWN: 0,
  SINGLE: 1,
  MULTIPLE: 2,
  MULTIPLE_TIME_WINDOWS: 3,
}

export const ViewType = {
  UNKNOWN: 0,
  NUMBER: 1,
  TREND: 2,
  GRAPH: 21,
  WORD_CLOUD: 22,
  TABLE: 23,
}
