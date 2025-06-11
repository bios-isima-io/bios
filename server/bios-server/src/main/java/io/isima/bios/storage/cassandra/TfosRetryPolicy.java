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
package io.isima.bios.storage.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class TfosRetryPolicy implements RetryPolicy {
  // private static final Logger logger = LoggerFactory.getLogger(TfosRetryPolicy.class);

  // to prevent infinite retry
  private static final int MAX_RETRY = 3;

  private RetryDecision maxLikelyToWorkCl(int knownOk, ConsistencyLevel currentCl) {

    if (knownOk >= 3) {
      return RetryDecision.retry(ConsistencyLevel.THREE);
    }

    if (knownOk == 2) {
      return RetryDecision.retry(ConsistencyLevel.TWO);
    }

    // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
    // so even if we get 0 alive replicas, there might be
    // a node up in some other datacenter
    if (knownOk == 1 || currentCl == ConsistencyLevel.EACH_QUORUM) {
      return RetryDecision.retry(ConsistencyLevel.ONE);
    }

    return RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onReadTimeout(
      Statement statement,
      ConsistencyLevel cl,
      int requiredResponses,
      int receivedResponses,
      boolean dataRetrieved,
      int nbRetry) {
    if (nbRetry >= MAX_RETRY) {
      return RetryDecision.rethrow();
    }

    // CAS reads are not all that useful in terms of visibility of the writes since CAS write
    // supports the normal consistency levels on the committing phase. So the main use case for CAS
    // reads is probably for when you've timed out on a CAS write and want to make sure what
    // happened. Downgrading in that case would be always wrong so we just special case to rethrow.
    if (cl.isSerial()) {
      return RetryDecision.rethrow();
    }

    switch (cl) {
      case ALL:
      case LOCAL_QUORUM:
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      case QUORUM:
        return RetryDecision.retry(ConsistencyLevel.ONE);
      default:
        // fall down
    }

    // follow DowngradingConsistencyRetryPolicy
    if (receivedResponses < requiredResponses) {
      // Tries the biggest CL that is expected to work
      return maxLikelyToWorkCl(receivedResponses, cl);
    }

    return !dataRetrieved ? RetryDecision.retry(cl) : RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onWriteTimeout(
      Statement statement,
      ConsistencyLevel cl,
      WriteType writeType,
      int requiredAcks,
      int receivedAcks,
      int nbRetry) {
    if (nbRetry >= MAX_RETRY) {
      return RetryDecision.rethrow();
    }

    // Retrying an LWT on timeout (i.e. CAS conflict) from the retry policy does not seem to work
    // properly. It would cause InvalidQueryException as SERIAL is not supported.
    // We have CAS timeout handling mechanism in our app, so would let it handle the failure.
    // See also BIOS-4736.
    if (writeType == WriteType.CAS) {
      return RetryDecision.rethrow();
    }

    if (cl == ConsistencyLevel.LOCAL_QUORUM || cl == ConsistencyLevel.ALL) {
      return RetryDecision.retry(ConsistencyLevel.QUORUM);
    }

    switch (writeType) {
      case SIMPLE:
      case BATCH:
        // Since we provide atomicity there is no point in retrying
        return receivedAcks > 0 ? RetryDecision.ignore() : RetryDecision.rethrow();
      case UNLOGGED_BATCH:
        // Since only part of the batch could have been persisted,
        // retry with whatever consistency should allow to persist all
        return maxLikelyToWorkCl(receivedAcks, cl);
      case BATCH_LOG:
        return RetryDecision.retry(cl);
      default:
        return RetryDecision.rethrow();
    }
  }

  @Override
  public RetryDecision onUnavailable(
      Statement statement,
      ConsistencyLevel cl,
      int requiredReplica,
      int aliveReplica,
      int nbRetry) {
    // logger.warn("onUnavailable statement={} cl={} requiredReplica={} aliveReplic={} nbRetry={}",
    // statement, cl, requiredReplica, aliveReplica, nbRetry);
    if (nbRetry >= MAX_RETRY) {
      return RetryDecision.rethrow();
    }

    switch (cl) {
      case ALL:
      case LOCAL_QUORUM:
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      case QUORUM:
      case LOCAL_ONE:
        return RetryDecision.retry(ConsistencyLevel.ONE);
      case SERIAL:
      case LOCAL_SERIAL:
        return RetryDecision.rethrow();
      default:
        // follow DowngradingConsistencyRetryPolicy
        return maxLikelyToWorkCl(aliveReplica, cl);
    }
  }

  @Override
  public RetryDecision onRequestError(
      Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
    return RetryDecision.tryNextHost(cl);
  }

  @Override
  public void init(Cluster cluster) {}

  @Override
  public void close() {}
}
