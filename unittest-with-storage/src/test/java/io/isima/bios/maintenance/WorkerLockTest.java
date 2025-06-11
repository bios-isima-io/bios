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
package io.isima.bios.maintenance;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.it.tools.Bios2TestModules;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkerLockTest {

  private static WorkerLock workerLock;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Bios2TestModules.startModulesWithoutMaintenance(WorkerLockTest.class);
    workerLock = new WorkerLock(2, 1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Bios2TestModules.shutdown();
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    workerLock.setAutoRefresh(true);
  }

  @Test
  public void testBasic() throws ApplicationException, InterruptedException {
    // turn off auto refresh to test expiry
    workerLock.setAutoRefresh(false);

    final String targetOne = "target.one";
    final String targetTwo = "target.two";

    // acquire two locks
    WorkerLock.LockEntity lockTwo = null;
    try (WorkerLock.LockEntity lockOne = workerLock.lock(targetOne)) {
      assertNotNull(lockOne);
      lockTwo = workerLock.lock(targetTwo);
      assertNotNull(lockTwo);

      // verify both targets are locked
      try (WorkerLock.LockEntity lockOne2 = workerLock.lock(targetOne)) {
        assertNull(lockOne2);
      }
      final WorkerLock.LockEntity lockTwo2 = workerLock.lock(targetTwo);
      assertNull(lockTwo2);

      // the lock one keeps refreshing, the lock two does not
      for (int i = 0; i < 3; ++i) {
        Thread.sleep(1000);
        lockOne.refresh();
      }

      // the target one should still be locked
      final WorkerLock.LockEntity lockOne3 = workerLock.lock(targetOne);
      assertNull(lockOne3);
      // the target two should be expired
      final WorkerLock.LockEntity lockTwo3 = workerLock.lock(targetTwo);
      assertNotNull(lockTwo3);
    } finally {
      if (lockTwo != null) {
        lockTwo.release();
      }
    }

    // two locks have been released. Check lock availability once again
    try (WorkerLock.LockEntity lockOne = workerLock.lock(targetOne)) {
      assertNotNull(lockOne);
    }
    try (WorkerLock.LockEntity lockTwo4 = workerLock.lock(targetTwo)) {
      assertNotNull(lockTwo4);
    }
  }

  @Test
  public void testUnlockOnException() throws ApplicationException, InterruptedException {
    // turn off auto refresh to test expiry
    workerLock.setAutoRefresh(false);

    final String targetThree = "target.three";

    // acquire lock and throw an exception
    try {
      try (WorkerLock.LockEntity lock = workerLock.lock(targetThree)) {
        assertNotNull(lock);
        throw new Exception("test exception");
      }
    } catch (Exception e) {
      // do nothing
    }

    // two locks have been released. Check lock availability once again
    try (WorkerLock.LockEntity lock = workerLock.lock(targetThree)) {
      assertNotNull(lock);
    }
  }

  @Test
  public void testAutoRefresh() throws ApplicationException, InterruptedException {
    final String targetThree = "target.three";

    try (WorkerLock.LockEntity lockA = workerLock.lock(targetThree)) {
      assertNotNull(lockA);
      Thread.sleep(3000);

      assertTrue(lockA.isValid());

      try (WorkerLock.LockEntity lockB = workerLock.lock(targetThree)) {
        assertNull(lockB);
      }

      assertTrue(lockA.isValid());
    }

    try (WorkerLock.LockEntity lock = workerLock.lock(targetThree)) {
      assertNotNull(lock);
    }

    Thread.sleep(3000);

    try (WorkerLock.LockEntity lock = workerLock.lock(targetThree)) {
      assertNotNull(lock);
    }
  }
}
