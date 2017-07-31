package io.iohk.iodb

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit}

/** executes multiple tasks in background thread, waits until they finish, fails if any tasks throws exception */
class ForkExecutor(duration: Long) {

  val endTime = System.currentTimeMillis() + duration

  val exception = new AtomicReference[Throwable]()

  val executor = Executors.newCachedThreadPool()

  def keepRunning: Boolean = System.currentTimeMillis() < endTime


  def execute(task: => Unit): Unit = {
    executor.submit(TestUtils.runnable {
      try {
        task
      } catch {
        case e: Throwable => exception.compareAndSet(null, e)
      }
    })
  }

  def finish(): Unit = {
    executor.shutdown()

    def rethrow(): Unit = {
      if (exception.get() != null) {
        throw new RuntimeException(exception.get())
      }
    }

    while (!executor.awaitTermination(10, TimeUnit.MILLISECONDS)) {
      rethrow()
    }
    rethrow()
  }


}
