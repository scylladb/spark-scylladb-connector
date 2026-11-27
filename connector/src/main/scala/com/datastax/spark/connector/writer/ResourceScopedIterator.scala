/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.spark.connector.writer

/** Maps `data` with a function derived from a resource that the iterator owns.
  *
  * The resource is acquired on the first call to `next`, so an iterator that is never consumed acquires none.
  * Right after acquiring it, the iterator registers its release with `registerRelease`, for example as a Spark task
  * completion listener, which also runs when the task fails or is cancelled. The resource is released exactly once:
  * when `data` is exhausted, when deriving the function or mapping an element fails, or when the registered release
  * runs, whichever comes first, also when the consumer stops early. After that, the iterator has no next element.
  *
  * @param acquire acquires the resource
  * @param release releases the resource
  * @param registerRelease registers a function that releases the resource; it may run it right away, for example
  *                        when the task has completed already
  * @param init derives the function that maps the elements from the resource */
private[connector] final class ResourceScopedIterator[R, A, B](
    data: Iterator[A],
    acquire: () => R,
    release: R => Unit,
    registerRelease: (() => Unit) => Unit,
    init: R => A => B) extends Iterator[B] {

  private sealed trait State
  private case object NotStarted extends State
  private case class Acquired(resource: R) extends State
  private case class Started(resource: R, map: A => B) extends State
  private case object Closed extends State

  // Guarded by this
  private var state: State = NotStarted

  private def mapper: A => B = synchronized {
    state match {
      case Started(_, map) => map
      case Closed => throw new IllegalStateException("The iterator has released its resource")
      case Acquired(_) => throw new IllegalStateException("The iterator is being initialized")
      case NotStarted =>
        state = Acquired(acquire())
        closeOnFailure(registerRelease(() => close()))
        state match {
          case Acquired(resource) =>
            val map = closeOnFailure(init(resource))
            state = Started(resource, map)
            map
          case _ =>
            throw new IllegalStateException("The iterator released its resource when it registered the release")
        }
    }
  }

  /** Releases the resource, if the iterator holds one and has not released it yet. Ends the iteration. */
  def close(): Unit = {
    val resource = synchronized {
      val held = state match {
        case Acquired(resource) => Some(resource)
        case Started(resource, _) => Some(resource)
        case NotStarted | Closed => None
      }
      state = Closed
      held
    }
    resource.foreach(release)
  }

  private def closeOnFailure[T](code: => T): T =
    try code
    catch {
      case e: Throwable =>
        close()
        throw e
    }

  override def hasNext: Boolean = synchronized(state) != Closed && closeOnFailure {
    val more = data.hasNext
    if (!more) close()
    more
  }

  override def next(): B = {
    val map = mapper
    closeOnFailure(map(data.next()))
  }
}
