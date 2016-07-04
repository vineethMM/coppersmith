//
// Copyright 2016 Commonwealth Bank of Australia
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//        http://www.apache.org/licenses/LICENSE-2.0
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

package commbank.coppersmith.scalding

import com.twitter.scalding.typed.{TypedPipe, TypedPipeFactory}
import com.twitter.scalding.TupleSetter.singleSetter
import com.twitter.scalding.ExecutionCounters

import cascading.tuple.Fields
import cascading.pipe.Each
import cascading.operation.state.Counter

object CoppersmithStats {
  val group = "Coppersmith"
  val log = org.slf4j.LoggerFactory.getLogger(getClass())

  implicit def fromTypedPipe[T](typedPipe: TypedPipe[T]) = new CoppersmithStats(typedPipe)

  /** Log (at INFO level) all coppersmith counters found in the passed [[com.twitter.scalding.ExecutionCounters]]. */
  def log(counters: ExecutionCounters): Unit =
    counters.keys.filter(_.group == group).foreach { key =>
      log.info(f"${counters(key)}%10d  ${key.counter}")
    }
}

class CoppersmithStats[T](typedPipe: TypedPipe[T]) extends {
  def withCounter(name: String) = TypedPipeFactory({ (fd, mode) =>
    // The logic to drop down to cascading duplicates the (unfortunately private) method TypedPipe.onRawSingle
    val oldPipe = typedPipe.toPipe(new Fields(java.lang.Integer.valueOf(0)))(fd, mode, singleSetter)
    val newPipe = new Each(oldPipe, new Counter(CoppersmithStats.group, name))
    TypedPipe.fromSingleField[T](newPipe)(fd, mode)
  })
}