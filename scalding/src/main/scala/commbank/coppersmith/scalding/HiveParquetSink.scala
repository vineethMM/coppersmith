//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package commbank.coppersmith.scalding

import com.twitter.scalding.{Execution, TupleSetter, TypedPipe}

import org.apache.hadoop.fs.Path

import au.com.cba.omnia.maestro.api._

import commbank.coppersmith._, Feature._
import Partitions.PathComponents
import FeatureSink.AttemptedWriteToCommitted
import CoppersmithStats.fromTypedPipe

/**
  * Parquet FeatureSink implementation - create using HiveParquetSink.apply in companion object.
  */
case class HiveParquetSink[T <: ThriftStruct : Manifest : FeatureValueEnc, P : TupleSetter] private(
  table:         HiveTable[T, (P, T)],
  partitionPath: Path,
  mergeFiles: Option[Int]
) extends FeatureSink {
  def write(features: TypedPipe[(FeatureValue[Value], FeatureTime)],
            metadataSet: MetadataSet[Any]): FeatureSink.WriteResult = {
    FeatureSink.isCommitted(partitionPath).flatMap(committed =>
      if (committed) {
        Execution.from(Left(AttemptedWriteToCommitted(partitionPath)))
      } else {
        val eavts = {
          mergeFiles match {
            // Force an additional MapReduce step, which reads the feature values from the previous step,
            // and writes them to the given number of parquet part files.
            case Some(n) => features.forceToDisk.groupBy(_._1.entity).withReducers(n).values
            case None    => features
          }
        }.map(implicitly[FeatureValueEnc[T]].encode).withCounter("write.parquet")
        for {
          counters <- table.writeExecution(eavts)
          _        <- Execution.from(CoppersmithStats.logCounters(counters))
        } yield Right(Set(partitionPath))
      }
    )
  }
}

object HiveParquetSink {
  type DatabaseName = String
  type TableName    = String

  /**
    * Configure a parquet feature sink, accessible as a Hive table.
    *
    * @param dbName     Name of the (existing) hive database to use.
    * @param tableName  Name of the hive table (will be created if it does not exist).
    * @param tablePath  HDFS path where the table data will be written.
    * @param partition  The partition of the table to create (must not exist).
    * @param mergeFiles If `Some(N)` for an integer N, then ensure at most N part files
    *                   are produced, by merging the part files output by feature generation.
    *                   If `None`, don't merge.
    */
  def apply[
    T <: ThriftStruct : Manifest : FeatureValueEnc,
    P : Manifest : PathComponents
  ](
    dbName:    DatabaseName,
    tableName: TableName,
    tablePath: Path,
    partition: FixedSinkPartition[T, P],
    mergeFiles: Option[Int] = None
  ): HiveParquetSink[T, P] = {
    // Note: This needs to be explicitly specified so that the TupleSetter.singleSetter
    // instance isn't used (causing a failure at runtime).
    implicit val partitionSetter = partition.tupleSetter
    val hiveTable = HiveTable[T, P](
      dbName,
      tableName,
      partition.underlying,
      tablePath.toString
    )

    val pathComponents = implicitly[PathComponents[P]].toComponents(partition.partitionValue)
    val partitionRelPath = new Path(partition.underlying.pattern.format(pathComponents: _*))

    HiveParquetSink[T, P](hiveTable, new Path(tablePath, partitionRelPath), mergeFiles)
  }
}
