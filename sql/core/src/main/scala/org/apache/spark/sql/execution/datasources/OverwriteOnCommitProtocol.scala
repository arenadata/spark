/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.JobContext

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol


class OverwriteOnCommitProtocol(
                                 jobId: String,
                                 path: String,
                                 dynamicPartitionOverwrite: Boolean)
  extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) with Logging {

  private val scheduledDeletes = new ConcurrentLinkedQueue[(Path, Boolean)]()

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    logInfo(s"Deferring deleteWithJob(path=$path, recursive=$recursive) until commitJob")
    scheduledDeletes.add(path -> recursive)
    true
  }

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    val algo = conf.getInt("mapreduce.fileoutputcommitter.algorithm.version", 1)
    if (algo == 2) {
      throw new UnsupportedOperationException(
        s"${getClass.getSimpleName} requires FileOutputCommitter algorithm.version=1. " +
          s"Set spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=1 for this job."
      )
    }
    super.setupJob(jobContext)
  }

  override def commitJob(
                          jobContext: JobContext,
                          taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val conf = jobContext.getConfiguration
    val fs = new Path(path).getFileSystem(conf)
    val outputRoot = new Path(path)
    def deleteChildrenExceptTemporary(dir: Path): Unit = {
      if (!fs.exists(dir)) return
      fs.listStatus(dir).foreach { st =>
        val child = st.getPath
        if (child.getName != "_temporary") {
          try {
            if (!fs.delete(child, true)) {
              logWarning(s"Delete returned false for $child; continuing")
            }
          } catch {
            case t: Throwable =>
              logWarning(s"Ignoring error deleting $child before commit", t)
          }
        }
      }
    }

    Stream
      .continually(scheduledDeletes.poll())
      .takeWhile(_ != null)
      .foreach { case (p, recursive) =>
        if (recursive && p == outputRoot) {
          logInfo(s"Cleaning output root children (keeping _temporary): $p")
          deleteChildrenExceptTemporary(outputRoot)
        } else {
          try {
            if (!fs.delete(p, recursive)) {
              logWarning(s"Delete returned false for $p; continuing")
            }
          } catch {
            case t: Throwable =>
              logWarning(s"Ignoring error deleting $p before commit", t)
          }
        }
      }

    super.commitJob(jobContext, taskCommits)
  }

  override def abortJob(jobContext: JobContext): Unit = {
    scheduledDeletes.clear()
    super.abortJob(jobContext)
  }
}
