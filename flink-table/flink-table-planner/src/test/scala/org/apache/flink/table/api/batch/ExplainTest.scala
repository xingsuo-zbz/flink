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
package org.apache.flink.table.api.batch

import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.table.types.logical.{BigIntType, IntType, VarCharType}
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class ExplainTest(extended: Boolean) extends TableTestBase {

  private val extraDetails = if (extended) {
    Array(ExplainDetail.CHANGELOG_MODE, ExplainDetail.ESTIMATED_COST)
  } else {
    Array.empty[ExplainDetail]
  }

  private val util = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
  util.addDataStream[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addDataStream[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)

  val STRING = VarCharType.STRING_TYPE
  val LONG = new BigIntType()
  val INT = new IntType()

  @BeforeEach
  def before(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(4))
  }

  @TestTemplate
  def testExplainWithTableSourceScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithDataStreamScan(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithFilter(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 WHERE mod(a, 2) = 0", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithAgg(): Unit = {
    util.verifyExplain("SELECT COUNT(*) FROM MyTable1 GROUP BY a", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithJoin(): Unit = {
    // TODO support other join operators when them are supported
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    util.verifyExplain("SELECT a, b, c, e, f FROM MyTable1, MyTable2 WHERE a = d", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithUnion(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 UNION ALL SELECT * FROM MyTable2", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithSort(): Unit = {
    util.verifyExplain("SELECT * FROM MyTable1 ORDER BY a LIMIT 5", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithSingleSink(): Unit = {
    val table = util.tableEnv.sqlQuery("SELECT * FROM MyTable1 WHERE a > 10")
    val sink = util.createCollectTableSink(Array("a", "b", "c"), Array(INT, LONG, STRING))
    util.verifyExplainInsert(table, sink, "sink", extraDetails: _*)
  }

  @TestTemplate
  def testExplainWithMultiSinks(): Unit = {
    val stmtSet = util.tableEnv.createStatementSet()
    val table = util.tableEnv.sqlQuery("SELECT a, COUNT(*) AS cnt FROM MyTable1 GROUP BY a")
    util.tableEnv.createTemporaryView("TempTable", table)

    val table1 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt > 10")
    val sink1 = util.createCollectTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink1", sink1)
    stmtSet.addInsert("sink1", table1)

    val table2 = util.tableEnv.sqlQuery("SELECT * FROM TempTable WHERE cnt < 10")
    val sink2 = util.createCollectTableSink(Array("a", "cnt"), Array(INT, LONG))
    util.tableEnv.asInstanceOf[TableEnvironmentInternal].registerTableSinkInternal("sink2", sink2)
    stmtSet.addInsert("sink2", table2)

    util.verifyExplain(stmtSet, extraDetails: _*)
  }

  @TestTemplate
  def testExplainMultipleInput(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |select * from
        |   (select a, sum(b) from MyTable1 group by a) v1,
        |   (select d, sum(e) from MyTable2 group by d) v2
        |   where a = d
        |""".stripMargin
    util.verifyExplain(sql, extraDetails: _*)
  }

}

object ExplainTest {
  @Parameters(name = "extended={0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
