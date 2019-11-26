package com.joveox.ycsb.scylla

import java.util.Properties

import com.datastax.oss.driver.api.core.CqlSession
import com.joveox.ycsb.common.{ConfigManager, JoveoYCSBWorkload}

class ScyllaWorkload extends JoveoYCSBWorkload {

  private var session : CqlSession = _
  private var utils : ScyllaUtils = _

  override def init(p: Properties): Unit = {
    super.init(p)
    session = ScyllaDBSession.build( false )
    ScyllaDBSession.setup( ConfigManager.get.schema, session )
    utils = ScyllaUtils.init( ConfigManager.get.schema, ConfigManager.get.useCaseManager, session)
  }

  override def cleanup(): Unit = {
    super.cleanup()
    utils.close()
  }

}
