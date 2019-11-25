package com.joveox.ycsb.scylla

import java.util.Properties

import com.datastax.oss.driver.api.core.CqlSession
import com.joveox.ycsb.common.JoveoYCSBWorkload

class ScyllaWorkload extends JoveoYCSBWorkload {

  private var session : CqlSession = _
  private var utils : ScyllaUtils = _

  override def init(p: Properties): Unit = {
    super.init(p)
    session = ScyllaDBSession.build( ScyllaConf( p ) )
    utils = ScyllaUtils.init( loadManager, session)
    utils.setup()
  }

  override def cleanup(): Unit = {
    super.cleanup()
    utils.close()
  }

}
