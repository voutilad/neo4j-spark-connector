package org.neo4j.spark.util

import org.neo4j.driver.{Driver, Session}

object Neo4jUtil {

  // @todo write test
  def closeSafety(autoCloseable: AutoCloseable): Unit = {
    if( autoCloseable == null) {
      Unit
    }

    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case _ => autoCloseable.close()
      }
    } catch {
      case _ => Unit // @todo Log
    }
  }

}
