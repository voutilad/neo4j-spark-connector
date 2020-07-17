package org.neo4j.spark.util

import org.neo4j.driver.Session

object Neo4jUtil {

  def closeSafety(autoCloseable: AutoCloseable): Unit = {
    if (autoCloseable == null) {
      return Unit
    }

    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case _ => autoCloseable.close()
      }
    } catch {
      case _ => throw new Exception("This exception should be logged")// @todo Log
    }
  }

}
