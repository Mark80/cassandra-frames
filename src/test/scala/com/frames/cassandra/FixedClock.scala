package com.frames.cassandra

import java.time.{Clock, Instant, ZoneId}

trait FixedClock {

  implicit val fixedClock: Clock = Clock.fixed(Instant.parse("2019-01-01T12:00:00.00Z"), ZoneId.of("Europe/Paris"))

}
