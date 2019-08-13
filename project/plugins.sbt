addSbtPlugin("org.scalameta"    % "sbt-scalafmt"        % "2.0.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.15")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"        % "0.14.6")
resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
