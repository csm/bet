(def netty-version "4.1.50.Final")

(defproject bet "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.2.603"]
                 [fullcontact/full.async "1.1.0"]
                 [io.netty/netty-buffer ~netty-version]
                 [io.netty/netty-common ~netty-version]
                 [io.netty/netty-codec-http ~netty-version]
                 [io.netty/netty-codec-http2 ~netty-version]
                 [io.netty/netty-resolver ~netty-version]
                 [io.netty/netty-resolver-dns ~netty-version]
                 [io.netty/netty-transport ~netty-version]
                 [io.netty/netty-transport-native-epoll ~netty-version]
                 [io.aleph/dirigiste "0.1.6-alpha2"]
                 [potemkin "0.4.3"]
                 [byte-streams "0.2.5-alpha2"]
                 [com.cognitect/anomalies "0.1.12"]
                 [io.pedestal/pedestal.log "0.5.8"]
                 [com.google.guava/guava "29.0-jre"]]
  :repl-options {:init-ns bet.core})
