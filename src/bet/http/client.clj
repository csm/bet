(ns bet.http.client
  (:require [bet.flow :as flow]
            [bet.impl.http.client :as client]
            [bet.impl.http.client-middleware :as middleware]
            [bet.impl.netty :as netty]
            [clojure.core.async :as async]
            [full.async :as fasync]
            [io.pedestal.log :as log])
  (:import (io.aleph.dirigiste Pools)
           (java.net URI InetSocketAddress)
           (java.util.concurrent TimeoutException)))

(defn- create-connection
  "Returns a channel that yields a function which, given an HTTP request, returns
   a channel representing the HTTP response.  If the server disconnects, all responses
   will be errors, and a new connection must be created."
  [^URI uri options middleware on-closed]
  (let [scheme (.getScheme uri)
        ssl? (= "https" scheme)]
       (fasync/go-try
         (-> (client/http-connection
               (InetSocketAddress/createUnresolved
                 (.getHost uri)
                 (int
                   (or
                     (when (pos? (.getPort uri)) (.getPort uri))
                     (if ssl? 443 80))))
               ssl?
               (if on-closed
                 (assoc options :on-closed on-closed)
                 options))
             (fasync/<?)
             (middleware)))))

(def ^:private connection-stats-callbacks (atom #{}))

(defn register-connection-stats-callback
  "Registers a callback which will be called with connection-pool stats."
  [c]
  (swap! connection-stats-callbacks conj c))

(defn unregister-connection-stats-callback
  "Unregisters a previous connection-pool stats callback."
  [c]
  (swap! connection-stats-callbacks disj c))

(def default-response-executor
  (flow/utilization-executor 0.9 256 {:onto? false}))

(defn connection-pool
  [{:keys [connections-per-host
           total-connections
           target-utilization
           connection-options
           dns-options
           stats-callback
           control-period
           middleware
           max-queue-size]
    :or {connections-per-host 8
         total-connections 1024
         target-utilization 0.9
         control-period 60000
         middleware identity ; middleware/wrap-request
         max-queue-size 65536}}]
  (when (and (false? (:keep-alive? connection-options))
             (pos? (:idle-timeout connection-options 0)))
    (throw
      (IllegalArgumentException.
         ":idle-timeout option is not allowed when :keep-alive? is explicitly disabled")))

  (let [log-activity (:log-activity connection-options)
        dns-options' (if-not (and (some? dns-options)
                                  (not (contains? dns-options :epoll?)))
                       dns-options
                       (let [epoll? (:epoll? connection-options false)]
                         (assoc dns-options :epoll? epoll?)))
        conn-options' (cond-> connection-options
                              (some? dns-options')
                              (assoc :name-resolver (netty/dns-resolver-group dns-options'))

                              (some? log-activity)
                              (assoc :log-activity (netty/activity-logger "aleph-client" log-activity)))
        p (promise)
        pool (flow/instrumented-pool
               {:generate       (fn [host]
                                  (let [c (promise)
                                        conn (create-connection
                                               host
                                               conn-options'
                                               middleware
                                               #(flow/dispose @p host [@c]))]
                                    (deliver c conn)
                                    [conn]))
                :destroy        (fn [_ c]
                                  (async/go
                                    (when-let [c (async/poll! (first c))]
                                      (client/close-connection c))))
                :control-period control-period
                :max-queue-size max-queue-size
                :controller     (Pools/utilizationController
                                  target-utilization
                                  connections-per-host
                                  total-connections)
                :stats-callback stats-callback})]
    @(deliver p pool)))

(def default-connection-pool
  (connection-pool
    {:stats-callback
     (fn [s]
       (doseq [c @connection-stats-callbacks]
         (c s)))}))

(defmacro maybe-timeout<?
  ([port timeout] `(maybe-timeout<? ~port ~timeout :bet.impl.http.client/timeout))
  ([port timeout timeout-val]
   `(if (some? ~timeout)
      (let [port# ~port
            [val# comp#] (fasync/alts? [port# (async/timeout ~timeout)])]
        (if (identical? port# comp#)
          val#
          ~timeout-val))
      (fasync/<? ~port))))

(defn request
  "Make a request based on the given request map. Returns a promise channel
  that will yield a ring response map."
  [{:keys [pool
           middleware
           pool-timeout
           response-executor
           connection-timeout
           request-timeout
           read-timeout
           follow-redirects?]
    :or {pool default-connection-pool
         response-executor default-response-executor
         middleware identity
         connection-timeout 6e4} ;; 60 seconds
    :as req}]
  (fasync/go-try
    (let [k (client/req->domain req)
          start (System/currentTimeMillis)
          conn (maybe-timeout<? (flow/acquire pool k) pool-timeout ::timeout)]
      (log/debug :task ::request :conn conn)
      (if (= ::timeout conn)
        (throw (TimeoutException. "timed out acquiring connection from pool"))
        (try
          (let [c (maybe-timeout<? (first conn) connection-timeout ::timeout)]
            (log/debug :task ::request :c c)
            (if (= ::timeout c)
              (throw (TimeoutException. "timed out connecting to remote host"))
              (let [req-chan (c req)
                    end (System/currentTimeMillis)
                    response (maybe-timeout<? req-chan request-timeout ::timeout)]
                (if (= ::timeout response)
                  (do
                    (flow/dispose pool k conn)
                    (throw (TimeoutException. "request timed out")))
                  (let [complete (async/go (maybe-timeout<? (:bet/complete response) read-timeout))]
                    (async/take! complete (fn [_] (flow/dispose pool k conn)))
                    (-> response
                        (dissoc :bet/complete)
                        (assoc :connection-time (- end start))
                        (middleware/handle-cookies req)
                        (middleware/handle-redirects request req)))))))
          (catch Exception e
            (flow/dispose pool k conn)
            e))))))

  ;(executor/with-executor response-executor
  ;                        ((middleware
  ;                           (fn [req]
  ;                             (let [k (client/req->domain req)
  ;                                   start (System/currentTimeMillis)]
  ;
  ;                               ;; acquire a connection
  ;                               (-> (flow/acquire pool k)
  ;                                   (maybe-timeout! pool-timeout)
  ;
  ;                                   ;; pool timeout triggered
  ;                                   (d/catch' TimeoutException
  ;                                     (fn [^Throwable e]
  ;                                       (d/error-deferred (PoolTimeoutException. e))))
  ;
  ;                                   (d/chain'
  ;                                     (fn [conn])
  ;
  ;                                       ;; get the wrapper for the connection, which may or may not be realized yet
  ;                                       (-> (first conn))
  ;
  ;                                           (maybe-timeout! connection-timeout)
  ;
  ;                                           ;; connection timeout triggered, dispose of the connetion
  ;                                           (d/catch' TimeoutException)
  ;                                                     (fn [^Throwable e])
  ;                                                       (flow/dispose pool k conn)
  ;                                                       (d/error-deferred (ConnectionTimeoutException. e))
  ;
  ;                                           ;; connection failed, bail out
  ;                                           (d/catch')
  ;                                             (fn [e])
  ;                                               (flow/dispose pool k conn)
  ;                                               (d/error-deferred e)
  ;
  ;                                           ;; actually make the request now
  ;                                           (d/chain')
  ;                                             (fn [conn'])
  ;                                               (when-not (nil? conn'))
  ;                                                 (let [end (System/currentTimeMillis)])
  ;                                                   (-> (conn' req))
  ;                                                       (maybe-timeout! request-timeout)
  ;
  ;                                           ;; request timeout triggered, dispose of the connection
  ;                                           (d/catch' TimeoutException)
  ;                                                     (fn [^Throwable e])
  ;                                                       (flow/dispose pool k conn)
  ;                                                       (d/error-deferred (RequestTimeoutException. e))
  ;
  ;                                           ;; request failed, dispose of the connection
  ;                                           (d/catch')
  ;                                             (fn [e])
  ;                                               (flow/dispose pool k conn)
  ;                                               (d/error-deferred e)
  ;
  ;                                                                           ;; clean up the response
  ;                                                                           (d/chain')
  ;                                                                             (fn [rsp])
  ;
  ;                                                                               ;; only release the connection back once the response is complete
  ;                                                                               (-> (:aleph/complete rsp))
  ;                                                                                   (maybe-timeout! read-timeout)
  ;
  ;                                                                                   (d/catch' TimeoutException)
  ;                                                                                             (fn [^Throwable e])
  ;                                                                                               (flow/dispose pool k conn)
  ;                                                                                               (d/error-deferred (ReadTimeoutException. e))
  ;
  ;                                                                                   (d/chain')
  ;                                                                                     (fn [early?])
  ;                                                                                       (if (or early?))
  ;                                                                                               (not (:aleph/keep-alive? rsp))
  ;                                                                                               (<= 400 (:status rsp))
  ;                                                                                         (flow/dispose pool k conn)
  ;                                                                                         (flow/release pool k conn)
  ;                                                                               (-> rsp)
  ;                                                                                   (dissoc :aleph/complete)
  ;                                                                                   (assoc :connection-time (- end start))
  ;
  ;                                                                 (fn [rsp])
  ;                                                                   (->> rsp)
  ;                                                                        (middleware/handle-cookies req)
  ;                                                                        (middleware/handle-redirects request req)))))
  ;                                             req)))