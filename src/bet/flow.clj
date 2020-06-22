(ns bet.flow
  "Based off of namespace aleph.flow from aleph."
  (:require [cognitect.anomalies :as anomalies]
            [clojure.core.async :as async])
  (:import [io.aleph.dirigiste IPool IPool$Controller IPool$Generator IPool$Generator Stats Stats$Metric IPool$AcquireCallback Executors Executor$Controller Executor Pool]
           (java.util.concurrent TimeUnit SynchronousQueue LinkedBlockingQueue ArrayBlockingQueue ThreadFactory)
           (java.util EnumSet)))

(defn- stats->map
  "Converts a Dirigiste `Stats` object into a map of values onto quantiles."
  ([s]
   (stats->map s [0.5 0.9 0.95 0.99 0.999]))
  ([^Stats s quantiles]
   (let [stats (.getMetrics s)
         q #(zipmap quantiles (mapv % quantiles))]
     (merge
       {:num-workers (.getNumWorkers s)}
       (when (contains? stats Stats$Metric/QUEUE_LENGTH)
         {:queue-length (q #(.getQueueLength s %))})
       (when (contains? stats Stats$Metric/QUEUE_LATENCY)
         {:queue-latency (q #(double (/ (.getQueueLatency s %) 1e6)))})
       (when (contains? stats Stats$Metric/TASK_LATENCY)
         {:task-latency (q #(double (/ (.getTaskLatency s %) 1e6)))})
       (when (contains? stats Stats$Metric/TASK_ARRIVAL_RATE)
         {:task-arrival-rate (q #(.getTaskArrivalRate s %))})
       (when (contains? stats Stats$Metric/TASK_COMPLETION_RATE)
         {:task-completion-rate (q #(.getTaskCompletionRate s %))})
       (when (contains? stats Stats$Metric/TASK_REJECTION_RATE)
         {:task-rejection-rate (q #(.getTaskRejectionRate s %))})
       (when (contains? stats Stats$Metric/UTILIZATION)
         {:utilization (q #(.getUtilization s %))})))))

(defn instrumented-pool
  "Returns a [Dirigiste](https://github.com/ztellman/dirigiste) object pool, which can be interacted
   with via `acquire`, `release`, and `dispose`.

   |:---|:----
   | `generate` | a single-arg funcion which takes a key, and returns an object which should be non-equal to any other generated object |
   | `destroy` | an optional two-arg function which takes a key and object, and releases any associated resources |
   | `stats-callback` | a function which will be invoked every `control-period` with a map of keys onto associated statistics |
   | `max-queue-size` | the maximum number of pending acquires per key that are allowed before `acquire` will start to throw a `java.util.concurrent.RejectedExecutionException`.
   | `sample-period` | the interval, in milliseconds, between sampling the state of the pool for resizing and gathering statistics, defaults to `10`.
   | `control-period` | the interval, in milliseconds, between use of the controller to adjust the size of the pool, defaults to `10000`.
   | `controller` | a Dirigiste controller that is used to gide the pool's size."
  [{:keys
        [generate
         destroy
         stats-callback
         max-queue-size
         sample-period
         control-period
         controller]
    :or {sample-period 10
         control-period 10000
         max-queue-size 65536}}]
  (let [^IPool$Controller c controller]
    (assert controller "must specify :controller")
    (assert generate   "must specify :generate")
    (Pool.
      (reify IPool$Generator
        (generate [_ k]
          (generate k))
        (destroy [_ k v]
          (when destroy
            (destroy k v))))

      (reify IPool$Controller
        (shouldIncrement [_ key objects-per-key total-objects]
          (.shouldIncrement c key objects-per-key total-objects))
        (adjustment [_ key->stats]
          (when stats-callback
            (stats-callback
              (zipmap
                (map str (keys key->stats))
                (map stats->map (vals key->stats)))))
          (.adjustment c key->stats)))

      max-queue-size
      sample-period
      control-period
      TimeUnit/MILLISECONDS)))

(defn acquire
  "Acquires an object from the pool for key `k`, returning a promise channel containing the object.
  May return an anomaly if an error occurs."
  [^IPool p k]
  (let [ch (async/promise-chan)]
    (try
      (.acquire p k
                (reify IPool$AcquireCallback
                  (handleObject [_ obj]
                    (when-not (async/put! ch obj)
                      (.release p k obj)))))
      (catch Throwable e
        (async/put! ch #:cognitect.anomalies{:category ::anomalies/fault
                                             :exception e})))
    ch))

(defn release
  "Releases an object for key `k` back to the pool."
  [^IPool p k obj]
  (.release p k obj))

(defn dispose
  "Disposes of a pooled object which is no longer valid."
  [^IPool p k obj]
  (.dispose p k obj))

(def ^:private factory-count (atom 0))

(def ^:private ^ThreadLocal executor-thread-local (ThreadLocal.))

(defn- ^ThreadFactory thread-factory0
  ([name-generator executor-promise]
   (thread-factory0 name-generator executor-promise nil))
  ([name-generator executor-promise stack-size]
   (reify ThreadFactory
     (newThread [_ runnable]
       (let [name (name-generator)
             f #(do
                  (.set executor-thread-local @executor-promise)
                  (.run ^Runnable runnable))]
         (doto
           (if stack-size
             (Thread. nil f name stack-size)
             (Thread. nil f name))
           (.setDaemon true)))))))

(defn instrumented-executor
  "Returns a `java.util.concurrent.ExecutorService`, using [Dirigiste](https://github.com/ztellman/dirigiste).

   |:---|:----
   | `thread-factory` | an optional `java.util.concurrent.ThreadFactory` that creates the executor's threads. |
   | `queue-length` | the maximum number of pending tasks before `.execute()` begins throwing `java.util.concurrent.RejectedExecutionException`, defaults to `0`.
   | `stats-callback` | a function that will be invoked every `control-period` with the relevant statistics for the executor.
   | `sample-period` | the interval, in milliseconds, between sampling the state of the executor for resizing and gathering statistics, defaults to `25`.
   | `control-period` | the interval, in milliseconds, between use of the controller to adjust the size of the executor, defaults to `10000`.
   | `controller` | the Dirigiste controller that is used to guide the pool's size.
   | `metrics` | an `EnumSet` of the metrics that should be gathered for the controller, defaults to all.
   | `initial-thread-count` | the number of threads that the pool should begin with.
   | `onto?` | if true, all streams and deferred generated in the scope of this executor will also be 'on' this executor."
  [{:keys
        [thread-factory
         queue-length
         stats-callback
         sample-period
         control-period
         controller
         metrics
         initial-thread-count
         onto?]
    :or {initial-thread-count 1
         sample-period 25
         control-period 10000
         metrics (EnumSet/allOf Stats$Metric)
         onto? true}}]
  (let [executor-promise (promise)
        thread-count (atom 0)
        factory (swap! factory-count inc)
        thread-factory (if thread-factory
                         thread-factory
                         (thread-factory0
                           #(str "bet-pool-" factory "-" (swap! thread-count inc))
                           (if onto?
                             executor-promise
                             (deliver (promise) nil))))
        ^Executor$Controller c controller
        metrics (if (identical? :none metrics)
                  (EnumSet/noneOf Stats$Metric)
                  metrics)]
    (assert controller "must specify :controller")
    @(deliver executor-promise
              (Executor.
                thread-factory
                (if (and queue-length (pos? queue-length))
                  (if (<= queue-length 1024)
                    (ArrayBlockingQueue. queue-length false)
                    (LinkedBlockingQueue. (int queue-length)))
                  (SynchronousQueue. false))
                (if stats-callback
                  (reify Executor$Controller
                    (shouldIncrement [_ n]
                      (.shouldIncrement c n))
                    (adjustment [_ s]
                      (stats-callback (stats->map s))
                      (.adjustment c s)))
                  c)
                initial-thread-count
                metrics
                sample-period
                control-period
                TimeUnit/MILLISECONDS))))

(defn utilization-executor
  "Returns an executor which sizes the thread pool according to target utilization, within
   `[0,1]`, up to `max-threads`.  The `queue-length` for this executor is always `0`, and by
   default has an unbounded number of threads."
  ([utilization]
   (utilization-executor utilization Integer/MAX_VALUE nil))
  ([utilization max-threads]
   (utilization-executor utilization max-threads nil))
  ([utilization max-threads options]
   (instrumented-executor
     (assoc options
       :queue-length 0
       :max-threads max-threads
       :controller (Executors/utilizationController utilization max-threads)))))