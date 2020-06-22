(ns bet.impl.pool
  (:require [full.async :as fasync])
  (:import (java.util.concurrent Semaphore ConcurrentHashMap ConcurrentLinkedQueue TimeUnit)
           (com.google.common.cache CacheBuilder CacheLoader LoadingCache Weigher RemovalListener)
           (java.util Queue)
           (java.util.concurrent.locks ReentrantLock Lock)))

(defprotocol Resource
  (total-permits [this]
    "Return the total number of permits available in this resource.")
  (permits [this]
    "Return the number of permits this resource has available.")
  (take-permit! [this]
    "Take a permit from this resource. Returns the resource the caller
    may use on success, returns nil if no permit could be acquired.")
  (release-permit! [this resource]
    "Release a permit from this resource. Will return true if the argument
    given belonged to this resource, and was properly released."))

(defprotocol AsyncPool
  (acquire! [this k]
    "Fetch a value from the pool for key k. Returns a promise channel
    that will yield the value, or possibly an exception if no resource
    could be created.")
  (release! [this k v]
    "Release a value back to the pool."))

(defrecord SimpleResource [permits resource semaphore]
  Resource
  (total-permits [_] permits)
  (permits [_] (.availablePermits semaphore))
  (take-permit! [_] (when (.tryAcquire semaphore) resource))
  (release-permit! [_ r] (when (identical? resource r)
                           (.release semaphore)
                           true)))

(defn simple-resource
  "Create a simple resource containing the given number of permits."
  [resource permits]
  (->SimpleResource permits resource (Semaphore. permits)))

(defmacro try-with-acquire
  "Attempt to acquire semaphore, executing if-acquired if the permit was
  acquired and releasing the semaphore after execution, or executes
  if-not-acquired if the permit was not acquired."
  [semaphore if-acquired if-not-acquired]
  `(if (.tryAcquire ~semaphore)
     (try ~if-acquired
          (finally (.release ~semaphore)))
     ~if-not-acquired))

(defn resource-pool
  "Create a resource pool.

  builder is a 1-arg function that receives the requested key, and should
  return a channel that yields a new resource, or returns a Throwable on
  error. The returned value may satisfy the Resource protocol, if it will
  manage its own resource count.

  destroyer is a 2-arg function that receives the key, and the returned
  resource from the builder, and should release that resource.

  The optional argument max-per-key governs how many total resources will
  be created for a given key."
  [builder destroyer & {:keys [max-per-key expire-time expire-time-units] :or {expire-time-units TimeUnit/MILLISECONDS}}]
  (let [qref (promise)
        queue-cache ^LoadingCache (cond-> (CacheBuilder/newBuilder)
                                          :then (.concurrencyLevel (or (Long/getLong "clojure.core.async.pool-size") 8))
                                          ; need to rethink pool-size here. This could wind up trying to release resources still in use
                                          ;(some? pool-size) (.maximumWeight pool-size)
                                          ;(some? pool-size) (.weigher (reify Weigher
                                          ;                              (weigh [_ _ v]
                                          ;                                (.size ^Queue (:queue v))))
                                          (some? expire-time) (.expireAfterWrite expire-time expire-time-units)
                                          :then (.removalListener
                                                  (reify RemovalListener
                                                    (onRemoval [_ notification]
                                                      (when (.wasEvicted notification)
                                                        (let [new-q (volatile! [])]
                                                          (doseq [resource (:queue (.getValue notification))]
                                                            (if (= (total-permits resource) (permits resource))
                                                              (destroyer (.getKey notification)
                                                                         (if (::owned (meta resource))
                                                                           (:resource resource)
                                                                           resource))
                                                              (vswap! new-q conj resource)))
                                                          ; if resources are still in use, add them back to the cache
                                                          (when (not-empty @new-q)
                                                            (.put @qref (.getKey notification) (assoc (.getValue notification) :queue new-q))))))))
                                          :then (.build (proxy [CacheLoader] []
                                                          (load [k] {:key       k
                                                                     :semaphore (Semaphore. 1)
                                                                     :queue     (volatile! [])}))))]
    (deliver qref queue-cache)
    (reify AsyncPool
      (acquire! [_ k]
        (fasync/go-try
          (let [entry (.get queue-cache k)]
            (loop []
              (try-with-acquire
                (:semaphore entry)
                (let [queue (deref (:queue entry))
                      ret (if-let [resource (->> (some #(pos? (permits %)) queue))]
                            (take-permit! resource)
                            (let [resource (as-> (builder k) r
                                                 (if (satisfies? Resource r)
                                                   r
                                                   (with-meta (simple-resource r 1) {::owned true})))]
                              (vswap! queue conj resource)
                              (take-permit! resource)))]
                  (.put queue-cache k entry)
                  ret)
                (recur))))))

      (release! [_ k v]
        (fasync/go-try
          (let [entry (.get queue-cache k)]
            (loop []
              (try-with-acquire
                (:semaphore entry)
                (do
                  (loop [queue (:queue entry)]
                    (when-let [resource (first queue)]
                      (when-not (release-permit! resource v)
                        (recur (rest queue)))))
                  (.put queue-cache k entry))
                (recur)))))))))