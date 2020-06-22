(ns bet.impl.util
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl])
  (:import (java.io InputStream)
           (java.util LinkedList)
           (io.netty.channel ChannelFuture)
           (clojure.lang IObj)))

(defn stream->chan
  "Turn an input stream into a channel that yields byte arrays."
  ([in] (stream->chan in 4096))
  ([in buffer-size] (stream->chan in buffer-size nil))
  ([^InputStream in buffer-size buf-or-n]
   (let [ch (async/chan buf-or-n)]
     (async/thread
       (let [read-buffer (byte-array buffer-size)]
         (loop []
           (let [read (.read in read-buffer)]
             (cond (pos? read)
                   (let [out-buffer (byte-array read)]
                     (System/arraycopy read-buffer 0 out-buffer 0 read)
                     (when (async/>!! ch out-buffer)
                       (recur)))

                   (neg? read)
                   (async/close! ch)

                   :else (recur))))))
     ch)))

(defprotocol IOnClose
  (on-close [_ handler]
    "Add handler to the on-close listener list. When the channel closes,
    calls handler. If already closed, calls handler immediately."))

(defrecord ChannelWrapper [port on-close-handlers]
  impl/ReadPort
  (take! [_ handler] (impl/take! port handler))

  impl/WritePort
  (put! [_ val handler] (impl/put! port val handler))

  impl/Channel
  (close! [_]
    (when (not (impl/closed? port))
      (impl/close! port)
      (run! #(%) @on-close-handlers)))
  (closed? [_] (impl/closed? port))

  IOnClose
  (on-close [_ handler]
    (if (impl/closed? port)
      (handler)
      (swap! on-close-handlers conj handler))))

(defn ->channel-wrapper
  [port]
  (->ChannelWrapper port (atom [])))

; like sliding-buffer/dropping-buffer, but throws an exception
; when the buffer is full
(deftype ErrorOnFullBuffer [^LinkedList buf ^long n]
  impl/UnblockingBuffer
  impl/Buffer
  (full? [_] false)
  (remove! [_] (.removeLast buf))
  (add!* [_ item]
    (when (>= (.size buf) n)
      (throw (IllegalStateException. "buffer is full")))))