(ns bet.impl.http.core
  (:require [clojure.string :as str]
            [full.async :as fasync]
            [potemkin :as p]
            [clojure.set :as set]
            [io.pedestal.log :as log]
            [bet.impl.netty :as netty]
            [clojure.java.io :as io]
            [byte-streams :as bs]
            [clojure.core.async :as async])
  (:import (io.netty.handler.codec.http HttpMethod HttpVersion HttpResponseStatus DefaultHttpResponse DefaultHttpRequest HttpUtil HttpResponse HttpHeaders HttpMessage DefaultLastHttpContent HttpContent LastHttpContent HttpChunkedInput)
           (java.nio ByteBuffer)
           (io.netty.buffer ByteBuf)
           (java.io File RandomAccessFile)
           (io.netty.handler.stream ChunkedFile ChunkedInput ChunkedWriteHandler)
           (java.nio.file Path)
           (io.netty.channel DefaultFileRegion Channel ChannelFuture ChannelFutureListener ChannelPipeline ChannelHandler)
           (java.util.concurrent ConcurrentHashMap TimeUnit)
           (io.netty.handler.timeout IdleStateHandler IdleState IdleStateEvent)))

(def non-standard-keys
  (let [ks ["Content-MD5"
            "ETag"
            "WWW-Authenticate"
            "X-XSS-Protection"
            "X-WebKit-CSP"
            "X-UA-Compatible"
            "X-ATT-DeviceId"
            "DNT"
            "P3P"
            "TE"]]
    (zipmap
      (map str/lower-case ks)
      (map #(HttpHeaders/newEntity %) ks))))

(def ^ConcurrentHashMap cached-header-keys (ConcurrentHashMap.))

(defn normalize-header-key
  "Normalizes a header key to `Ab-Cd` format."
  [s]
  (if-let [s' (.get cached-header-keys s)]
    s'
    (let [s' (str/lower-case (name s))
          s' (or
               (non-standard-keys s')
               (->> (str/split s' #"-")
                    (map str/capitalize)
                    (str/join "-")
                    HttpHeaders/newEntity))]

      ;; in practice this should never happen, so we
      ;; can be stupid about cache expiration
      (when (< 10000 (.size cached-header-keys))
        (.clear cached-header-keys))

      (.put cached-header-keys s s')
      s')))

(defn map->headers! [^HttpHeaders h m]
  (doseq [e m]
    (let [k (normalize-header-key (key e))
          v (val e)]

      (cond
        (nil? v)
        (throw (IllegalArgumentException. (str "nil value for header key '" k "'")))

        (sequential? v)
        (.add h ^CharSequence k ^Iterable v)

        :else
        (.add h ^CharSequence k ^Object v)))))

(defn ring-response->netty-response [m]
  (let [status (get m :status 200)
        headers (get m :headers)
        rsp (DefaultHttpResponse.
              HttpVersion/HTTP_1_1
              (HttpResponseStatus/valueOf status)
              false)]
    (when headers
      (map->headers! (.headers rsp) headers))
    rsp))

(defn ring-request->netty-request [m]
  (let [headers (get m :headers)
        req (DefaultHttpRequest.
              HttpVersion/HTTP_1_1
              (-> m (get :request-method) name str/upper-case HttpMethod/valueOf)
              (str (get m :uri)
                   (when-let [q (get m :query-string)]
                     (str "?" q))))]
    (when headers
      (map->headers! (.headers req) headers))
    req))

(p/def-map-type HeaderMap
                [^HttpHeaders headers
                 added
                 removed
                 mta]
  (meta [_] mta)
  (with-meta [_ m]
    (HeaderMap.
      headers
      added
      removed
      m))
  (keys [_]
    (set/difference
      (set/union
        (set (map str/lower-case (.names headers)))
        (set (keys added)))
      (set removed)))
  (assoc [_ k v]
    (HeaderMap.
      headers
      (assoc added k v)
      (disj removed k)
      mta))
  (dissoc [_ k]
    (HeaderMap.
      headers
      (dissoc added k)
      (conj (or removed #{}) k)
      mta))
  (get [_ k default-value]
    (if (contains? removed k)
      default-value
      (if-let [e (find added k)]
        (val e)
        (let [k' (str/lower-case (name k))
              vs (.getAll headers k')]
          (if (.isEmpty vs)
            default-value
            (if (== 1 (.size vs))
              (.get vs 0)
              (str/join "," vs))))))))

(defn headers->map [^HttpHeaders h]
  (HeaderMap. h nil nil nil))

(p/def-derived-map NettyResponse [^HttpResponse rsp complete body]
                   :status (-> rsp .status .code)
                   :bet/keep-alive? (HttpUtil/isKeepAlive rsp)
                   :headers (-> rsp .headers headers->map)
                   :bet/complete complete
                   :body body)

(def default-chunk-size 8192)

(deftype HttpFile [^File fd ^long offset ^long length ^long chunk-size])

(defmethod print-method HttpFile [^HttpFile file ^java.io.Writer w]
  (.write w (format "HttpFile[fd:%s offset:%s length:%s]"
                    (.-fd file)
                    (.-offset file)
                    (.-length file))))

(defn http-file
  ([path]
   (http-file path nil nil default-chunk-size))
  ([path offset length]
   (http-file path offset length default-chunk-size))
  ([path offset length chunk-size]
   (let [^File
         fd (cond
              (string? path)
              (io/file path)

              (instance? File path)
              path

              (instance? Path path)
              (.toFile ^Path path)

              :else
              (throw
                (IllegalArgumentException.
                   (str "cannot conver " (class path) " to file, "
                          "expected either string, java.io.File "
                          "or java.nio.file.Path"))))
         region? (or (some? offset) (some? length))]
     (when-not (.exists fd)
       (throw
         (IllegalArgumentException.
            (str fd " file does not exist"))))

     (when (.isDirectory fd)
       (throw
         (IllegalArgumentException.
            (str fd " is a directory, file expected"))))

     (when (and region? (not (<= 0 offset)))
       (throw
         (IllegalArgumentException.
            "offset of the region should be 0 or greater")))

     (when (and region? (not (pos? length)))
       (throw
         (IllegalArgumentException.
            "length of the region should be greater than 0")))

     (let [len (.length fd)
           [p c] (if region?
                   [offset length]
                   [0 len])
           chunk-size (or chunk-size default-chunk-size)]
       (when (and region? (< len (+ offset length)))
         (throw
           (IllegalArgumentException.
              "the region exceeds the size of the file")))

       (HttpFile. fd p c chunk-size)))))

(defn netty-response->ring-response [rsp complete body]
  (->NettyResponse rsp complete body))

(defn has-content-length? [^HttpMessage msg]
  (HttpUtil/isContentLengthSet msg))

(defn try-set-content-length! [^HttpMessage msg ^long length]
  (when-not (has-content-length? msg)
    (HttpUtil/setContentLength msg length)))

(defn chunked-writer-enabled? [^Channel ch]
  (some? (-> ch netty/channel .pipeline (.get ChunkedWriteHandler))))

(defn to-readable-channel
  [body]
  (cond (satisfies? clojure.core.async.impl.protocols/ReadPort body)
        body

        (sequential? body)
        (doto (async/chan) (async/onto-chan!! body))))

(let [ary-class (class (byte-array 0))]
  (defn coerce-element [x]
    (if (or
          (instance? String x)
          (instance? ary-class x)
          (instance? ByteBuffer x)
          (instance? ByteBuf x))
      x
      (str x))))

(def empty-last-content LastHttpContent/EMPTY_LAST_CONTENT)

(defn send-streaming-body
  [ch ^HttpMessage msg body]
  (fasync/go-try
    (HttpUtil/setTransferEncodingChunked msg (boolean (not (has-content-length? msg))))
    (fasync/<? (netty/write ch msg))
    (let [body-chan (to-readable-channel body)]
      (loop []
        (when-let [element (some-> (async/<! body-chan) (coerce-element))]
          (fasync/<? (netty/write ch element))))
      (fasync/<? (netty/write-and-flush ch empty-last-content)))))

(defn send-contiguous-body [ch ^HttpMessage msg body]
  (let [omitted? (identical? :bet/omitted body)
        body (if (or (nil? body) omitted?)
               empty-last-content
               (DefaultLastHttpContent. (netty/to-byte-buf ch body)))
        length (-> ^HttpContent body .content .readableBytes)]

    (when-not omitted?
      (if (instance? HttpResponse msg)
        (let [code (-> ^HttpResponse msg .status .code)]
          (when-not (or (<= 100 code 199) (= 204 code))
            (try-set-content-length! msg length)))
        (try-set-content-length! msg length)))

    (fasync/go-try
      (fasync/<? (netty/write ch msg))
      (fasync/<? (netty/write-and-flush ch body)))))

(defn send-chunked-file
  [ch ^HttpMessage msg ^HttpFile file]
  (let [raf (RandomAccessFile. ^File (.-fd file) "r")
        cf (ChunkedFile. raf
                         (.-offset file)
                         (.-length file)
                         (.-chunk-size file))]
    (try-set-content-length! msg (.-length file))
    (fasync/go-try
      (fasync/<? (netty/write ch msg))
      (fasync/<? (netty/write-and-flush ch (HttpChunkedInput. cf))))))

(defn send-chunked-body [ch ^HttpMessage msg ^ChunkedInput body]
  (fasync/go-try
    (fasync/<? (netty/write ch msg))
    (fasync/<? (netty/write-and-flush ch body))))

(defn send-file-region [ch ^HttpMessage msg ^HttpFile file]
  (let [raf (RandomAccessFile. ^File (.-fd file) "r")
        fc (.getChannel raf)
        fr (DefaultFileRegion. fc (.-offset file) (.-length file))]
    (try-set-content-length! msg (.-length file))
    (fasync/go-try
      (fasync/<? (netty/write ch msg))
      (fasync/<? (netty/write ch fr))
      (fasync/<? (netty/write-and-flush ch empty-last-content)))))

(defn send-file-body [ch ssl? ^HttpMessage msg ^HttpFile file]
  (fasync/go-try
    (cond
      ssl?
      (fasync/<? (send-streaming-body ch msg
                                      (let [chan (async/chan)]
                                        (-> file
                                            (bs/to-byte-buffers {:chunk-size (.-chunk-size file)})
                                            (->> (async/onto-chan!! chan)))
                                        chan)))

      (chunked-writer-enabled? ch)
      (fasync/<? (send-chunked-file ch msg file))

      :else
      (fasync/<? (send-file-region ch msg file)))))

(let [ary-class (class (byte-array 0))
      handle-cleanup
      (fn [ch f]
        (async/go
          (try
            (let [^ChannelFuture f (fasync/<? f)]
              (if f
                (.addListener f ChannelFutureListener/CLOSE)
                (netty/close ch)))
            (catch Exception _))))]
  (defn send-message
    [ch keep-alive? ssl? ^HttpMessage msg body]

    (fasync/go-try
      (let [f (fasync/<?
                (cond
                  (or
                    (nil? body)
                    (identical? :bet/omitted body)
                    (instance? String body)
                    (instance? ary-class body)
                    (instance? ByteBuffer body)
                    (instance? ByteBuf body))
                  (send-contiguous-body ch msg body)

                  (instance? ChunkedInput body)
                  (send-chunked-body ch msg body)

                  (instance? File body)
                  (send-file-body ch ssl? msg (http-file body))

                  (instance? Path body)
                  (send-file-body ch ssl? msg (http-file body))

                  (instance? HttpFile body)
                  (send-file-body ch ssl? msg body)

                  :else
                  (let [class-name (.getName (class body))]
                    (try
                      (send-streaming-body ch msg body)
                      (catch Throwable e
                        (log/error :task ::send-message
                                   :phase :error
                                   :body-classname class-name
                                   :cause e))))))]

        (when-not keep-alive?
          (handle-cleanup ch f))

        f))))

(defn close-on-idle-handler []
  (netty/channel-handler
    :user-event-triggered
    ([_ ctx evt]
     (if (and (instance? IdleStateEvent evt)
              (= IdleState/ALL_IDLE (.state ^IdleStateEvent evt)))
       (netty/close ctx)
       (.fireUserEventTriggered ctx evt)))))

(defn attach-idle-handlers [^ChannelPipeline pipeline idle-timeout]
  (if (pos? idle-timeout)
    (doto pipeline
      (.addLast "idle" ^ChannelHandler (IdleStateHandler. 0 0 idle-timeout TimeUnit/MILLISECONDS))
      (.addLast "idle-close" ^ChannelHandler (close-on-idle-handler)))
    pipeline))