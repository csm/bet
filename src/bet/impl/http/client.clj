(ns bet.impl.http.client
  (:require [bet.impl.http.core :as http]
            [bet.impl.http.multipart :as multipart]
            [bet.impl.netty :as netty]
            [bet.impl.util :as util]
            [clojure.core.async :as async]
            [full.async :as fasync]
            [io.pedestal.log :as log]
            [byte-streams :as bs])
  (:import (java.net InetSocketAddress IDN URI URL)
           (io.netty.channel ChannelPipeline ChannelHandler Channel ChannelInitializer ChannelOutboundHandler ChannelPromise SimpleChannelInboundHandler)
           (io.netty.handler.codec.http HttpClientCodec HttpObjectAggregator HttpRequest HttpClientUpgradeHandler HttpClientUpgradeHandler$UpgradeEvent HttpResponse HttpResponseStatus HttpContent LastHttpContent FullHttpResponse HttpUtil HttpHeaderNames)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.ssl SslContextBuilder ApplicationProtocolConfig ApplicationProtocolConfig$Protocol ApplicationProtocolConfig$SelectorFailureBehavior ApplicationProtocolConfig$SelectedListenerFailureBehavior ApplicationProtocolNames OpenSsl SslProvider SslContext ApplicationProtocolNegotiationHandler)
           (io.netty.handler.ssl.util InsecureTrustManagerFactory)
           (java.security.cert X509Certificate)
           (io.netty.handler.codec.http2 DefaultHttp2Connection HttpToHttp2ConnectionHandlerBuilder DelegatingDecompressorFrameListener InboundHttp2ToHttpAdapter InboundHttp2ToHttpAdapterBuilder Http2FrameLogger Http2ClientUpgradeCodec)
           (io.netty.handler.logging LogLevel LoggingHandler)
           (java.io IOException)
           (java.util.concurrent.atomic AtomicInteger)
           (io.netty.handler.codec TooLongFrameException)
           (io.netty.buffer ByteBuf)))

(defn raw-client-handler
  [response-chan buffer-capacity]
  (let [stream (atom nil)
        complete (atom nil)

        handle-response
        (fn [response complete body]
          (async/put! response-chan
                      (http/netty-response->ring-response
                        response
                        complete
                        body)))]
    (netty/channel-inbound-handler
      :exception-caught
      ([_ ctx ex]
       (when-not (instance? IOException ex)
         (log/warn ex "error in HTTP client")))

      :channel-inactive
      ([_ ctx]
       (when-let [s @stream]
         (async/close! s))
       (async/close! response-chan)
       (.fireChannelInactive ctx))

      :channel-read
      ([_ ctx msg]
       (cond
         (instance? HttpResponse msg)
         (let [rsp msg]
           (let [s (util/->channel-wrapper (async/chan))
                 c (async/promise-chan)]
             (reset! stream s)
             (reset! complete c)
             (util/on-close s #(async/put! c true))
             (handle-response rsp c s)))

         (instance? HttpContent msg)
         (let [content (.content ^HttpContent msg)]
           (when-not (async/offer! @stream content)
             (-> (.channel ctx) .config (.setAutoRead false))
             (async/put! @stream content
                         (fn [res]
                           (-> (.channel ctx) .config (.setAutoRead true)))))
           (when (instance? LastHttpContent msg)
             (async/put! @complete false)
             (async/close! @stream)))

         :else
         (.fireChannelRead ctx msg))))))

(defn client-handler
  [response-chan ^long buffer-capacity]
  (let [response (atom nil)
        buffer (atom [])
        buffer-size (AtomicInteger. 0)
        stream (atom nil)
        complete (atom nil)
        handle-response (fn [rsp complete body]
                          (async/put! response-chan
                                      (http/netty-response->ring-response
                                        rsp
                                        complete
                                        body)))]

    (netty/channel-inbound-handler

      :exception-caught
      ([_ ctx ex]
       (cond
         ; could happens when io.netty.handler.codec.http.HttpObjectAggregator
         ; is part of the pipeline
         (instance? TooLongFrameException ex)
         (async/put! response-chan ex)

         (not (instance? IOException ex))
         (log/warn ex "error in HTTP client")))

      :channel-inactive
      ([_ ctx]
       (when-let [s @stream]
         (async/close! s))
       (doseq [b @buffer]
         (netty/release b))
       (async/close! response-chan)
       (.fireChannelInactive ctx))

      :channel-read
      ([_ ctx msg]

       (cond
         ; happens when io.netty.handler.codec.http.HttpObjectAggregator is part of the pipeline
         (instance? FullHttpResponse msg)
         (let [^FullHttpResponse rsp msg
               content (.content rsp)
               c (async/promise-chan)
               s (util/->channel-wrapper (async/chan 1))]
           (util/on-close s #(async/put! c true))
           (async/put! s (netty/buf->array content))
           (netty/release content)
           (handle-response rsp c s)
           (async/close! s))

         (instance? HttpResponse msg)
         (let [rsp msg]
           (if (HttpUtil/isTransferEncodingChunked rsp)
             (let [s (util/->channel-wrapper (async/chan))
                   c (async/promise-chan)]
               (reset! stream s)
               (reset! complete c)
               (util/on-close s #(async/put! c true))
               (handle-response rsp c s))
             (reset! response rsp)))

         (instance? HttpContent msg)
         (let [content (.content ^HttpContent msg)]
           (if (instance? LastHttpContent msg)
             (do

               (if-let [s @stream]
                 (do
                   (async/put! s (netty/buf->array content))
                   (netty/release content)
                   (async/put! @complete false)
                   (async/close! s))
                 (let [bufs (conj @buffer content)
                       bytes (netty/bufs->array bufs)]
                   (doseq [b bufs]
                     (netty/release b))
                   (handle-response @response (doto (async/chan) (async/put! false)) bytes)))

               (.set buffer-size 0)
               (reset! stream nil)
               (reset! buffer [])
               (reset! response nil))

             (if-let [s @stream]

              ;; already have a stream going
               (let [val (netty/buf->array content)]
                 (netty/release content)
                 (when-not (async/offer! s val)
                   (-> (.channel ctx) .config (.setAutoRead false))
                   (async/put! s val
                               (fn [res]
                                 (-> (.channel ctx) .config (.setAutoRead true))))))

               (let [len (.readableBytes ^ByteBuf content)]
                 (when-not (zero? len)
                   (swap! buffer conj content))
                 (let [size (.addAndGet buffer-size len)]

                  ;; buffer size exceeded, flush it as a stream
                   (when (< buffer-capacity size)
                     (let [bufs @buffer
                           c (async/promise-chan)
                           s (doto (util/->channel-wrapper (async/chan))
                               (async/put! (netty/bufs->array bufs)))]
                       (doseq [b bufs]
                         (netty/release b))
                       (reset! buffer [])
                       (reset! stream s)
                       (reset! complete c)
                       (util/on-close s #(async/put! c true))
                       (handle-response @response c s))))))))

         :else
         (.fireChannelRead ctx msg))))))

(defn- http2-initialzer
  "Initialize HTTP/2 support when using TLS with ALPN. Will optionally attempt upgrade to http/2 over plaintext when upgrade-plaintext? is true."
  [{:keys [host port ^SslContext ssl-context response-buffer-size max-initial-line-length max-header-size max-chunk-size upgrade-plaintext?]
    :or   {response-buffer-size    65536
           max-initial-line-length 65536
           max-header-size         65536
           max-chunk-size          65536}}]
  (fn [pipeline]
    (if ssl-context
      (.addLast pipeline (proxy [ApplicationProtocolNegotiationHandler] [ApplicationProtocolNames/HTTP_1_1]
                           (configurePipeline [ctx protocol]
                             (log/debug :task ::http2-initializer
                                        :phase :tls-configure-pipeline
                                        :ctx ctx
                                        :protocol protocol)
                             (cond (= protocol ApplicationProtocolNames/HTTP_2)
                                   (let [connection (DefaultHttp2Connection. false)
                                         logger (Http2FrameLogger. LogLevel/DEBUG "bet.impl.http.client/http2-initializer")
                                         connection-handler (-> (HttpToHttp2ConnectionHandlerBuilder.)
                                                                (.frameListener (DelegatingDecompressorFrameListener.
                                                                                  connection
                                                                                  (-> (InboundHttp2ToHttpAdapterBuilder. connection)
                                                                                      (.maxContentLength Integer/MAX_VALUE)
                                                                                      (.propagateSettings true)
                                                                                      (.build))))
                                                                (.frameLogger logger)
                                                                (.connection connection)
                                                                (.build))
                                         pipeline (.pipeline ctx)
                                         settings-handler (proxy [SimpleChannelInboundHandler] []
                                                            (channelRead0 [ctx msg]
                                                              (log/info :task ::http2-initializer
                                                                        :phase :read-http2-settings
                                                                        :settings msg)
                                                              (.remove pipeline ^ChannelHandler this)))]
                                     (.addLast pipeline "http2-connection" connection-handler)
                                     (.addLast pipeline "http2-settings-handler" settings-handler))

                                   (= protocol ApplicationProtocolNames/HTTP_1_1)
                                   (.addLast (.pipeline ctx) (HttpClientCodec.
                                                               max-initial-line-length
                                                               max-header-size
                                                               max-chunk-size
                                                               false
                                                               false))

                                   :else
                                   (do (.close ctx)
                                       (throw (IllegalStateException. (str "unknown protocol: " protocol))))))))
      (if upgrade-plaintext?
        (let [src-codec (HttpClientCodec.
                          max-initial-line-length
                          max-header-size
                          max-chunk-size
                          false
                          false)
              connection (DefaultHttp2Connection. false)
              logger (Http2FrameLogger. LogLevel/INFO "bet.impl.http.client/http2-initializer")
              connection-handler (-> (HttpToHttp2ConnectionHandlerBuilder.)
                                     (.frameListener (DelegatingDecompressorFrameListener.
                                                       connection
                                                       (-> (InboundHttp2ToHttpAdapterBuilder. connection)
                                                           (.maxContentLength Integer/MAX_VALUE)
                                                           (.propagateSettings true)
                                                           (.build))))
                                     (.frameLogger logger)
                                     (.connection connection)
                                     (.build))
              upgrade-codec (Http2ClientUpgradeCodec. connection-handler)
              upgrade-handler (HttpClientUpgradeHandler. src-codec upgrade-codec max-chunk-size)]
          (.addLast pipeline upgrade-handler))
        (let [http-handler (HttpClientCodec.
                             max-initial-line-length
                             max-header-size
                             max-chunk-size
                             false
                             false)]
          (.addLast pipeline http-handler))))))

(defn pipeline-builder
  [response-stream
   {:keys
    [pipeline-transform
     response-buffer-size
     max-initial-line-length
     max-header-size
     max-chunk-size
     raw-stream?
     proxy-options
     ssl?
     idle-timeout
     log-activity]
    :or
    {pipeline-transform identity
     response-buffer-size 65536
     max-initial-line-length 65536
     max-header-size 65536
     max-chunk-size 65536
     idle-timeout 0}
    :as options}]
  (fn [^ChannelPipeline pipeline]
    (let [handler (if raw-stream?
                    (raw-client-handler response-stream response-buffer-size)
                    (client-handler response-stream response-buffer-size))
          logger (cond
                   (instance? LoggingHandler log-activity)
                   log-activity

                   (some? log-activity)
                   (netty/activity-logger "aleph-client" log-activity)

                   :else
                   nil)
          http2-init (http2-initialzer options)]
      (doto pipeline
        (http2-init)
        (.addLast "streamer" ^ChannelHandler (ChunkedWriteHandler.))
        (.addLast "handler" ^ChannelHandler handler)
        (http/attach-idle-handlers idle-timeout))
      (comment "TODO implement this"
               (when (some? proxy-options)
                 (let [proxy (proxy-handler (assoc proxy-options :ssl? ssl?))]
                   (.addFirst pipeline "proxy" ^ChannelHandler proxy)
                   ;; well, we need to wait before the proxy responded with
                   ;; HTTP/1.1 200 Connection established
                   ;; before sending any requests
                   (when (instance? ProxyHandler proxy)
                     (.addAfter pipeline
                                "proxy"
                                "pending-proxy-connection"
                                ^ChannelHandler
                                (pending-proxy-connection-handler response-stream))))))
      (when (some? logger)
        (.addFirst pipeline "activity-logger" ^ChannelHandler logger))
      (pipeline-transform pipeline))))

(defn insecure-ssl-client-context
  []
  (let [provider (if (and (OpenSsl/isAvailable) (OpenSsl/isAlpnSupported))
                   SslProvider/OPENSSL
                   SslProvider/JDK)]
    (-> (SslContextBuilder/forClient)
        (.trustManager InsecureTrustManagerFactory/INSTANCE)
        (.sslProvider provider)
        (.applicationProtocolConfig (ApplicationProtocolConfig. ApplicationProtocolConfig$Protocol/ALPN
                                                                ApplicationProtocolConfig$SelectorFailureBehavior/FATAL_ALERT
                                                                ApplicationProtocolConfig$SelectedListenerFailureBehavior/FATAL_ALERT
                                                                (into-array String [ApplicationProtocolNames/HTTP_2
                                                                                    ApplicationProtocolNames/HTTP_1_1]))))))

(defn ssl-client-context
  ([] (ssl-client-context {}))
  ([{:keys [application-protocols]
     :or {application-protocols [ApplicationProtocolNames/HTTP_2 ApplicationProtocolNames/HTTP_1_1]}
     :as arg-map}]
   (netty/ssl-client-context (assoc arg-map :application-protocol-config (ApplicationProtocolConfig.
                                                                           ApplicationProtocolConfig$Protocol/ALPN
                                                                           ApplicationProtocolConfig$SelectorFailureBehavior/FATAL_ALERT
                                                                           ApplicationProtocolConfig$SelectedListenerFailureBehavior/FATAL_ALERT
                                                                           (into-array String application-protocols))
                                            :ssl-provider (if (and (OpenSsl/isAvailable) (OpenSsl/isAlpnSupported))
                                                            SslProvider/OPENSSL
                                                            SslProvider/JDK)))))

(defn non-tunnel-proxy? [{:keys [tunnel? user http-headers ssl?]
                          :as proxy-options}]
  (and (some? proxy-options)
       (not tunnel?)
       (not ssl?)
       (nil? user)
       (nil? http-headers)))

(defn http-connection
  [^InetSocketAddress remote-address
   ssl?
   {:keys [local-address
           raw-stream?
           bootstrap-transform
           name-resolver
           keep-alive?
           insecure?
           ssl-context
           response-buffer-size
           on-closed
           response-executor
           epoll?
           proxy-options]
    :or {bootstrap-transform identity
         keep-alive? true
         response-buffer-size 65536
         epoll? false
         name-resolver :default}
    :as options}]
  (let [responses (util/->channel-wrapper (async/chan 1024))
        requests (async/chan 1024)
        host (.getHostName remote-address)
        port (.getPort remote-address)
        explicit-port? (and (pos? port) (not= port (if ssl? 443 80)))
        ssl-context (when ssl?
                      (or ssl-context
                          (if insecure?
                            (insecure-ssl-client-context)
                            (ssl-client-context))))
        c (netty/create-client
            (pipeline-builder responses (assoc options :ssl-context ssl-context
                                                       :host host
                                                       :port port))
            (when ssl?
              (or ssl-context
                  (if insecure?
                    (insecure-ssl-client-context)
                    (ssl-client-context))))
            bootstrap-transform
            remote-address
            local-address
            epoll?
            name-resolver)
        request-consumer (fn request-consumer [ch]
                           (async/go-loop []
                             (let [req (async/<! requests)]
                               (try


                                 ;; this will usually happen because of a malformed request
                                 (catch Throwable e
                                   (async/>! responses e))))))]
    (fasync/go-try
      (let [^Channel ch (fasync/<? c)]
        (util/on-close responses
                       (fn []
                         (when on-closed (on-closed))
                         (async/close! requests)))
        (let [t0 (System/nanoTime)]
          (fn [req]
            (if (contains? req ::close)
              (netty/wrap-future (netty/close ch))
              (fasync/go-try
                (let [raw-stream? (get req :raw-stream? raw-stream?)
                      rsp (let [proxy-options' (when (some? proxy-options)
                                                 (assoc proxy-options :ssl? ssl?))
                                ^HttpRequest req' (http/ring-request->netty-request
                                                    (if (non-tunnel-proxy? proxy-options')
                                                      (assoc req :uri (:request-url req))
                                                      req))]
                            (when-not (.get (.headers req') "Host")
                              (.set (.headers req') HttpHeaderNames/HOST (str host (when explicit-port? (str ":" port)))))
                            (when-not (.get (.headers req') "Connection")
                              (HttpUtil/setKeepAlive req' keep-alive?))
                            (when (and (non-tunnel-proxy? proxy-options')
                                       (get proxy-options :keep-alive? true)
                                       (not (.get (.headers req') "Proxy-Connection")))
                              (.set (.headers req') "Proxy-Connection" "Keep-Alive"))

                            (let [body (:body req)
                                  parts (:multipart req)
                                  multipart? (some? parts)
                                  [req' body] (cond
                                                ;; RFC #7231 4.3.8. TRACE
                                                ;; A client MUST NOT send a message body...
                                                (= :trace (:request-method req))
                                                (do
                                                  (when (or (some? body) multipart?)
                                                    (log/warn :error "TRACE request body was omitted"))
                                                  [req' nil])

                                                (not multipart?)
                                                [req' body]

                                                :else
                                                (multipart/encode-request req' parts))]

                              (when-let [save-message (get req :bet/save-request-message)]
                                ;; debug purpose only
                                ;; note, that req' is effectively mutable, so
                                ;; it will "capture" all changes made during "send-message"
                                ;; execution
                                (reset! save-message req'))

                              (when-let [save-body (get req :bet/save-request-body)]
                                ;; might be different in case we use :multipart
                                (reset! save-body body))

                              (fasync/<?
                                (http/send-message ch true ssl? req' body))))]
                  (log/debug :task ::http-connection
                             :phase :received-response
                             :response rsp)
                  (cond (nil? rsp)
                        (throw (ex-info (format "connection was closed after %.3f seconds" (/ (- (System/nanoTime) t0) 1e9))
                                        {:request req}))

                        raw-stream?
                        rsp

                        :else
                        (let [body (:body rsp)]
                          ;(when-not keep-alive?
                          ;  (if (s/stream? body)
                          ;    (s/on-closed body #(netty/close ch))
                          ;    (netty/close ch)]
                          (assoc rsp
                            :body
                            (bs/to-input-stream body
                                                {:buffer-size response-buffer-size})))))))))))))

(defn close-connection [f]
  (f
    {:method :get
     :url "http://example.com"
     ::close true}))

(let [no-url (fn [req]
               (URI.
                 (name (or (:scheme req) :http))
                 nil
                 (some-> (or (:host req) (:server-name req)) IDN/toASCII)
                 (or (:port req) (:server-port req) -1)
                 nil
                 nil
                 nil))]

  (defn ^java.net.URI req->domain [req]
    (if-let [url (:url req)]
      (let [^URL uri (URL. url)]
        (URI.
          (.getProtocol uri)
          nil
          (IDN/toASCII (.getHost uri))
          (.getPort uri)
          nil
          nil
          nil))
      (no-url req))))