(ns bet.impl.netty
  (:require [byte-streams :as bs]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as impl]
            [full.async :as fasync])
  (:import (io.netty.channel.epoll Epoll EpollDatagramChannel EpollSocketChannel EpollEventLoopGroup)
           (io.netty.resolver.dns DnsNameResolverBuilder DnsServerAddressStreamProvider DnsAddressResolverGroup SequentialDnsServerAddressStreamProvider SingletonDnsServerAddressStreamProvider)
           (io.netty.channel.socket.nio NioDatagramChannel NioSocketChannel)
           (io.netty.resolver ResolvedAddressTypes NoopAddressResolverGroup AddressResolverGroup)
           (java.net InetSocketAddress URI SocketAddress)
           (io.netty.handler.logging LoggingHandler LogLevel)
           (io.netty.channel ChannelPipeline ChannelFuture ChannelOption ChannelInitializer Channel ChannelInboundHandler ChannelHandler ChannelHandlerContext ChannelOutboundHandler VoidChannelPromise)
           (io.netty.handler.ssl SslContext SslContextBuilder ApplicationProtocolConfig ApplicationProtocolConfig$Protocol ApplicationProtocolConfig$SelectedListenerFailureBehavior ApplicationProtocolConfig$SelectorFailureBehavior)
           (java.util.concurrent ThreadFactory CancellationException)
           (io.netty.util.internal SystemPropertyUtil)
           (io.netty.bootstrap Bootstrap)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.util.concurrent Future GenericFutureListener)
           (io.netty.handler.ssl.util InsecureTrustManagerFactory)
           (java.security.cert X509Certificate)
           (java.io InputStream File)
           (java.security PrivateKey)
           (java.nio ByteBuffer)
           (io.netty.buffer ByteBuf ByteBufUtil Unpooled)
           (java.nio.charset Charset)))

(defn epoll-available? []
  (Epoll/isAvailable))

(defn coerce-log-level [level]
  (if (instance? LogLevel level)
    level
    (let [netty-level (case level
                        :trace LogLevel/TRACE
                        :debug LogLevel/DEBUG
                        :info LogLevel/INFO
                        :warn LogLevel/WARN
                        :error LogLevel/ERROR
                        nil)]
      (when (nil? netty-level)
        (throw (IllegalArgumentException.
                 (str "unknown log level given: " level))))
      netty-level)))

(defn activity-logger
  ([level]
   (LoggingHandler. ^LogLevel (coerce-log-level level)))
  ([^String name level]
   (LoggingHandler. name ^LogLevel (coerce-log-level level))))

(defn convert-address-types [address-types]
  (case address-types
    :ipv4-only ResolvedAddressTypes/IPV4_ONLY
    :ipv6-only ResolvedAddressTypes/IPV6_ONLY
    :ipv4-preferred ResolvedAddressTypes/IPV4_PREFERRED
    :ipv6-preferred ResolvedAddressTypes/IPV6_PREFERRED))

(def dns-default-port 53)

(defn dns-name-servers-provider [servers]
  (let [addresses (->> servers
                       (map (fn [server]
                              (cond
                                (instance? InetSocketAddress server)
                                server

                                (string? server)
                                (let [^URI uri (URI. (str "dns://" server))
                                      port (.getPort uri)
                                      port' (int (if (= -1 port) dns-default-port port))]
                                  (InetSocketAddress. (.getHost uri) port'))

                                :else
                                (throw
                                  (IllegalArgumentException.
                                    (format "Don't know how to create InetSocketAddress from '%s'"
                                               server)))))))]
    (if (= 1 (count addresses))
      (SingletonDnsServerAddressStreamProvider. (first addresses))
      (SequentialDnsServerAddressStreamProvider. ^Iterable addresses))))

(defn dns-resolver-group
  "Creates an instance of DnsAddressResolverGroup that might be set as a resolver to Bootstrap.

   DNS options are a map of:

   |:--- |:---
   | `max-payload-size` | sets capacity of the datagram packet buffer (in bytes), defaults to `4096`
   | `max-queries-per-resolve` | sets the maximum allowed number of DNS queries to send when resolving a host name, defaults to `16`
   | `address-types` | sets the list of the protocol families of the address resolved, should be one of `:ipv4-only`, `:ipv4-preferred`, `:ipv6-only`, `:ipv4-preferred`  (calculated automatically based on ipv4/ipv6 support when not set explicitly)
   | `query-timeout` | sets the timeout of each DNS query performed by this resolver (in milliseconds), defaults to `5000`
   | `min-ttl` | sets minimum TTL of the cached DNS resource records (in seconds), defaults to `0`
   | `max-ttl` | sets maximum TTL of the cached DNS resource records (in seconds), defaults to `Integer/MAX_VALUE` (the resolver will respect the TTL from the DNS)
   | `negative-ttl` | sets the TTL of the cache for the failed DNS queries (in seconds)
   | `trace-enabled?` | if set to `true`, the resolver generates the detailed trace information in an exception message, defaults to `false`
   | `opt-resources-enabled?` | if set to `true`, enables the automatic inclusion of a optional records that tries to give the remote DNS server a hint about how much data the resolver can read per response, defaults to `true`
   | `search-domains` | sets the list of search domains of the resolver, when not given the default list is used (platform dependent)
   | `ndots` | sets the number of dots which must appear in a name before an initial absolute query is made, defaults to `-1`
   | `decode-idn?` | set if domain / host names should be decoded to unicode when received, defaults to `true`
   | `recursion-desired?` | if set to `true`, the resolver sends a DNS query with the RD (recursion desired) flag set, defaults to `true`
   | `name-servers` | optional list of DNS server addresses, automatically discovered when not set (platform dependent)
   | `epoll?` | if `true`, uses `epoll` when available, defaults to `false`"
  [{:keys [max-payload-size
           max-queries-per-resolve
           address-types
           query-timeout
           min-ttl
           max-ttl
           negative-ttl
           trace-enabled?
           opt-resources-enabled?
           search-domains
           ndots
           decode-idn?
           recursion-desired?
           name-servers
           epoll?]
    :or {max-payload-size 4096
         max-queries-per-resolve 16
         query-timeout 5000
         min-ttl 0
         max-ttl Integer/MAX_VALUE
         trace-enabled? false
         opt-resources-enabled? true
         ndots -1
         decode-idn? true
         recursion-desired? true
         epoll? false}}]
  (let [^Class
        channel-type (if (and epoll? (epoll-available?))
                       EpollDatagramChannel
                       NioDatagramChannel)

        b (cond-> (doto (DnsNameResolverBuilder.)
                    (.channelType channel-type)
                    (.maxPayloadSize max-payload-size)
                    (.maxQueriesPerResolve max-queries-per-resolve)
                    (.queryTimeoutMillis query-timeout)
                    (.ttl min-ttl max-ttl)
                    (.traceEnabled trace-enabled?)
                    (.optResourceEnabled opt-resources-enabled?)
                    (.ndots ndots)
                    (.decodeIdn decode-idn?)
                    (.recursionDesired recursion-desired?))

                  (some? address-types)
                  (.resolvedAddressTypes (convert-address-types address-types))

                  (some? negative-ttl)
                  (.negativeTtl negative-ttl)

                  (and (some? search-domains)
                       (not (empty? search-domains)))
                  (.searchDomains search-domains)

                  (and (some? name-servers)
                       (not (empty? name-servers)))
                  (.nameServerProvider ^DnsServerAddressStreamProvider
                                       (dns-name-servers-provider name-servers)))]
    (DnsAddressResolverGroup. b)))

(defn get-default-event-loop-threads
  "Determines the default number of threads to use for a Netty EventLoopGroup.
   This mimics the default used by Netty as of version 4.1."
  []
  (let [cpu-count (->> (Runtime/getRuntime) (.availableProcessors))]
    (max 1 (SystemPropertyUtil/getInt "io.netty.eventLoopThreads" (* cpu-count 2)))))

(def ^ThreadLocal executor-thread-local (ThreadLocal.))

(defn ^ThreadFactory enumerating-thread-factory
  [prefix daemon?]
  (let [num-threads (atom 0)
        name-generator #(str prefix "-" (swap! num-threads inc))]
    (reify ThreadFactory
      (newThread [_ runnable]
        (let [name (name-generator)
              curr-loader (.getClassLoader (class enumerating-thread-factory))]
          (doto
            (Thread. nil runnable name)
            (.setDaemon daemon?)
            (.setContextClassLoader curr-loader)))))))

(def ^String client-event-thread-pool-name "aleph-netty-client-event-pool")

(def epoll-client-group
  (delay
    (let [thread-count (get-default-event-loop-threads)
          thread-factory (enumerating-thread-factory client-event-thread-pool-name true)]
      (EpollEventLoopGroup. (long thread-count) thread-factory))))

(def nio-client-group
  (delay
    (let [thread-count (get-default-event-loop-threads)
          thread-factory (enumerating-thread-factory client-event-thread-pool-name true)]
      (NioEventLoopGroup. (long thread-count) thread-factory))))

(defn pipeline-initializer [pipeline-builder]
  (proxy [ChannelInitializer] []
    (initChannel [^Channel ch]
      (pipeline-builder ^ChannelPipeline (.pipeline ch)))))

(definline put-result!
  [ch result]
  `(if (some? ~result)
     (async/put! ~ch ~result)
     (async/close! ~ch)))

(defn wrap-future
  [^Future f]
  (if (and f (not (instance? VoidChannelPromise f)))
    (if (.isSuccess f)
      (doto (async/promise-chan) (put-result! (.getNow f)))
      (let [ch (async/promise-chan)
            class-loader (or (clojure.lang.RT/baseLoader)
                             (clojure.lang.RT/makeClassLoader))]
        (.addListener f
          (reify GenericFutureListener
            (operationComplete [_ _]
              (try
                (. clojure.lang.Var
                   (pushThreadBindings {clojure.lang.Compiler/LOADER
                                        class-loader}))
                (cond
                  (.isSuccess f)
                  (put-result! ch (.getNow f))

                  (.isCancelled f)
                  (async/put! ch (CancellationException. "future is cancelled"))

                  (some? (.cause f))
                  (if (instance? java.nio.channels.ClosedChannelException (.cause f))
                    (async/close! ch)
                    (async/put! ch (.cause f)))

                  :else
                  (async/put! ch (IllegalStateException. "future in unknown state")))
                (finally
                  (. clojure.lang.Var (popThreadBindings)))))))
        ch))
    (doto (async/chan) (async/close!))))

(defn create-client
  ([pipeline-builder
    ssl-context
    bootstrap-transform
    remote-address
    local-address
    epoll?]
   (create-client pipeline-builder
                 ssl-context
                 bootstrap-transform
                 remote-address
                 local-address
                 epoll?
                 nil))
  ([pipeline-builder
    ^SslContext ssl-context
    bootstrap-transform
    ^SocketAddress remote-address
    ^SocketAddress local-address
    epoll?
    name-resolver]
   (let [^Class
         channel (if (and epoll? (epoll-available?))
                   EpollSocketChannel
                   NioSocketChannel)

         pipeline-builder (if ssl-context
                            (fn [^ChannelPipeline p]
                              (.addLast p "ssl-handler"
                                       (.newHandler ^SslContext ssl-context
                                                    (-> p .channel .alloc)
                                                    (.getHostName ^InetSocketAddress remote-address)
                                                    (.getPort ^InetSocketAddress remote-address)))
                              (pipeline-builder p))
                            pipeline-builder)]
     (fasync/go-try
       (let [client-group (if (and epoll? (epoll-available?))
                            @epoll-client-group
                            @nio-client-group)
             resolver' (when (some? name-resolver)
                         (cond
                           (= :default name-resolver) nil
                           (= :noop name-resolver) NoopAddressResolverGroup/INSTANCE
                           (instance? AddressResolverGroup name-resolver) name-resolver))
             b (doto (Bootstrap.)
                 (.option ChannelOption/SO_REUSEADDR true)
                 ; TODO
                 ;(.option ChannelOption/MAX_MESSAGES_PER_READ Integer/MAX_VALUE)
                 (.group client-group)
                 (.channel channel)
                 (.handler (pipeline-initializer pipeline-builder))
                 (.resolver resolver')
                 bootstrap-transform)

             f (if local-address
                 (.connect b remote-address local-address)
                 (.connect b remote-address))]

         (fasync/<? (wrap-future f))
         (.channel ^ChannelFuture f))))))

(defn insecure-ssl-client-context []
  (-> (SslContextBuilder/forClient)
      (.trustManager InsecureTrustManagerFactory/INSTANCE)
      .build))

(defn check-ssl-args
  [private-key certificate-chain]
  (when-not
    (or (and (instance? File private-key) (instance? File certificate-chain))
        (and (instance? InputStream private-key) (instance? InputStream certificate-chain))
        (and (instance? PrivateKey private-key) (instance? (class (into-array X509Certificate [])) certificate-chain)))
    (throw (IllegalArgumentException. "ssl-client-context arguments invalid"))))

(defn ssl-client-context
  "Creates a new client SSL context.

  Keyword arguments are:

  |:---|:----
  | `private-key` | A `java.io.File`, `java.io.InputStream`, or `java.security.PrivateKey` containing the client-side private key.
  | `certificate-chain` | A `java.io.File`, `java.io.InputStream`, or array of `java.security.cert.X509Certificate` containing the client's certificate chain.
  | `private-key-password` | A string, the private key's password (optional).
  | `trust-store` | A `java.io.File`, `java.io.InputStream`, array of `java.security.cert.X509Certificate`, or a `javax.net.ssl.TrustManagerFactory` to initialize the context's trust manager.

  Note that if specified, the types of `private-key` and `certificate-chain` must be
  \"compatible\": either both input streams, both files, or a private key and an array
  of certificates."
  ([] (ssl-client-context {}))
  ([{:keys [private-key private-key-password certificate-chain trust-store application-protocol-config ssl-provider]}]
   (cond-> (SslContextBuilder/forClient)

           (and private-key certificate-chain)
           (as-> ctx
              (do
                (check-ssl-args private-key certificate-chain)
                (if (instance? (class (into-array X509Certificate [])) certificate-chain)
                  (.keyManager ctx
                               private-key
                               private-key-password
                               certificate-chain)
                  (.keyManager ctx
                               certificate-chain
                               private-key
                               private-key-password))))

           trust-store
           (.trustManager trust-store)

           application-protocol-config
           (.applicationProtocolConfig application-protocol-config)

           ssl-provider
           (.sslProvider ssl-provider)

           :then
           (.build))))

(defmacro channel-inbound-handler
  [& {:as handlers}]
  `(reify
     ChannelHandler
     ChannelInboundHandler

     (handlerAdded
       ~@(or (:handler-added handlers) `([_# _#])))
     (handlerRemoved
       ~@(or (:handler-removed handlers) `([_# _#])))
     (exceptionCaught
       ~@(or (:exception-caught handlers)
             `([_# ctx# cause#]
               (.fireExceptionCaught ctx# cause#))))
     (channelRegistered
       ~@(or (:channel-registered handlers)
             `([_# ctx#]
               (.fireChannelRegistered ctx#))))
     (channelUnregistered
       ~@(or (:channel-unregistered handlers)
             `([_# ctx#]
               (.fireChannelUnregistered ctx#))))
     (channelActive
       ~@(or (:channel-active handlers)
             `([_# ctx#]
               (.fireChannelActive ctx#))))
     (channelInactive
       ~@(or (:channel-inactive handlers)
             `([_# ctx#]
               (.fireChannelInactive ctx#))))
     (channelRead
       ~@(or (:channel-read handlers)
             `([_# ctx# msg#]
               (.fireChannelRead ctx# msg#))))
     (channelReadComplete
       ~@(or (:channel-read-complete handlers)
             `([_# ctx#]
               (.fireChannelReadComplete ctx#))))
     (userEventTriggered
       ~@(or (:user-event-triggered handlers)
             `([_# ctx# evt#]
               (.fireUserEventTriggered ctx# evt#))))
     (channelWritabilityChanged
       ~@(or (:channel-writability-changed handlers)
             `([_# ctx#]
               (.fireChannelWritabilityChanged ctx#))))))

(defmacro channel-handler
  [& {:as handlers}]
  `(reify
     ChannelHandler
     ChannelInboundHandler
     ChannelOutboundHandler

     (handlerAdded
       ~@(or (:handler-added handlers) `([_# _#])))
     (handlerRemoved
       ~@(or (:handler-removed handlers) `([_# _#])))
     (exceptionCaught
       ~@(or (:exception-caught handlers)
             `([_# ctx# cause#]
               (.fireExceptionCaught ctx# cause#))))
     (channelRegistered
       ~@(or (:channel-registered handlers)
             `([_# ctx#]
               (.fireChannelRegistered ctx#))))
     (channelUnregistered
       ~@(or (:channel-unregistered handlers)
             `([_# ctx#]
               (.fireChannelUnregistered ctx#))))
     (channelActive
       ~@(or (:channel-active handlers)
             `([_# ctx#]
               (.fireChannelActive ctx#))))
     (channelInactive
       ~@(or (:channel-inactive handlers)
             `([_# ctx#]
               (.fireChannelInactive ctx#))))
     (channelRead
       ~@(or (:channel-read handlers)
             `([_# ctx# msg#]
               (.fireChannelRead ctx# msg#))))
     (channelReadComplete
       ~@(or (:channel-read-complete handlers)
             `([_# ctx#]
               (.fireChannelReadComplete ctx#))))
     (userEventTriggered
       ~@(or (:user-event-triggered handlers)
             `([_# ctx# evt#]
               (.fireUserEventTriggered ctx# evt#))))
     (channelWritabilityChanged
       ~@(or (:channel-writability-changed handlers)
             `([_# ctx#]
               (.fireChannelWritabilityChanged ctx#))))
     (bind
       ~@(or (:bind handlers)
             `([_# ctx# local-address# promise#]
               (.bind ctx# local-address# promise#))))
     (connect
       ~@(or (:connect handlers)
             `([_# ctx# remote-address# local-address# promise#]
               (.connect ctx# remote-address# local-address# promise#))))
     (disconnect
       ~@(or (:disconnect handlers)
             `([_# ctx# promise#]
               (.disconnect ctx# promise#))))
     (close
       ~@(or (:close handlers)
             `([_# ctx# promise#]
               (.close ctx# promise#))))
     (read
       ~@(or (:read handlers)
             `([_# ctx#]
               (.read ctx#))))
     (write
       ~@(or (:write handlers)
             `([_# ctx# msg# promise#]
               (.write ctx# msg# promise#))))
     (flush
       ~@(or (:flush handlers)
             `([_# ctx#]
               (.flush ctx#))))))

(defrecord MetaChan [port]
  impl/ReadPort
  (take! [_ handler] (impl/take! port handler))

  impl/WritePort
  (put! [_ val handler] (impl/put! port val handler))

  impl/Channel
  (close! [_] (impl/close! port))
  (closed? [_] (impl/closed? port)))

(defn buffered-chan
  [^Channel ch])

(definline release [x]
  `(io.netty.util.ReferenceCountUtil/release ~x))

(definline buf->array [buf]
  `(ByteBufUtil/getBytes ~buf))

(defn bufs->array [bufs]
  (ByteBufUtil/getBytes (Unpooled/wrappedBuffer (into-array ByteBuf bufs))))

(def ^:const array-class (class (clojure.core/byte-array 0)))

(defn allocate [x]
  (if (instance? Channel x)
    (-> ^Channel x .alloc .ioBuffer)
    (-> ^ChannelHandlerContext x .alloc .ioBuffer)))

(let [charset (Charset/forName "UTF-8")]
  (defn append-to-buf! [^ByteBuf buf x]
    (cond
      (instance? array-class x)
      (.writeBytes buf ^bytes x)

      (instance? String x)
      (.writeBytes buf (.getBytes ^String x charset))

      (instance? ByteBuf x)
      (let [b (.writeBytes buf ^ByteBuf x)]
        (release x)
        b)

      :else
      (.writeBytes buf (bs/to-byte-buffer x))))

  (defn to-byte-buf
    ([x]
     (cond
       (nil? x)
       Unpooled/EMPTY_BUFFER

       (instance? array-class x)
       (Unpooled/copiedBuffer ^bytes x)

       (instance? String x)
       (-> ^String x (.getBytes charset) ByteBuffer/wrap Unpooled/wrappedBuffer)

       (instance? ByteBuffer x)
       (Unpooled/wrappedBuffer ^ByteBuffer x)

       (instance? ByteBuf x)
       x

       :else
       (bs/convert x ByteBuf)))
    ([ch x]
     (if (nil? x)
       Unpooled/EMPTY_BUFFER
       (doto (allocate ch)
         (append-to-buf! x))))))

(defn write [x msg]
  (wrap-future
    (if (instance? Channel x)
      (.write ^Channel x msg (.voidPromise ^Channel x))
      (.write ^ChannelHandlerContext x msg (.voidPromise ^ChannelHandlerContext x)))))

(defn write-and-flush
  [x msg]
  (wrap-future
    (if (instance? Channel x)
      (.writeAndFlush ^Channel x msg)
      (.writeAndFlush ^ChannelHandlerContext x msg))))

(defn flush [x]
  (if (instance? Channel x)
    (.flush ^Channel x)
    (.flush ^ChannelHandlerContext x)))

(defn close [x]
  (wrap-future
    (if (instance? Channel x)
      (.close ^Channel x)
      (.close ^ChannelHandlerContext x))))

(defn ^Channel channel [x]
  (if (instance? Channel x)
    x
    (.channel ^ChannelHandlerContext x)))

(defmacro safe-execute [ch & body]
  `(let [f# (fn [] ~@body)
         event-loop# (-> ~ch bet.impl.netty/channel .eventLoop)
         ch# (async/promise-chan)]
     (if (.inEventLoop event-loop#)
       (doto ch# (async/put! (f#)))
       (.execute event-loop#
                 (fn []
                   (try
                     (async/put! ch# (f#))
                     (catch Throwable e#
                       (async/put! ch# e#))))))
     ch#))