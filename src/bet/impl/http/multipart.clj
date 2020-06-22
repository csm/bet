(ns bet.impl.http.multipart
  (:require [bet.impl.netty :as netty])
  (:import (io.netty.handler.codec.http HttpConstants DefaultHttpRequest)
           (io.netty.handler.codec.http.multipart MemoryAttribute HttpPostRequestEncoder)
           (java.net URLConnection)
           (java.nio.charset Charset)
           (java.util Locale)
           (java.io File)))

(defn mime-type-descriptor
  [^String mime-type ^String encoding]
  (str
    (-> (or mime-type "application/octet-stream") .trim (.toLowerCase Locale/US))
    (when encoding
      (str "; charset=" encoding))))

(defn encode-request [^DefaultHttpRequest req parts]
  (let [^HttpPostRequestEncoder encoder (HttpPostRequestEncoder. req true)]
    (doseq [{:keys [part-name content mime-type charset name]} parts]
      (if (instance? File content)
        (let [filename (.getName ^File content)
              name (or name filename)
              mime-type (or mime-type
                            (URLConnection/guessContentTypeFromName filename))
              content-type (mime-type-descriptor mime-type charset)]
          (.addBodyFileUpload encoder
                              part-name
                              name
                              content
                              content-type
                              false))
        (let [^Charset charset (cond
                                 (nil? charset)
                                 HttpConstants/DEFAULT_CHARSET

                                 (string? charset)
                                 (Charset/forName charset)

                                 (instance? Charset charset)
                                 charset)
              attr (if (string? content)
                     (MemoryAttribute. ^String part-name ^String content charset)
                     (doto (MemoryAttribute. ^String part-name charset)
                       (.addContent (netty/to-byte-buf content) true)))]
          (.addBodyHttpData encoder attr))))
    (let [req' (.finalizeRequest encoder)]
      [req' (when (.isChunked encoder) encoder)])))