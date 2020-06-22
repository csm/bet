(ns bet.impl.http.client-middleware
  (:require [clojure.string :as str]
            [potemkin :as p])
  (:import (io.netty.handler.codec.http HttpHeaders HttpHeaderNames)
           (bet.impl.http.core HeaderMap)
           (io.netty.handler.codec.http.cookie ClientCookieDecoder ClientCookieEncoder DefaultCookie)
           (java.net IDN URL URLDecoder URLEncoder)))

(defn url-encode
  ([^String s]
   (url-encode s "UTF-8"))
  ([^String s ^String encoding]
   (URLEncoder/encode s encoding)))

(defn url-encode-illegal-characters
  "Takes a raw url path or query and url-encodes any illegal characters.
  Minimizes ambiguity by encoding space to %20."
  [path-or-query]
  (when path-or-query
    (-> path-or-query
        (str/replace " " "%20")
        (str/replace
          #"[^a-zA-Z0-9\.\-\_\~\!\$\&\'\(\)\*\+\,\;\=\:\@\/\%\?]"
          url-encode))))

(defn parse-url
  "Parse a URL string into a map of interesting parts."
  [url]
  (let [url-parsed (URL. url)]
    {:scheme       (keyword (.getProtocol url-parsed))
     :server-name  (.getHost url-parsed)
     :server-port  (when (pos? (.getPort url-parsed)))
     :uri          (url-encode-illegal-characters (.getPath url-parsed))
     :user-info    (when-let [user-info (.getUserInfo url-parsed)]
                     (URLDecoder/decode user-info "UTF-8"))
     :query-string (url-encode-illegal-characters (.getQuery url-parsed))}))

(defprotocol CookieSpec
  "Implement rules for accepting and returning cookies"
  (parse-cookie [this cookie-str])
  (write-cookies [this cookies])
  (match-cookie-origin? [this origin cookie]))

(defprotocol CookieStore
  (get-cookies [this])
  (add-cookies! [this cookies]))

(def ^String cookie-header-name (.toString HttpHeaderNames/COOKIE))
(def ^String set-cookie-header-name (.toString HttpHeaderNames/SET_COOKIE))

;; That's a pretty liberal domain check.
;; Under RFC2965 your domain should contain leading "." to match successors,
;; but this extra dot is ignored by more recent specifications.
;; So, if you need obsolete RFC2965 compatible behavior, feel free to
;; plug your one `CookieSpec` with redefined `match-cookie` impl.
(defn match-cookie-domain? [origin domain]
  (let [origin' (if (str/starts-with? origin ".") origin (str "." origin))
        domain' (if (str/starts-with? domain ".") domain (str "." domain))]
    (str/ends-with? origin' domain')))

;; Reimplementation of org.apache.http.impl.cookie.BasicPathHandler path match logic
(defn match-cookie-path? [origin-path cookie-path]
  (let [norm-path (if (and (not= "/" cookie-path) (= \/ (last cookie-path)))
                    (subs cookie-path 0 (dec (count cookie-path)))
                    cookie-path)]
    (and (str/starts-with? origin-path norm-path)
         (or (= "/" norm-path)
             (= (count origin-path) (count norm-path))
             (= \/ (-> origin-path (subs (count norm-path)) first))))))


(let [uri->path (fn [uri]
                  (cond
                    (nil? uri) "/"
                    (str/starts-with? uri "/") uri
                    :else (str "/" uri)))]

  (defn req->cookie-origin [{:keys [url] :as req}]
    (if (some? url)
      (let [{:keys [server-name server-port uri scheme]} (parse-url url)]
        {:host    server-name
         :port    server-port
         :secure? (= :https scheme)
         :path    (uri->path uri)})
      {:host    (some-> (or (:host req) (:server-name req)) IDN/toASCII)
       :port    (or (:port req) (:server-port req) -1)
       :secure? (= :https (or (:scheme req) :http))
       :path    (uri->path (:uri req))})))

(defn cookie->netty-cookie [{:keys [domain http-only? secure? max-age name path value]}]
  (doto (DefaultCookie. name value)
    (.setDomain domain)
    (.setPath path)
    (.setHttpOnly (or http-only? false))
    (.setSecure (or secure? false))
    (.setMaxAge (or max-age io.netty.handler.codec.http.cookie.Cookie/UNDEFINED_MAX_AGE))))

(p/def-derived-map Cookie
                   [^DefaultCookie cookie]
                   :domain (.domain cookie)
                   :http-only? (.isHttpOnly cookie)
                   :secure? (.isSecure cookie)
                   :max-age (let [max-age (.maxAge cookie)]
                              (when-not (= max-age io.netty.handler.codec.http.cookie.Cookie/UNDEFINED_MAX_AGE)
                                max-age))
                   :name (.name cookie)
                   :path (.path cookie)
                   :value (.value cookie))

(defn netty-cookie->cookie [^DefaultCookie cookie]
  (->Cookie cookie))

(defn cookie-expired? [{:keys [created max-age]}]
  (cond
    (nil? max-age) false
    (>= 0 max-age) true
    (nil? created) false
    :else (>= (System/currentTimeMillis) (+ created max-age))))

(def default-cookie-spec
  "Default cookie spec implementation providing RFC6265 compliant behavior
   with no validation for cookie names and values. In case you need strict validation
   feel free to create impl. using {ClientCookieDecoder,ClientCookiEncoder}/STRICT
   instead of LAX instances"
  (reify CookieSpec
    (parse-cookie [_ cookie-str]
      (.decode ClientCookieDecoder/LAX cookie-str))
    (write-cookies [_ cookies]
      (.encode ClientCookieEncoder/LAX ^java.lang.Iterable cookies))
    (match-cookie-origin? [_ origin {:keys [domain path secure?] :as cookie}]
      (cond
        (and secure? (not (:secure? origin)))
        false

        (and (some? domain)
             (not (match-cookie-domain? (:host origin) domain)))
        false

        (and (some? path)
             (not (match-cookie-path? (:path origin) path)))
        false

        (cookie-expired? cookie)
        false

        :else
        true))))

(defn decode-set-cookie-header
  ([header]
   (decode-set-cookie-header default-cookie-spec header))
  ([cookie-spec header]
   (some->> header
            (parse-cookie cookie-spec)
            (netty-cookie->cookie))))

(defn extract-cookies-from-response-headers
  ([headers]
   (extract-cookies-from-response-headers default-cookie-spec headers))
  ([cookie-spec ^HeaderMap headers]
   (let [^HttpHeaders raw-headers (.headers headers)]
     (->> (.getAll raw-headers set-cookie-header-name)
         (map (partial decode-set-cookie-header cookie-spec))))))

(defn handle-cookies [{:keys [cookie-store cookie-spec]
                       :or {cookie-spec default-cookie-spec}}
                      {:keys [headers] :as rsp}]
  (if (nil? cookie-store)
    rsp
    (let [cookies (extract-cookies-from-response-headers cookie-spec headers)]
      (when-not (empty? cookies)
        (add-cookies! cookie-store cookies))
      ;; pairing with what clj_http does with parsed cookies
      ;; (as it's impossible to tell what cookies were parsed
      ;; from this particular response, you will be forced
      ;; to parse header twice otherwise)
      (assoc rsp :cookies cookies))))

(let [param-? (memoize #(keyword (str (name %) "?")))]
  (defn opt
    "Check the request parameters for a keyword  boolean option, with or without
     the ?

     Returns false if either of the values are false, or the value of
     (or key1 key2) otherwise (truthy)"
    [req param]
    (let [param' (param-? param)
          v1 (clojure.core/get req param)
          v2 (clojure.core/get req param')]
      (if (false? v1)
        false
        (if (false? v2)
          false
          (or v1 v2))))))

(defn follow-redirect
  "Attempts to follow the redirects from the \"location\" header, if no such
  header exists (bad server!), returns the response without following the
  request."
  [client
   {:keys [uri url scheme server-name server-port trace-redirects]
    :or {trace-redirects []}
    :as req}
   {:keys [body] :as rsp}]
  (let [url (or url (str (name scheme) "://" server-name
                         (when server-port (str ":" server-port)) uri))]
    (if-let [raw-redirect (get-in rsp [:headers "location"])]
      (let [redirect (str (URL. (URL. url) raw-redirect))]
        (client
          (-> req
              (dissoc :query-params)
              (assoc :url redirect)
              (assoc :trace-redirects (conj trace-redirects redirect)))))
      ;; Oh well, we tried, but if no location is set, return the response
      rsp)))

(defn redirect?
  [{:keys [status]}]
  (<= 300 status 399))

(defn handle-redirects
  "Middleware that follows redirects in the response. A slingshot exception is
  thrown if too many redirects occur. Options
  :follow-redirects - default:true, whether to follow redirects
  :max-redirects - default:20, maximum number of redirects to follow
  :force-redirects - default:false, force redirecting methods to GET requests

  In the response:
  :redirects-count - number of redirects
  :trace-redirects - vector of sites the request was redirected from"
  [client
   {:keys [request-method max-redirects redirects-count trace-redirects url]
    :or {redirects-count 0
         ;; max-redirects default taken from Firefox
         max-redirects 20}
    :as req}
   {:keys [status] :as rsp}]
  (let [rsp-r (if (empty? trace-redirects)
                rsp
                (assoc rsp :trace-redirects trace-redirects))]
    (cond
      (false? (opt req :follow-redirects))
      rsp

      (not (redirect? rsp-r))
      rsp-r

      (and max-redirects (> redirects-count max-redirects))
      (if (opt req :throw-exceptions)
        (throw (ex-info (str "too many redirects: " redirects-count) req))
        rsp-r)

      (= 303 status)
      (follow-redirect client
                       (assoc req
                         :request-method :get
                         :redirects-count (inc redirects-count))
                       rsp-r)


      (#{301 302} status)
      (cond
        (#{:get :head} request-method)
        (follow-redirect client
                         (assoc req
                           :redirects-count (inc redirects-count))
                         rsp-r)

        (opt req :force-redirects)
        (follow-redirect client
                         (assoc req
                           :request-method :get
                           :redirects-count (inc redirects-count))
                         rsp-r)

        :else
        rsp-r)

      (= 307 status)
      (if (or (#{:get :head} request-method)
              (opt req :force-redirects))
        (follow-redirect client
                         (assoc req :redirects-count (inc redirects-count)) rsp-r)
        rsp-r)

      :else
      rsp-r)))