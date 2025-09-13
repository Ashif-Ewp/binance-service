// // /home/ashif/chart_raiders/binance-service/scale/udf.js
// const Binance = require("./binance");
// const WebSocket = require("ws");
// const Redis = require("ioredis");
// const Bottleneck = require("bottleneck");
// const redis = new Redis({
//   host: "127.0.0.1" || "localhost",
//   port: 6379,
//   password: "my_master_password",
// });
// const limiter = new Bottleneck({ maxConcurrent: 10, minTime: 50 });

// class UDFError extends Error {}
// class SymbolNotFound extends UDFError {}
// class InvalidResolution extends UDFError {}

// class UDF {
//   constructor() {
//     this.binance = new Binance();
//     this.supportedResolutions = [
//       "1",
//       "3",
//       "5",
//       "15",
//       "30",
//       "60",
//       "120",
//       "240",
//       "360",
//       "480",
//       "720",
//       "1D",
//       "3D",
//       "1W",
//       "1M",
//     ];
//     this.intervalMap = {
//       1: "1m",
//       3: "3m",
//       5: "5m",
//       15: "15m",
//       30: "30m",
//       60: "1h",
//       120: "2h",
//       240: "4h",
//       360: "6h",
//       480: "8h",
//       720: "12h",
//       D: "1d",
//       "1D": "1d",
//       "3D": "3d",
//       W: "1w",
//       "1W": "1w",
//       M: "1M",
//       "1M": "1M",
//     };
//     this.subscribers = new Map();
//     this.latestKlines = new Map();
//     this.binanceWs = null;
//     this.subscriptions = new Set();
//     this.connectBinanceWS();
//     this.preloadKlines();

//     setInterval(() => this.loadSymbols(), 30000);
//     this.loadSymbols();
//   }

//   async preloadKlines() {
//     const popularPairs = ["BTCUSDT"];
//     const intervals = ["1m", "5m", "1h"];
//     for (const symbol of popularPairs) {
//       for (const interval of intervals) {
//         try {
//           const klines = await limiter.schedule(() =>
//             this.binance.klines(
//               symbol,
//               interval,
//               Date.now() - 24 * 3600 * 1000,
//               Date.now(),
//               500
//             )
//           );
//           const key = `kline:${symbol}@${interval}:recent`;
//           await redis.setex(key, 3600, JSON.stringify(klines));
//         } catch (err) {
//           console.error(`Preload error for ${symbol}@${interval}:`, err);
//         }
//       }
//     }
//   }

//   async connectBinanceWS() {
//     if (this.binanceWs) return;
//     this.binanceWs = new WebSocket("wss://fstream.binance.com/ws");

//     this.binanceWs.on("open", async () => {
//       console.log(`Worker ${process.pid}: Binance WebSocket connected`);
//       const streams = (await redis.smembers("binance:subscriptions")) || [];
//       for (const stream of streams) {
//         this.subscriptions.add(stream);
//         this.binanceWs.send(
//           JSON.stringify({
//             method: "SUBSCRIBE",
//             params: [stream],
//             id: Date.now(),
//           })
//         );
//       }
//     });

//     this.binanceWs.on("close", () => {
//       console.log(
//         `Worker ${process.pid}: Binance WebSocket closed, reconnecting...`
//       );
//       this.binanceWs = null;
//       setTimeout(() => this.connectBinanceWS(), 1000);
//     });

//     this.binanceWs.on("error", (err) => {
//       console.error(`Worker ${process.pid}: Binance WebSocket error:`, err);
//     });

//     this.binanceWs.on("message", async (data) => {
//       const msg = JSON.parse(data.toString());
//       console.log({ msg });
//       if (msg.id && msg.result !== null) {
//         console.error(
//           `Worker ${process.pid}: Binance subscription failed:`,
//           msg
//         );
//         return;
//       }
//       if (msg.e === "kline") {
//         const k = msg.k;
//         const key = `${k.s}@${this.intervalMap[k.i] || k.i}`;
//         const bar = {
//           time: Math.floor(k.t / 1000),
//           open: parseFloat(k.o),
//           high: parseFloat(k.h),
//           low: parseFloat(k.l),
//           close: parseFloat(k.c),
//           volume: parseFloat(k.v),
//         };
//         console.log({ bar });
//         this.latestKlines.set(key, bar);
//         await redis.setex(`kline:${key}`, 3600, JSON.stringify(bar));

//         const subscribers = this.subscribers.get(key) || [];
//         setImmediate(() => {
//           for (const callback of subscribers) callback(bar);
//         });

//         if (k.x) {
//           const nextTime = k.T + 1;
//           const nextBar = {
//             time: Math.floor(nextTime / 1000),
//             open: parseFloat(k.c),
//             high: parseFloat(k.c),
//             low: parseFloat(k.c),
//             close: parseFloat(k.c),
//             volume: 0,
//           };
//           this.latestKlines.set(key, nextBar);
//           await redis.setex(`kline:${key}`, 3600, JSON.stringify(nextBar));
//         }
//       }
//     });
//   }

//   async subscribeBars(symbol, resolution, onTick, ws) {
//     const interval = this.intervalMap[resolution];
//     if (!interval) {
//       console.error(`Worker ${process.pid}: Invalid resolution: ${resolution}`);
//       return;
//     }
//     const key = `${symbol}@${interval}`;
//     if (!this.subscribers.has(key)) this.subscribers.set(key, []);
//     this.subscribers.get(key).push(onTick);
//     ws._callbacks = ws._callbacks || [];
//     ws._callbacks.push(onTick);

//     const stream = `${symbol.toLowerCase()}@kline_${interval}`;
//     if (this.subscriptions.size >= 300) {
//       console.warn(
//         `Worker ${process.pid}: Stream limit reached, using HTTP polling`
//       );
//       setInterval(async () => {
//         try {
//           const klines = await limiter.schedule(() =>
//             this.binance.klines(
//               symbol,
//               interval,
//               Date.now() - 60000,
//               Date.now(),
//               1
//             )
//           );
//           if (klines.length > 0) {
//             const bar = {
//               time: Math.floor(klines[0][0] / 1000),
//               open: parseFloat(klines[0][1]),
//               high: parseFloat(klines[0][2]),
//               low: parseFloat(klines[0][3]),
//               close: parseFloat(klines[0][4]),
//               volume: parseFloat(klines[0][5]),
//             };
//             this.latestKlines.set(key, bar);
//             await redis.setex(`kline:${key}`, 3600, JSON.stringify(bar));
//             const subscribers = this.subscribers.get(key) || [];
//             for (const callback of subscribers) callback(bar);
//           }
//         } catch (err) {
//           console.error(`Polling error for ${key}:`, err);
//         }
//       }, 5000);
//       return;
//     }

//     if (!this.subscriptions.has(stream)) {
//       this.subscriptions.add(stream);
//       await redis.sadd("binance:subscriptions", stream);
//       if (this.binanceWs && this.binanceWs.readyState === WebSocket.OPEN) {
//         this.binanceWs.send(
//           JSON.stringify({
//             method: "SUBSCRIBE",
//             params: [stream],
//             id: Date.now(),
//           })
//         );
//       }
//     }

//     const cached = await redis.get(`kline:${key}`);
//     if (cached) {
//       onTick(JSON.parse(cached));
//     } else {
//       const klines = await limiter.schedule(() =>
//         this.binance.klines(
//           symbol,
//           interval,
//           Date.now() - 60000 * 60,
//           Date.now(),
//           1
//         )
//       );
//       if (klines && klines.length > 0) {
//         const b = klines[0];
//         const bar = {
//           time: Math.floor(b[0] / 1000),
//           open: parseFloat(b[1]),
//           high: parseFloat(b[2]),
//           low: parseFloat(b[3]),
//           close: parseFloat(b[4]),
//           volume: parseFloat(b[5]),
//         };
//         this.latestKlines.set(key, bar);
//         await redis.setex(`kline:${key}`, 3600, JSON.stringify(bar));
//         onTick(bar);
//       }
//     }
//   }

//   async unsubscribeBars(ws) {
//     for (const [key, callbacks] of this.subscribers.entries()) {
//       const newCallbacks = callbacks.filter(
//         (cb) => !ws._callbacks?.includes(cb)
//       );
//       this.subscribers.set(key, newCallbacks);
//       if (newCallbacks.length === 0) {
//         this.subscribers.delete(key);
//         const [symbol, interval] = key.split("@");
//         const stream = `${symbol.toLowerCase()}@kline_${interval}`;
//         this.subscriptions.delete(stream);
//         await redis.srem("binance:subscriptions", stream);
//         if (this.binanceWs && this.binanceWs.readyState === WebSocket.OPEN) {
//           this.binanceWs.send(
//             JSON.stringify({
//               method: "UNSUBSCRIBE",
//               params: [stream],
//               id: Date.now(),
//             })
//           );
//         }
//       }
//     }
//   }

//   loadSymbols() {
//     function pricescale(symbol) {
//       for (let filter of symbol.filters) {
//         if (filter.filterType == "PRICE_FILTER") {
//           return Math.round(1 / parseFloat(filter.tickSize));
//         }
//       }
//       return 1;
//     }

//     const promise = limiter
//       .schedule(() => this.binance.exchangeInfo())
//       .catch((err) => {
//         console.error(`Worker ${process.pid}:`, err);
//         setTimeout(() => this.loadSymbols(), 1000);
//       });
//     this.symbols = promise.then((info) => {
//       const symbols = info.symbols.map((symbol) => ({
//         symbol: symbol.symbol,
//         ticker: symbol.symbol,
//         name: symbol.symbol,
//         full_name: symbol.symbol,
//         description: `${symbol.baseAsset} / ${symbol.quoteAsset}`,
//         exchange: "BINANCE",
//         listed_exchange: "BINANCE",
//         type: "crypto",
//         currency_code: symbol.quoteAsset,
//         session: "24x7",
//         timezone: "UTC",
//         minmovement: 1,
//         minmov: 1,
//         minmovement2: 0,
//         minmov2: 0,
//         pricescale: pricescale(symbol),
//         supported_resolutions: this.supportedResolutions,
//         has_intraday: true,
//         has_daily: true,
//         has_weekly_and_monthly: true,
//         data_status: "streaming",
//       }));
//       redis.setex("udf:symbols", 3600, JSON.stringify(symbols));
//       return symbols;
//     });
//     this.allSymbols = promise.then((info) => {
//       let set = new Set();
//       for (const symbol of info.symbols) {
//         set.add(symbol.symbol);
//       }
//       return set;
//     });
//   }

//   async checkSymbol(symbol) {
//     const symbols = await this.allSymbols;
//     return symbols.has(symbol);
//   }

//   asTable(items) {
//     let result = {};
//     for (const item of items) {
//       for (const key in item) {
//         if (!result[key]) result[key] = [];
//         result[key].push(item[key]);
//       }
//     }
//     for (const key in result) {
//       const values = [...new Set(result[key])];
//       if (values.length === 1) result[key] = values[0];
//     }
//     return result;
//   }

//   async config() {
//     return {
//       exchanges: [
//         { value: "BINANCE", name: "Binance", desc: "Binance Exchange" },
//       ],
//       symbols_types: [{ value: "crypto", name: "Cryptocurrency" }],
//       supported_resolutions: this.supportedResolutions,
//       supports_search: true,
//       supports_group_request: false,
//       supports_marks: false,
//       supports_timescale_marks: false,
//       supports_time: true,
//       supports_realtime: true,
//     };
//   }

//   async symbolInfo() {
//     const cached = await redis.get("udf:symbols");
//     if (cached) return this.asTable(JSON.parse(cached));
//     const symbols = await this.symbols;
//     return this.asTable(symbols);
//   }

//   async symbol(symbol) {
//     const symbols = await this.symbols;
//     const comps = symbol.split(":");
//     const s = (comps.length > 1 ? comps[1] : symbol).toUpperCase();
//     for (const symbol of symbols) {
//       if (symbol.symbol === s) return symbol;
//     }
//     throw new SymbolNotFound();
//   }

//   async search(query, type, exchange, limit) {
//     let symbols = await this.symbols;
//     if (type) symbols = symbols.filter((s) => s.type === type);
//     if (exchange) symbols = symbols.filter((s) => s.exchange === exchange);
//     query = query.toUpperCase();
//     symbols = symbols.filter((s) => s.symbol.indexOf(query) >= 0);
//     if (limit) symbols = symbols.slice(0, limit);
//     return symbols.map((s) => ({
//       symbol: s.symbol,
//       full_name: s.full_name,
//       description: s.description,
//       exchange: s.exchange,
//       ticker: s.ticker,
//       type: s.type,
//     }));
//   }

//   async history(symbol, from, to, resolution) {
//     const cacheKey = `udf:history:${symbol}:${from}:${to}:${resolution}`;
//     const cached = await redis.get(cacheKey);
//     if (cached) return JSON.parse(cached);

//     const hasSymbol = await this.checkSymbol(symbol);
//     if (!hasSymbol) throw new SymbolNotFound();

//     const interval = this.intervalMap[resolution];
//     if (!interval) throw new InvalidResolution();

//     from = Number(from) * 1000;
//     to = Number(to) * 1000;
//     let totalKlines = [];
//     let startTime = from;

//     const fetchKlines = async () => {
//       const klines = await limiter.schedule(() =>
//         this.binance.klines(symbol, interval, startTime, to, 500)
//       );
//       totalKlines = totalKlines.concat(klines);
//       if (klines.length === 500) {
//         startTime = klines[klines.length - 1][0] + 1;
//         await fetchKlines();
//       }
//     };
//     await fetchKlines();

//     if (totalKlines.length === 0) return { s: "no_data" };
//     const result = {
//       s: "ok",
//       t: totalKlines.map((b) => Math.floor(b[0] / 1000)),
//       c: totalKlines.map((b) => parseFloat(b[4])),
//       o: totalKlines.map((b) => parseFloat(b[1])),
//       h: totalKlines.map((b) => parseFloat(b[2])),
//       l: totalKlines.map((b) => parseFloat(b[3])),
//       v: totalKlines.map((b) => parseFloat(b[5])),
//     };
//     await redis.setex(cacheKey, 300, JSON.stringify(result));
//     return result;
//   }
// }

// UDF.Error = UDFError;
// UDF.SymbolNotFound = SymbolNotFound;
// UDF.InvalidResolution = InvalidResolution;

// module.exports = UDF;

// /home/ashif/chart_raiders/binance-service/scale/udf.js
const Binance = require("./binance");
const WebSocket = require("ws");
const Redis = require("ioredis");
const Bottleneck = require("bottleneck");

const redis = new Redis({
  host: "127.0.0.1",
  port: 6379,
  password: "my_master_password",
});
const redisPub = new Redis({
  host: "127.0.0.1",
  port: 6379,
  password: "my_master_password",
});
const redisSub = new Redis({
  host: "127.0.0.1",
  port: 6379,
  password: "my_master_password",
});

const limiter = new Bottleneck({ maxConcurrent: 10, minTime: 50 });

class UDFError extends Error {}
class SymbolNotFound extends UDFError {}
class InvalidResolution extends UDFError {}

class UDF {
  constructor() {
    this.binance = new Binance();
    this.supportedResolutions = [
      "1",
      "3",
      "5",
      "15",
      "30",
      "60",
      "120",
      "240",
      "360",
      "480",
      "720",
      "1D",
      "3D",
      "1W",
      "1M",
    ];

    // maps TradingView resolution -> binance interval
    this.intervalMap = {
      1: "1m",
      3: "3m",
      5: "5m",
      15: "15m",
      30: "30m",
      60: "1h",
      120: "2h",
      240: "4h",
      360: "6h",
      480: "8h",
      720: "12h",
      "1D": "1d",
      "3D": "3d",
      "1W": "1w",
      "1M": "1M",
    };

    // local maps: key = `${SYMBOL}@${INTERVAL}`, e.g. BTCUSDT@1m
    this.subscribers = new Map(); // key -> [callbacks]
    this.latestKlines = new Map(); // key -> latest bar object
    this.binanceWs = null;
    this.subscriptions = new Set(); // set of stream strings like btcusdt@kline_1m

    // reference counting channel subscribers we requested on redisSub
    this.redisChannelRefCount = new Map(); // channel -> count

    this.connectBinanceWS();
    this.setupRedisSubscriber(); // subscribe to published kline messages
    this.preloadKlines();

    setInterval(() => this.loadSymbols(), 30000);
    this.loadSymbols();
  }

  // ---------------------
  // Redis Pub/Sub handling
  // ---------------------
  setupRedisSubscriber() {
    // when any worker publishes to 'kline:<symbol@interval>' we receive it here
    redisSub.on("message", (channel, message) => {
      try {
        // channel: kline:BTCUSDT@1m
        if (!channel.startsWith("kline:")) return;
        const key = channel.slice("kline:".length); // BTCUSDT@1m
        const bar = JSON.parse(message);
        this.latestKlines.set(key, bar);
        const callbacks = this.subscribers.get(key) || [];
        if (callbacks.length > 0) {
          // notify local subscribers
          setImmediate(() => {
            for (const cb of callbacks) {
              try {
                cb(bar);
              } catch (e) {
                console.error("callback error:", e);
              }
            }
          });
        }
      } catch (err) {
        console.error("Error handling redisSub message:", err);
      }
    });

    redisSub.on("error", (err) => {
      console.error("redisSub error:", err);
    });
  }

  // publish to redis so all workers get the event
  async publishKline(key, bar) {
    try {
      await redisPub.publish(`kline:${key}`, JSON.stringify(bar));
    } catch (err) {
      console.error("publishKline error:", err);
    }
  }

  async ensureRedisChannelSubscribed(key) {
    const channel = `kline:${key}`;
    const count = this.redisChannelRefCount.get(channel) || 0;
    if (count === 0) {
      try {
        await redisSub.subscribe(channel);
      } catch (err) {
        console.error("redisSub.subscribe error:", err);
      }
    }
    this.redisChannelRefCount.set(channel, count + 1);
  }

  async ensureRedisChannelUnsubscribed(key) {
    const channel = `kline:${key}`;
    const count = this.redisChannelRefCount.get(channel) || 0;
    if (count <= 1) {
      // unsub
      try {
        await redisSub.unsubscribe(channel);
      } catch (err) {
        console.error("redisSub.unsubscribe error:", err);
      }
      this.redisChannelRefCount.delete(channel);
    } else {
      this.redisChannelRefCount.set(channel, count - 1);
    }
  }

  // ---------------------
  // Binance WebSocket
  // ---------------------
  async connectBinanceWS() {
    // don't reconnect if already connected
    if (this.binanceWs && this.binanceWs.readyState === WebSocket.OPEN) return;

    this.binanceWs = new WebSocket("wss://fstream.binance.com/ws");

    this.binanceWs.on("open", async () => {
      console.log(`Worker ${process.pid}: Binance WebSocket connected`);
      try {
        // load global list of streams from redis so this worker subscribes them at connect
        const streams = (await redis.smembers("binance:subscriptions")) || [];
        for (const stream of streams) {
          // add to local set and ask binanceWS to subscribe
          this.subscriptions.add(stream);
        }
        // subscribe in a single request (if any)
        if (this.subscriptions?.size > 0) {
          const params = Array.from(this.subscriptions);
          this.binanceWs.send(
            JSON.stringify({
              method: "SUBSCRIBE",
              params,
              id: Date.now(),
            })
          );
        }
      } catch (err) {
        console.error("Error on open handling:", err);
      }
    });

    this.binanceWs.on("close", () => {
      console.log(
        `Worker ${process.pid}: Binance WebSocket closed, reconnecting...`
      );
      this.binanceWs = null;
      setTimeout(() => this.connectBinanceWS(), 1000);
    });

    this.binanceWs.on("error", (err) => {
      console.error(`Worker ${process.pid}: Binance WebSocket error:`, err);
    });

    this.binanceWs.on("message", async (data) => {
      try {
        const msg = JSON.parse(data.toString());
        // If it's a subscription response with id
        if (msg.id && msg.result !== undefined) {
          // ignore subscribe result ok / null
          return;
        }
        // Combined streams may come with e === 'kline'
        if (msg.e === "kline" && msg.k) {
          const k = msg.k;
          const binanceInterval = k.i; // e.g. '1m'
          const key = `${k.s}@${binanceInterval}`; // e.g. BTCUSDT@1m
          const bar = {
            time: Math.floor(k.t / 1000),
            open: parseFloat(k.o),
            high: parseFloat(k.h),
            low: parseFloat(k.l),
            close: parseFloat(k.c),
            volume: parseFloat(k.v),
            isFinal: !!k.x,
          };

          // Save latest bar in redis (short TTL) and publish to workers
          try {
            await redis.set(`kline:${key}`, JSON.stringify(bar), "EX", 3600);
          } catch (err) {
            console.error("redis.set error for kline:", err);
          }

          // publish to redis channel so other workers receive it too
          await this.publishKline(key, bar);

          // Local delivery as well
          const callbacks = this.subscribers.get(key) || [];
          if (callbacks.length > 0) {
            setImmediate(() => {
              for (const cb of callbacks) {
                try {
                  cb(bar);
                } catch (e) {
                  console.error("subscriber callback error:", e);
                }
              }
            });
          }

          // If k.x (final candle) we can prepare next provisional bar
          if (k.x) {
            const nextTime = k.T + 1;
            const nextBar = {
              time: Math.floor(nextTime / 1000),
              open: parseFloat(k.c),
              high: parseFloat(k.c),
              low: parseFloat(k.c),
              close: parseFloat(k.c),
              volume: 0,
              isFinal: false,
            };
            this.latestKlines.set(key, nextBar);
            await redis.set(
              `kline:${key}`,
              JSON.stringify(nextBar),
              "EX",
              3600
            );
            await this.publishKline(key, nextBar);
          } else {
            // update latestKlines for non-final updates
            this.latestKlines.set(key, bar);
          }
        } else if (Array.isArray(msg)) {
          // combined streams may be arrays; iterate
          for (const inner of msg) {
            if (inner && inner.e === "kline") {
              // reuse same logic (slightly simplified)
              const k = inner.k;
              const key = `${k.s}@${k.i}`;
              const bar = {
                time: Math.floor(k.t / 1000),
                open: parseFloat(k.o),
                high: parseFloat(k.h),
                low: parseFloat(k.l),
                close: parseFloat(k.c),
                volume: parseFloat(k.v),
                isFinal: !!k.x,
              };
              await redis.set(`kline:${key}`, JSON.stringify(bar), "EX", 3600);
              await this.publishKline(key, bar);
              const cbs = this.subscribers.get(key) || [];
              for (const cb of cbs) cb(bar);
            }
          }
        } else {
          // ignore other events
        }
      } catch (err) {
        console.error("Error parsing Binance WS message:", err);
      }
    });
  }

  // ---------------------
  // Preload / symbol loading
  // ---------------------
  async preloadKlines() {
    const popularPairs = ["BTCUSDT"];
    const intervals = ["1m", "5m", "1h"];
    for (const symbol of popularPairs) {
      for (const interval of intervals) {
        try {
          const klines = await limiter.schedule(() =>
            this.binance.klines(
              symbol,
              interval,
              Date.now() - 24 * 3600 * 1000,
              Date.now(),
              500
            )
          );
          const key = `kline:${symbol}@${interval}`;
          await redis.set(key, JSON.stringify(klines), "EX", 3600);
        } catch (err) {
          console.error(`Preload error for ${symbol}@${interval}:`, err);
        }
      }
    }
  }

  loadSymbols() {
    function pricescale(symbol) {
      for (let filter of symbol.filters) {
        if (filter.filterType == "PRICE_FILTER") {
          return Math.round(1 / parseFloat(filter.tickSize));
        }
      }
      return 1;
    }

    const promise = limiter
      .schedule(() => this.binance.exchangeInfo())
      .catch((err) => {
        console.error(`Worker ${process.pid}: exchangeInfo error:`, err);
        setTimeout(() => this.loadSymbols(), 1000);
      });

    this.symbols = promise.then((info) => {
      const symbols = info.symbols.map((symbol) => ({
        symbol: symbol.symbol,
        ticker: symbol.symbol,
        name: symbol.symbol,
        full_name: symbol.symbol,
        description: `${symbol.baseAsset} / ${symbol.quoteAsset}`,
        exchange: "BINANCE",
        listed_exchange: "BINANCE",
        type: "crypto",
        currency_code: symbol.quoteAsset,
        session: "24x7",
        timezone: "UTC",
        minmovement: 1,
        minmov: 1,
        minmovement2: 0,
        minmov2: 0,
        pricescale: pricescale(symbol),
        supported_resolutions: this.supportedResolutions,
        has_intraday: true,
        has_daily: true,
        has_weekly_and_monthly: true,
        data_status: "streaming",
      }));
      redis.set("udf:symbols", JSON.stringify(symbols), "EX", 3600);
      return symbols;
    });

    this.allSymbols = promise.then((info) => {
      let set = new Set();
      for (const symbol of info.symbols) {
        set.add(symbol.symbol);
      }
      return set;
    });
  }

  // ---------------------
  // Subscribe / Unsubscribe logic
  // ---------------------
  // onTick receives bar objects in the shape: { time, open, high, low, close, volume, isFinal }
  async subscribeBars(symbol, resolution, onTick, ws) {
    // resolution from TradingView (e.g. '1', '5', '60', '1D', '1W' etc.)
    const interval = this.intervalMap[resolution];
    if (!interval) {
      console.error(`Worker ${process.pid}: Invalid resolution: ${resolution}`);
      throw new InvalidResolution();
    }
    const key = `${symbol.toUpperCase()}@${interval}`; // e.g. BTCUSDT@1m
    // register local callback
    if (!this.subscribers.has(key)) this.subscribers.set(key, []);
    this.subscribers.get(key).push(onTick);

    // track ws-specific callbacks for unsubscribe
    ws._callbacks = ws._callbacks || [];
    ws._callbacks.push(onTick);

    // ensure we subscribe to the redis channel so this worker gets published events
    await this.ensureRedisChannelSubscribed(key);

    // Determine stream name used by Binance WS
    const stream = `${symbol.toLowerCase()}@kline_${interval}`;

    // if too many subscriptions in this worker, fallback to HTTP polling
    if (this.subscriptions.size >= 300) {
      console.warn(
        `Worker ${process.pid}: Stream limit reached, using HTTP polling for ${key}`
      );
      const poller = setInterval(async () => {
        try {
          const klines = await limiter.schedule(() =>
            this.binance.klines(
              symbol,
              interval,
              Date.now() - 60000,
              Date.now(),
              1
            )
          );
          if (klines.length > 0) {
            const b = klines[0];
            const bar = {
              time: Math.floor(b[0] / 1000),
              open: parseFloat(b[1]),
              high: parseFloat(b[2]),
              low: parseFloat(b[3]),
              close: parseFloat(b[4]),
              volume: parseFloat(b[5]),
              isFinal: true,
            };

            await redis.set(`kline:${key}`, JSON.stringify(bar), "EX", 3600);
            await this.publishKline(key, bar);
          }
        } catch (err) {
          console.error("Polling error for", key, err);
        }
      }, 5000);

      // store poller so unsubscribe can clear it (attach to key)
      this._pollers = this._pollers || new Map();
      this._pollers.set(key, poller);

      return;
    }

    // add to global subscriptions (shared set kept in redis so new workers can pick it up on connect)
    if (!this.subscriptions.has(stream)) {
      this.subscriptions.add(stream);
      try {
        await redis.sadd("binance:subscriptions", stream);
      } catch (err) {
        console.error("redis.sadd error:", err);
      }
      // if binance ws is open, ask for subscription
      if (this.binanceWs && this.binanceWs.readyState === WebSocket.OPEN) {
        try {
          this.binanceWs.send(
            JSON.stringify({
              method: "SUBSCRIBE",
              params: [stream],
              id: Date.now(),
            })
          );
        } catch (err) {
          console.error("binanceWs.send SUBSCRIBE error:", err);
        }
      }
    }

    // deliver cached bar immediately if present

    try {
      const cached = await redis.get(`kline:${key}`);
      if (cached) {
        const cachedObj = JSON.parse(cached);
        // immediate callback
        setImmediate(() => {
          try {
            onTick(cachedObj);
          } catch (e) {
            console.error("onTick(cached) error:", e);
          }
        });
      } else {
        // try fallback to HTTP to prime latestKlines if no cached
        const klines = await limiter.schedule(() =>
          this.binance.klines(
            symbol,
            interval,
            Date.now() - 60000 * 60,
            Date.now(),
            1
          )
        );
        if (klines && klines.length > 0) {
          const b = klines[0];
          const bar = {
            time: Math.floor(b[0] / 1000),
            open: parseFloat(b[1]),
            high: parseFloat(b[2]),
            low: parseFloat(b[3]),
            close: parseFloat(b[4]),
            volume: parseFloat(b[5]),
            isFinal: true,
          };
          this.latestKlines.set(key, bar);
          await redis.set(`kline:${key}`, JSON.stringify(bar), "EX", 3600);
          setImmediate(() => {
            try {
              onTick(bar);
            } catch (e) {
              console.error("onTick(preloaded) error:", e);
            }
          });
        }
      }
    } catch (err) {
      console.error("subscribeBars cache/fetch error:", err);
    }
  }

  async unsubscribeBars(ws) {
    // if ws has no callbacks, nothing to do
    const callbacks = ws._callbacks || [];
    if (!callbacks.length) return;

    // remove each callback from all keys
    for (const [key, cbs] of Array.from(this.subscribers.entries())) {
      const remaining = cbs.filter((cb) => !callbacks.includes(cb));
      if (remaining.length === 0) {
        // fully remove local subscription
        this.subscribers.delete(key);

        // clear poller if any
        if (this._pollers && this._pollers.has(key)) {
          clearInterval(this._pollers.get(key));
          this._pollers.delete(key);
        }

        // remove stream mapping & global redis set
        const [symbol, interval] = key.split("@"); // key = BTCUSDT@1m
        const stream = `${symbol.toLowerCase()}@kline_${interval}`;
        this.subscriptions.delete(stream);
        try {
          await redis.srem("binance:subscriptions", stream);
        } catch (err) {
          console.error("redis.srem error:", err);
        }

        // inform binance ws to unsubscribe (if open)
        if (this.binanceWs && this.binanceWs.readyState === WebSocket.OPEN) {
          try {
            this.binanceWs.send(
              JSON.stringify({
                method: "UNSUBSCRIBE",
                params: [stream],
                id: Date.now(),
              })
            );
          } catch (err) {
            console.error("binanceWs.send UNSUBSCRIBE error:", err);
          }
        }

        // unsubscribe from redis channel for this worker (so other workers still receive if they have subscribers)
        await this.ensureRedisChannelUnsubscribed(key);
      } else {
        this.subscribers.set(key, remaining);
      }
    }

    // clear ws callbacks
    ws._callbacks = [];
  }

  // ---------------------
  // Symbol / metadata / helpers
  // ---------------------
  async checkSymbol(symbol) {
    const symbols = await this.allSymbols;
    return symbols.has(symbol);
  }

  asTable(items) {
    let result = {};
    for (const item of items) {
      for (const key in item) {
        if (!result[key]) result[key] = [];
        result[key].push(item[key]);
      }
    }
    for (const key in result) {
      const values = [...new Set(result[key])];
      if (values.length === 1) result[key] = values[0];
    }
    return result;
  }

  async config() {
    return {
      exchanges: [
        { value: "BINANCE", name: "Binance", desc: "Binance Exchange" },
      ],
      symbols_types: [{ value: "crypto", name: "Cryptocurrency" }],
      supported_resolutions: this.supportedResolutions,
      supports_search: true,
      supports_group_request: false,
      supports_marks: false,
      supports_timescale_marks: false,
      supports_time: true,
      supports_realtime: true,
    };
  }

  async symbolInfo() {
    const cached = await redis.get("udf:symbols");
    if (cached) return this.asTable(JSON.parse(cached));
    const symbols = await this.symbols;
    return this.asTable(symbols);
  }

  async symbol(symbol) {
    const symbols = await this.symbols;
    const comps = symbol.split(":");
    const s = (comps.length > 1 ? comps[1] : symbol).toUpperCase();
    for (const symbolEntry of symbols) {
      if (symbolEntry.symbol === s) return symbolEntry;
    }
    throw new SymbolNotFound();
  }

  async search(query, type, exchange, limit) {
    let symbols = await this.symbols;
    if (type) symbols = symbols.filter((s) => s.type === type);
    if (exchange) symbols = symbols.filter((s) => s.exchange === exchange);
    query = query.toUpperCase();
    symbols = symbols.filter((s) => s.symbol.indexOf(query) >= 0);
    if (limit) symbols = symbols.slice(0, limit);
    return symbols.map((s) => ({
      symbol: s.symbol,
      full_name: s.full_name,
      description: s.description,
      exchange: s.exchange,
      ticker: s.ticker,
      type: s.type,
    }));
  }

  async history(symbol, from, to, resolution) {
    const cacheKey = `udf:history:${symbol}:${from}:${to}:${resolution}`;
    const cached = await redis.get(cacheKey);

    if (cached) return JSON.parse(cached);

    const hasSymbol = await this.checkSymbol(symbol);
    if (!hasSymbol) throw new SymbolNotFound();

    const interval = this.intervalMap[resolution];
    if (!interval) throw new InvalidResolution();

    from = Number(from) * 1000;
    to = Number(to) * 1000;
    let totalKlines = [];
    let startTime = from;

    const fetchKlines = async () => {
      const klines = await limiter.schedule(() =>
        this.binance.klines(symbol, interval, startTime, to, 500)
      );
      totalKlines = totalKlines.concat(klines);
      if (klines.length === 500) {
        startTime = klines[klines.length - 1][0] + 1;
        await fetchKlines();
      }
    };
    await fetchKlines();

    if (totalKlines.length === 0) return { s: "no_data" };
    const result = {
      s: "ok",
      t: totalKlines.map((b) => Math.floor(b[0] / 1000)),
      c: totalKlines.map((b) => parseFloat(b[4])),
      o: totalKlines.map((b) => parseFloat(b[1])),
      h: totalKlines.map((b) => parseFloat(b[2])),
      l: totalKlines.map((b) => parseFloat(b[3])),
      v: totalKlines.map((b) => parseFloat(b[5])),
    };
    await redis.set(cacheKey, JSON.stringify(result), "EX", 300);
    return result;
  }
}

UDF.Error = UDFError;
UDF.SymbolNotFound = SymbolNotFound;
UDF.InvalidResolution = InvalidResolution;

module.exports = UDF;
