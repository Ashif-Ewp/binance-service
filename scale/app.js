// // /home/ashif/chart_raiders/binance-service/scale/app.js
// const cluster = require("cluster");
// const numCPUs = require("os").cpus().length;
// const express = require("express");
// const WebSocket = require("ws");
// const Redis = require("ioredis");
// const cors = require("cors");
// const morgan = require("morgan");
// const UDF = require("./udf");
// const query = require("./query");

// if (cluster.isMaster) {
//   console.log(`Master ${process.pid} is running`);
//   for (let i = 0; i < numCPUs; i++) {
//     cluster.fork();
//   }
//   cluster.on("exit", (worker) => {
//     console.log(`Worker ${worker.process.pid} died, restarting...`);
//     cluster.fork();
//   });
// } else {
//   const app = express();
//   const redis = new Redis({
//     host: "127.0.0.1" || "localhost",
//     port: 6379,
//     password: "my_master_password",
//   });
//   redis.on("connect", () => {
//     console.log("✅ Redis connected");
//   });

//   redis.on("error", (err) => {
//     console.error("❌ Redis error:", err);
//   });

//   app.use(cors());
//   app.use(morgan("tiny"));

//   const udf = new UDF();

//   function handlePromise(res, next, promise) {
//     promise
//       .then((result) => {
//         res.send(result);
//       })
//       .catch((err) => {
//         next(err);
//       });
//   }

//   // Endpoints
//   app.all("/", (req, res) => {
//     res
//       .set("Content-Type", "text/plain")
//       .send("Welcome to the Binance UDF Adapter for TradingView.");
//   });

//   app.get("/time", (req, res) => {
//     const time = Math.floor(Date.now() / 1000);
//     res.set("Content-Type", "text/plain").send(time.toString());
//   });

//   app.get("/config", async (req, res, next) => {
//     const cacheKey = "udf:config";
//     const cached = await redis.get(cacheKey);
//     if (cached) return res.send(JSON.parse(cached));
//     handlePromise(
//       res,
//       next,
//       udf.config().then((result) => {
//         redis.setex(cacheKey, 3600, JSON.stringify(result));
//         return result;
//       })
//     );
//   });

//   app.get("/symbol_info", async (req, res, next) => {
//     const cacheKey = "udf:symbol_info";
//     const cached = await redis.get(cacheKey);
//     if (cached) return res.send(JSON.parse(cached));
//     handlePromise(
//       res,
//       next,
//       udf.symbolInfo().then((result) => {
//         redis.setex(cacheKey, 3600, JSON.stringify(result));
//         return result;
//       })
//     );
//   });

//   app.get("/symbols", [query.symbol], (req, res, next) => {
//     handlePromise(res, next, udf.symbol(req.query.symbol));
//   });

//   app.get("/search", [query.query, query.limit], (req, res, next) => {
//     if (req.query.type === "") req.query.type = null;
//     if (req.query.exchange === "") req.query.exchange = null;
//     handlePromise(
//       res,
//       next,
//       udf.search(
//         req.query.query,
//         req.query.type,
//         req.query.exchange,
//         req.query.limit
//       )
//     );
//   });

//   app.get(
//     "/history",
//     [query.symbol, query.from, query.to, query.resolution],
//     async (req, res, next) => {
//       const cacheKey = `udf:history:${req.query.symbol}:${req.query.from}:${req.query.to}:${req.query.resolution}`;
//       const cached = await redis.get(cacheKey);
//       console.log({ cached });
//       if (cached) return res.send(JSON.parse(cached));
//       handlePromise(
//         res,
//         next,
//         udf
//           .history(
//             req.query.symbol,
//             req.query.from,
//             req.query.to,
//             req.query.resolution
//           )
//           .then(async (result) => {
//             await redis.setex(cacheKey, 60, JSON.stringify(result));
//             return result;
//           })
//       );
//     }
//   );

//   // Error handling
//   app.use((err, req, res, next) => {
//     if (err instanceof query.Error) {
//       return res.status(err.status).send({ s: "error", errmsg: err.message });
//     }
//     if (err instanceof UDF.SymbolNotFound) {
//       return res.status(404).send({ s: "error", errmsg: "Symbol Not Found" });
//     }
//     if (err instanceof UDF.InvalidResolution) {
//       return res.status(400).send({ s: "error", errmsg: "Invalid Resolution" });
//     }
//     console.error(err);
//     res.status(500).send({ s: "error", errmsg: "Internal Error" });
//   });
//   const PORT = process.env.PORT || 8081;
//   const server = app.listen(PORT, () => {
//     console.log(`Worker ${process.pid} listening on port ${PORT}`);
//   });

//   const wss = new WebSocket.Server({ server });
//   wss.on("connection", (ws) => {
//     console.log(`Worker ${process.pid}: New WebSocket client connected`);
//     ws.on("message", async (message) => {
//       console.log({ message });
//       try {
//         const data = JSON.parse(message.toString());
//         console.log({ data });
//         if (data.type === "subscribeBars") {
//           const { symbol, resolution } = data;
//           udf.subscribeBars(
//             symbol,
//             resolution,
//             (bar) => {
//               if (ws.readyState === WebSocket.OPEN) {
//                 console.log({ data });
//                 ws.send(JSON.stringify({ type: "bar", data: bar }));
//               }
//             },
//             ws
//           );
//         }
//       } catch (err) {
//         console.error(`Worker ${process.pid}: WebSocket message error:`, err);
//       }
//     });
//     ws.on("close", () => {
//       console.log(`Worker ${process.pid}: WebSocket client disconnected`);
//       udf.unsubscribeBars(ws);
//     });
//     ws.on("error", (err) => {
//       console.error(`Worker ${process.pid}: WebSocket client error:`, err);
//     });
//   });
// }

// /home/ashif/chart_raiders/binance-service/scale/app.js
const cluster = require("cluster");
const numCPUs = require("os").cpus().length;
const express = require("express");
const WebSocket = require("ws");
const Redis = require("ioredis");
const cors = require("cors");
const morgan = require("morgan");
const UDF = require("./udf");
const query = require("./query");
const redisConfig = require("./config/redisConfig");
const redisV1 = redisConfig.getInstance();

if (cluster.isMaster) {
  console.log(`👑 Master ${process.pid} is running`);

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on("exit", (worker) => {
    console.log(`⚠️ Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
} else {
  const app = express();

  // ✅ Redis client
  const redis = new Redis({
    host: "127.0.0.1",
    port: 6379,
    password: "my_master_password",
  });

  redis.on("connect", () => console.log("✅ Redis connected"));
  redis.on("error", (err) => console.error("❌ Redis error:", err));

  app.use(cors());
  app.use(morgan("tiny"));

  const udf = new UDF();

  function handlePromise(res, next, promise) {
    promise.then((result) => res.send(result)).catch((err) => next(err));
  }

  // -----------------
  // REST Endpoints
  // -----------------
  app.all("/", (req, res) => {
    res
      .set("Content-Type", "text/plain")
      .send("Welcome to the Binance UDF Adapter for TradingView.");
  });

  app.get("/time", (req, res) => {
    res
      .set("Content-Type", "text/plain")
      .send(Math.floor(Date.now() / 1000).toString());
  });

  app.get("/config", async (req, res, next) => {
    const cacheKey = "udf:config";
    const cached = await redis.get(cacheKey);
    if (cached) return res.send(JSON.parse(cached));

    handlePromise(
      res,
      next,
      udf.config().then((result) => {
        redis.set(cacheKey, JSON.stringify(result), "EX", 3600);
        return result;
      })
    );
  });

  app.get("/symbol_info", async (req, res, next) => {
    const cacheKey = "udf:symbol_info";
    const cached = await redis.get(cacheKey);
    if (cached) return res.send(JSON.parse(cached));

    handlePromise(
      res,
      next,
      udf.symbolInfo().then((result) => {
        redis.set(cacheKey, JSON.stringify(result), "EX", 3600);
        return result;
      })
    );
  });

  app.get("/symbols", [query.symbol], (req, res, next) => {
    handlePromise(res, next, udf.symbol(req.query.symbol));
  });

  app.get("/search", [query.query, query.limit], (req, res, next) => {
    if (req.query.type === "") req.query.type = null;
    if (req.query.exchange === "") req.query.exchange = null;

    handlePromise(
      res,
      next,
      udf.search(
        req.query.query,
        req.query.type,
        req.query.exchange,
        req.query.limit
      )
    );
  });

  app.get(
    "/history",
    [query.symbol, query.from, query.to, query.resolution],
    async (req, res, next) => {
      const cacheKey = `udf:history:${req.query.symbol}:${req.query.from}:${req.query.to}:${req.query.resolution}`;
      const cached = await redis.get(cacheKey);

      if (cached) {
        console.log(`📦 Cache hit for ${cacheKey}`);
        return res.send(JSON.parse(cached));
      }

      handlePromise(
        res,
        next,
        udf
          .history(
            req.query.symbol,
            req.query.from,
            req.query.to,
            req.query.resolution
          )
          .then(async (result) => {
            await redis.set(cacheKey, JSON.stringify(result), "EX", 60);
            return result;
          })
      );
    }
  );

  // -----------------
  // Error Handling
  // -----------------
  app.use((err, req, res, next) => {
    if (err instanceof query.Error) {
      return res.status(err.status).send({ s: "error", errmsg: err.message });
    }
    if (err instanceof UDF.SymbolNotFound) {
      return res.status(404).send({ s: "error", errmsg: "Symbol Not Found" });
    }
    if (err instanceof UDF.InvalidResolution) {
      return res.status(400).send({ s: "error", errmsg: "Invalid Resolution" });
    }
    console.error("🔥 Internal error:", err);
    res.status(500).send({ s: "error", errmsg: "Internal Error" });
  });

  // -----------------
  // Server + WS Setup
  // -----------------
  const PORT = process.env.PORT || 8081;
  const server = app.listen(PORT, () => {
    console.log(`🚀 Worker ${process.pid} listening on port ${PORT}`);
  });

  const wss = new WebSocket.Server({ server });
  const symbolState = {};

  wss.on("connection", (ws) => {
    console.log(`🔌 Worker ${process.pid}: WebSocket client connected`);

    ws.on("message", async (message) => {
      try {
        const data = JSON.parse(message.toString());
        console.log(`📩 WS Message:`, data);

        if (data?.type === "subscribeBars") {
          const { symbol, resolution } = data;
          // ✅ Forward proper ms timestamps
          udf.subscribeBars(
            symbol,
            resolution,
            async (bar) => {
              if (ws.readyState === WebSocket.OPEN) {
                const now = Date.now();
                if (!symbolState[symbol]) {
                  symbolState[symbol] = {
                    lastRedisUpdate: 0,
                    lastSavedPrice: null,
                  };
                }
                const state = symbolState[symbol];
                if (
                  now - state.lastRedisUpdate > 1000 ||
                  bar.close !== state.lastSavedPrice
                ) {
                  redisV1
                    .set(`price:${symbol}`, bar.close)
                    .catch((err) => console.error("Redis set error:", err));

                  state.lastSavedPrice = bar.close;
                  state.lastRedisUpdate = now;
                }

                // ws.send(
                //   JSON.stringify({
                //     type: "bar",
                //     data: {
                //       time: bar.time * 1000, // ✅ ensure milliseconds
                //       open: bar.open,
                //       high: bar.high,
                //       low: bar.low,
                //       close: bar.close,
                //       volume: bar.volume,
                //     },
                //   })
                // );

                ws.send(
                  JSON.stringify({
                    type: "bar",
                    symbol, // ✅ NEW: echo back the subscription symbol
                    resolution, // ✅ NEW: echo back the subscription resolution (e.g. "1","5","60","1D")
                    data: {
                      time: bar.time * 1000, // ✅ ms for TradingView realtime callback
                      open: bar.open,
                      high: bar.high,
                      low: bar.low,
                      close: bar.close,
                      volume: bar.volume,
                    },
                  })
                );
              }
            },
            ws
          );
        }

        if (data.type === "unsubscribeBars") {
          udf.unsubscribeBars(ws);
        }
      } catch (err) {
        console.error(`❌ WS message error in worker ${process.pid}:`, err);
      }
    });

    ws.on("close", () => {
      console.log(`❎ Worker ${process.pid}: WebSocket client disconnected`);
      udf.unsubscribeBars(ws);
    });

    ws.on("error", (err) => {
      console.error(`⚠️ Worker ${process.pid}: WebSocket client error:`, err);
    });
  });
}
