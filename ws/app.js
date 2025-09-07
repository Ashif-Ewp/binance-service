const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");
const morgan = require("morgan");
const UDF = require("./udf");
const query = require("./query");
const WebSocketHandler = require("./websocket-handler");

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use(morgan("tiny"));

const udf = new UDF();

// Initialize WebSocket Handler
const wsHandler = new WebSocketHandler(udf);

// WebSocket Server
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws, req) => {
  wsHandler.handleConnection(ws, req);
});

// Common helper function (keeping from original)
function handlePromise(res, next, promise) {
  promise
    .then((result) => {
      res.send(result);
    })
    .catch((err) => {
      next(err);
    });
}

// HTTP Endpoints (keeping original structure for fallback/compatibility)
app.all("/", (req, res) => {
  res
    .set("Content-Type", "text/plain")
    .send(
      "WebSocket UDF Adapter for TradingView. Connect via WebSocket to ws://your-domain:port or use HTTP endpoints."
    );
});

app.get("/time", (req, res) => {
  const time = Math.floor(Date.now() / 1000); // In seconds
  res.set("Content-Type", "text/plain").send(time.toString());
});

app.get("/config", (req, res, next) => {
  handlePromise(res, next, udf.config());
});

app.get("/symbol_info", (req, res, next) => {
  handlePromise(res, next, udf.symbolInfo());
});

app.get("/symbols", [query.symbol], (req, res, next) => {
  handlePromise(res, next, udf.symbol(req.query.symbol));
});

app.get("/search", [query.query, query.limit], (req, res, next) => {
  if (req.query.type === "") {
    req.query.type = null;
  }
  if (req.query.exchange === "") {
    req.query.exchange = null;
  }

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
  (req, res, next) => {
    handlePromise(
      res,
      next,
      udf.history(
        req.query.symbol,
        req.query.from,
        req.query.to,
        req.query.resolution
      )
    );
  }
);

// Handle errors (keeping original structure)
app.use((err, req, res, next) => {
  if (err instanceof query.Error) {
    return res.status(err.status).send({
      s: "error",
      errmsg: err.message,
    });
  }

  if (err instanceof UDF.SymbolNotFound) {
    return res.status(404).send({
      s: "error",
      errmsg: "Symbol Not Found",
    });
  }
  if (err instanceof UDF.InvalidResolution) {
    return res.status(400).send({
      s: "error",
      errmsg: "Invalid Resolution",
    });
  }

  console.error(err);
  res.status(500).send({
    s: "error",
    errmsg: "Internal Error",
  });
});

// Listen
const port = process.env.PORT || 8081;

server.listen(port, () => {
  console.log(`WebSocket UDF Server listening on port ${port}`);
  console.log(`HTTP endpoints: http://localhost:${port}`);
  console.log(`WebSocket endpoint: ws://localhost:${port}`);
});

// Export WebSocket handler for external access to broadcast functions
module.exports = { server, wss, wsHandler };
