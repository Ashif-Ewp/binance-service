const Binance = require("./binance");

class UDFError extends Error {}
class SymbolNotFound extends UDFError {}
class InvalidResolution extends UDFError {}

class UDF {
  constructor(wsHandler = null) {
    this.binance = new Binance();
    this.wsHandler = wsHandler; // WebSocket handler for real-time updates
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

    // Load symbols periodically
    setInterval(() => {
      this.loadSymbols();
    }, 30000);
    this.loadSymbols();

    // Initialize real-time data streaming if WebSocket handler is available
    if (this.wsHandler) {
      this.initializeRealTimeStreaming();
    }
  }

  /**
   * Set WebSocket handler for real-time updates
   * @param {WebSocketHandler} wsHandler - WebSocket handler instance
   */
  setWebSocketHandler(wsHandler) {
    this.wsHandler = wsHandler;
    this.initializeRealTimeStreaming();
  }

  /**
   * Initialize real-time data streaming
   */
  initializeRealTimeStreaming() {
    // This can be enhanced to connect to Binance WebSocket streams
    // For now, we'll simulate with periodic updates
    console.log("Real-time streaming initialized");

    // Example: Connect to Binance WebSocket for real-time price updates
    // You can implement this based on your needs
    this.setupBinanceWebSocketStream();
  }

  /**
   * Setup Binance WebSocket stream for real-time data
   */
  setupBinanceWebSocketStream() {
    // This is a placeholder for Binance WebSocket integration
    // You can implement actual Binance WebSocket connection here

    // Example implementation:
    /*
    const WebSocket = require('ws');
    const binanceWs = new WebSocket('wss://fstream.binance.com/ws/btcusdt@kline_1m');
    
    binanceWs.on('message', (data) => {
      const parsed = JSON.parse(data);
      if (parsed.k && this.wsHandler) {
        const bar = {
          time: Math.floor(parsed.k.t / 1000),
          open: parseFloat(parsed.k.o),
          high: parseFloat(parsed.k.h),
          low: parseFloat(parsed.k.l),
          close: parseFloat(parsed.k.c),
          volume: parseFloat(parsed.k.v)
        };
        
        this.wsHandler.broadcastUpdate(parsed.k.s, bar);
      }
    });
    */

    console.log("Binance WebSocket stream setup (placeholder)");
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

    const promise = this.binance.exchangeInfo().catch((err) => {
      console.error("Error loading symbols:", err);
      setTimeout(() => {
        this.loadSymbols();
      }, 1000);
    });

    this.symbols = promise.then((info) => {
      if (!info || !info.symbols) {
        console.error("Invalid exchange info received");
        return [];
      }

      return info.symbols.map((symbol) => {
        return {
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
        };
      });
    });

    this.allSymbols = promise.then((info) => {
      if (!info || !info.symbols) {
        return new Set();
      }

      let set = new Set();
      for (const symbol of info.symbols) {
        set.add(symbol.symbol);
      }
      return set;
    });
  }

  async checkSymbol(symbol) {
    try {
      const symbols = await this.allSymbols;
      return symbols.has(symbol);
    } catch (error) {
      console.error("Error checking symbol:", error);
      return false;
    }
  }

  /**
   * Convert items to response-as-a-table format.
   * @param {array} items - Items to convert.
   * @returns {object} Response-as-a-table formatted items.
   */
  asTable(items) {
    let result = {};

    if (!Array.isArray(items) || items.length === 0) {
      return result;
    }

    for (const item of items) {
      for (const key in item) {
        if (!result[key]) {
          result[key] = [];
        }
        result[key].push(item[key]);
      }
    }

    for (const key in result) {
      const values = [...new Set(result[key])];
      if (values.length === 1) {
        result[key] = values[0];
      }
    }

    return result;
  }

  /**
   * Data feed configuration data.
   */
  async config() {
    return {
      exchanges: [
        {
          value: "BINANCE",
          name: "Binance",
          desc: "Binance Exchange",
        },
      ],
      symbols_types: [
        {
          value: "crypto",
          name: "Cryptocurrency",
        },
      ],
      supported_resolutions: this.supportedResolutions,
      supports_search: true,
      supports_group_request: false,
      supports_marks: false,
      supports_timescale_marks: false,
      supports_time: true,
      // WebSocket specific configurations
      supports_streaming: this.wsHandler ? true : false,
      streaming_url: this.wsHandler ? "ws://your-domain:port" : null,
    };
  }

  /**
   * Symbols.
   * @returns {object} Response-as-a-table formatted symbols.
   */
  async symbolInfo() {
    try {
      const symbols = await this.symbols;
      return this.asTable(symbols);
    } catch (error) {
      console.error("Error getting symbol info:", error);
      return {};
    }
  }

  /**
   * Symbol resolve.
   * @param {string} symbol Symbol name or ticker.
   * @returns {object} Symbol.
   */
  async symbol(symbol) {
    try {
      const symbols = await this.symbols;

      const comps = symbol.split(":");
      const s = (comps.length > 1 ? comps[1] : symbol).toUpperCase();

      for (const symbolData of symbols) {
        if (symbolData.symbol === s) {
          return symbolData;
        }
      }

      throw new SymbolNotFound(`Symbol ${s} not found`);
    } catch (error) {
      if (error instanceof SymbolNotFound) {
        throw error;
      }
      console.error("Error resolving symbol:", error);
      throw new SymbolNotFound(`Error resolving symbol ${symbol}`);
    }
  }

  /**
   * Symbol search.
   * @param {string} query Text typed by the user in the Symbol Search edit box.
   * @param {string} type One of the symbol types supported by back-end.
   * @param {string} exchange One of the exchanges supported by back-end.
   * @param {number} limit The maximum number of symbols in a response.
   * @returns {array} Array of symbols.
   */
  async search(query, type, exchange, limit) {
    try {
      let symbols = await this.symbols;

      if (type) {
        symbols = symbols.filter((s) => s.type === type);
      }
      if (exchange) {
        symbols = symbols.filter((s) => s.exchange === exchange);
      }

      query = query.toUpperCase();
      symbols = symbols.filter((s) => s.symbol.indexOf(query) >= 0);

      if (limit && limit > 0) {
        symbols = symbols.slice(0, limit);
      }

      return symbols.map((s) => ({
        symbol: s.symbol,
        full_name: s.full_name,
        description: s.description,
        exchange: s.exchange,
        ticker: s.ticker,
        type: s.type,
      }));
    } catch (error) {
      console.error("Error searching symbols:", error);
      return [];
    }
  }

  /**
   * Bars.
   * @param {string} symbol - Symbol name or ticker.
   * @param {number} from - Unix timestamp (UTC) of leftmost required bar.
   * @param {number} to - Unix timestamp (UTC) of rightmost required bar.
   * @param {string} resolution - Bar resolution.
   */
  async history(symbol, from, to, resolution) {
    try {
      const hasSymbol = await this.checkSymbol(symbol);
      if (!hasSymbol) {
        throw new SymbolNotFound(`Symbol ${symbol} not found`);
      }

      const RESOLUTIONS_INTERVALS_MAP = {
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
        D: "1d",
        "1D": "1d",
        "3D": "3d",
        W: "1w",
        "1W": "1w",
        M: "1M",
        "1M": "1M",
      };

      const interval = RESOLUTIONS_INTERVALS_MAP[resolution];
      if (!interval) {
        throw new InvalidResolution(
          `Resolution ${resolution} is not supported`
        );
      }

      let totalKlines = [];

      from = Number(from) * 1000;
      to = Number(to) * 1000;

      let tt = parseInt(to);
      if (tt !== 0) {
        to = tt;
        if (from > to) {
          from = to - 10000000;
        }
      }

      while (true) {
        try {
          const klines = await this.binance.klines(
            symbol,
            interval,
            from,
            to,
            500
          );

          if (!klines || !Array.isArray(klines)) {
            console.error("Invalid klines response:", klines);
            break;
          }

          totalKlines = totalKlines.concat(klines);

          if (klines.length === 500) {
            from = klines[klines.length - 1][0] + 1;
          } else {
            break;
          }
        } catch (error) {
          console.error("Error fetching klines:", error);
          break;
        }
      }

      if (totalKlines.length === 0) {
        return { s: "no_data" };
      }

      return {
        s: "ok",
        t: totalKlines.map((b) => Math.floor(b[0] / 1000)),
        c: totalKlines.map((b) => parseFloat(b[4])),
        o: totalKlines.map((b) => parseFloat(b[1])),
        h: totalKlines.map((b) => parseFloat(b[2])),
        l: totalKlines.map((b) => parseFloat(b[3])),
        v: totalKlines.map((b) => parseFloat(b[5])),
      };
    } catch (error) {
      if (
        error instanceof SymbolNotFound ||
        error instanceof InvalidResolution
      ) {
        throw error;
      }
      console.error("Error getting history:", error);
      return { s: "error", errmsg: error.message };
    }
  }

  /**
   * Get statistics about the UDF instance
   */
  getStats() {
    return {
      supportedResolutions: this.supportedResolutions,
      hasWebSocketHandler: !!this.wsHandler,
      wsStats: this.wsHandler ? this.wsHandler.getStats() : null,
    };
  }
}

UDF.Error = UDFError;
UDF.SymbolNotFound = SymbolNotFound;
UDF.InvalidResolution = InvalidResolution;

module.exports = UDF;
