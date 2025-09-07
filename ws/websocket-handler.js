const WebSocket = require("ws");
const UDF = require("./udf");

/**
 * WebSocket connection and message handler for UDF protocol
 */
class WebSocketHandler {
  constructor(udf) {
    this.udf = udf;
    this.connections = new Map();
    this.subscriptions = new Map(); // symbol -> Set of connection IDs
  }

  /**
   * Handle new WebSocket connection
   * @param {WebSocket} ws - WebSocket connection
   * @param {object} req - HTTP request object
   */
  handleConnection(ws, req) {
    const connectionId = this.generateConnectionId();

    this.connections.set(connectionId, {
      ws,
      subscriptions: new Set(),
      lastPing: Date.now(),
      remoteAddress: req.connection.remoteAddress,
    });

    console.log(
      `WebSocket connected: ${connectionId} from ${req.connection.remoteAddress}`
    );

    // Set up event handlers
    ws.on("message", async (data) => {
      try {
        const message = JSON.parse(data);
        await this.handleMessage(connectionId, message);
      } catch (error) {
        console.error("WebSocket message error:", error);
        this.sendError(ws, "Invalid message format", error.message);
      }
    });

    ws.on("close", () => {
      console.log(`WebSocket disconnected: ${connectionId}`);
      this.cleanupConnection(connectionId);
    });

    ws.on("error", (error) => {
      console.error(`WebSocket error for ${connectionId}:`, error);
      this.cleanupConnection(connectionId);
    });

    ws.on("pong", () => {
      if (this.connections.has(connectionId)) {
        this.connections.get(connectionId).lastPing = Date.now();
      }
    });

    // Send connection acknowledgment
    this.sendMessage(ws, {
      type: "connection_ack",
      connectionId,
      timestamp: Date.now(),
    });

    // Setup ping/pong for connection health
    this.setupConnectionHealth(connectionId);
  }

  /**
   * Generate unique connection ID
   * @returns {string} Connection ID
   */
  generateConnectionId() {
    return Math.random().toString(36).substring(2) + Date.now().toString(36);
  }

  /**
   * Setup connection health monitoring
   * @param {string} connectionId - Connection ID
   */
  setupConnectionHealth(connectionId) {
    const pingInterval = setInterval(() => {
      if (this.connections.has(connectionId)) {
        const conn = this.connections.get(connectionId);
        const now = Date.now();

        if (now - conn.lastPing > 60000) {
          // 60 seconds timeout
          console.log(`Connection ${connectionId} timed out`);
          conn.ws.terminate();
          this.cleanupConnection(connectionId);
          clearInterval(pingInterval);
        } else if (conn.ws.readyState === WebSocket.OPEN) {
          conn.ws.ping();
        }
      } else {
        clearInterval(pingInterval);
      }
    }, 30000); // Ping every 30 seconds

    // Store interval reference for cleanup
    if (this.connections.has(connectionId)) {
      this.connections.get(connectionId).pingInterval = pingInterval;
    }
  }

  /**
   * Clean up connection and its subscriptions
   * @param {string} connectionId - Connection ID
   */
  cleanupConnection(connectionId) {
    const connection = this.connections.get(connectionId);
    if (connection) {
      // Clear ping interval
      if (connection.pingInterval) {
        clearInterval(connection.pingInterval);
      }

      // Remove all subscriptions for this connection
      connection.subscriptions.forEach((symbol) => {
        if (this.subscriptions.has(symbol)) {
          this.subscriptions.get(symbol).delete(connectionId);
          if (this.subscriptions.get(symbol).size === 0) {
            this.subscriptions.delete(symbol);
          }
        }
      });

      this.connections.delete(connectionId);
    }
  }

  /**
   * Handle WebSocket messages
   * @param {string} connectionId - Connection ID
   * @param {object} message - Parsed message object
   */
  async handleMessage(connectionId, message) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    const { ws } = connection;
    const { type, id, ...params } = message;

    try {
      switch (type) {
        case "config":
          await this.handleConfig(ws, id);
          break;

        case "symbol_info":
          await this.handleSymbolInfo(ws, id);
          break;

        case "resolve_symbol":
          await this.handleResolveSymbol(ws, id, params.symbol);
          break;

        case "search":
          await this.handleSearch(ws, id, params);
          break;

        case "history":
          await this.handleHistory(ws, id, params);
          break;

        case "subscribe":
          await this.handleSubscribe(connectionId, ws, id, params);
          break;

        case "unsubscribe":
          await this.handleUnsubscribe(connectionId, ws, id, params);
          break;

        case "ping":
          this.handlePing(connectionId, ws, id);
          break;

        default:
          this.sendError(ws, "Unknown message type", `Unknown type: ${type}`);
      }
    } catch (error) {
      console.error(`Error handling ${type}:`, error);
      this.sendErrorResponse(ws, id, error);
    }
  }

  /**
   * Handle config request
   */
  async handleConfig(ws, id) {
    const config = await this.udf.config();
    this.sendMessage(ws, {
      type: "config_response",
      id,
      data: config,
    });
  }

  /**
   * Handle symbol info request
   */
  async handleSymbolInfo(ws, id) {
    const symbolInfo = await this.udf.symbolInfo();
    this.sendMessage(ws, {
      type: "symbol_info_response",
      id,
      data: symbolInfo,
    });
  }

  /**
   * Handle resolve symbol request
   */
  async handleResolveSymbol(ws, id, symbol) {
    const resolvedSymbol = await this.udf.symbol(symbol);
    this.sendMessage(ws, {
      type: "resolve_symbol_response",
      id,
      data: resolvedSymbol,
    });
  }

  /**
   * Handle search request
   */
  async handleSearch(ws, id, params) {
    const { query, type, exchange, limit } = params;
    const searchResults = await this.udf.search(query, type, exchange, limit);
    this.sendMessage(ws, {
      type: "search_response",
      id,
      data: searchResults,
    });
  }

  /**
   * Handle history request
   */
  async handleHistory(ws, id, params) {
    const { symbol, from, to, resolution } = params;
    const history = await this.udf.history(symbol, from, to, resolution);
    this.sendMessage(ws, {
      type: "history_response",
      id,
      data: history,
    });
  }

  /**
   * Handle subscribe request
   */
  async handleSubscribe(connectionId, ws, id, params) {
    const { symbol, resolution } = params;
    const connection = this.connections.get(connectionId);

    // Add to connection's subscriptions
    connection.subscriptions.add(symbol);

    // Add to global subscriptions
    if (!this.subscriptions.has(symbol)) {
      this.subscriptions.set(symbol, new Set());
    }
    this.subscriptions.get(symbol).add(connectionId);

    this.sendMessage(ws, {
      type: "subscribe_response",
      id,
      symbol,
      resolution,
      status: "ok",
    });
  }

  /**
   * Handle unsubscribe request
   */
  async handleUnsubscribe(connectionId, ws, id, params) {
    const { symbol } = params;
    const connection = this.connections.get(connectionId);

    // Remove from connection's subscriptions
    connection.subscriptions.delete(symbol);

    // Remove from global subscriptions
    if (this.subscriptions.has(symbol)) {
      this.subscriptions.get(symbol).delete(connectionId);
      if (this.subscriptions.get(symbol).size === 0) {
        this.subscriptions.delete(symbol);
      }
    }

    this.sendMessage(ws, {
      type: "unsubscribe_response",
      id,
      symbol,
      status: "ok",
    });
  }

  /**
   * Handle ping request
   */
  handlePing(connectionId, ws, id) {
    const connection = this.connections.get(connectionId);
    if (connection) {
      connection.lastPing = Date.now();
    }

    this.sendMessage(ws, {
      type: "pong",
      id,
      timestamp: Date.now(),
    });
  }

  /**
   * Send message to WebSocket
   */
  sendMessage(ws, message) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  /**
   * Send error message
   */
  sendError(ws, message, details) {
    this.sendMessage(ws, {
      type: "error",
      message,
      details,
      timestamp: Date.now(),
    });
  }

  /**
   * Send error response for a specific request
   */
  sendErrorResponse(ws, id, error) {
    let errorMessage = "Internal server error";
    let errorCode = "INTERNAL_ERROR";

    if (error instanceof UDF.SymbolNotFound) {
      errorMessage = "Symbol not found";
      errorCode = "SYMBOL_NOT_FOUND";
    } else if (error instanceof UDF.InvalidResolution) {
      errorMessage = "Invalid resolution";
      errorCode = "INVALID_RESOLUTION";
    }

    this.sendMessage(ws, {
      type: "error_response",
      id,
      error: {
        code: errorCode,
        message: errorMessage,
        details: error.message,
      },
    });
  }

  /**
   * Broadcast real-time updates to subscribed connections
   * @param {string} symbol - Symbol name
   * @param {object} bar - Bar data
   */
  broadcastUpdate(symbol, bar) {
    if (this.subscriptions.has(symbol)) {
      const message = JSON.stringify({
        type: "bar_update",
        symbol,
        bar,
        timestamp: Date.now(),
      });

      this.subscriptions.get(symbol).forEach((connectionId) => {
        const connection = this.connections.get(connectionId);
        if (connection && connection.ws.readyState === WebSocket.OPEN) {
          connection.ws.send(message);
        }
      });
    }
  }

  /**
   * Get connection statistics
   */
  getStats() {
    return {
      totalConnections: this.connections.size,
      totalSubscriptions: Array.from(this.subscriptions.values()).reduce(
        (sum, set) => sum + set.size,
        0
      ),
      symbolsSubscribed: this.subscriptions.size,
      connections: Array.from(this.connections.entries()).map(([id, conn]) => ({
        id,
        remoteAddress: conn.remoteAddress,
        subscriptions: conn.subscriptions.size,
        lastPing: conn.lastPing,
      })),
    };
  }
}

module.exports = WebSocketHandler;
