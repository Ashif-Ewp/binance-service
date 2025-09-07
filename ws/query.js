/**
 * Enhanced query parameter validation and sanitization with WebSocket support.
 */

class QueryError extends Error {
  constructor(status, message, details = null) {
    super(message);
    this.status = status;
    this.details = details;
    this.name = "QueryError";
  }

  static missing(param) {
    return new QueryError(400, `Missing mandatory parameter '${param}'`, {
      param,
    });
  }

  static empty(param) {
    return new QueryError(400, `Empty parameter '${param}'`, { param });
  }

  static malformed(param, expected = null) {
    const message = expected
      ? `Malformed parameter '${param}'. Expected: ${expected}`
      : `Malformed parameter '${param}'`;
    return new QueryError(400, message, { param, expected });
  }

  static outOfRange(param, min, max, value) {
    return new QueryError(
      400,
      `Parameter '${param}' out of range. Expected: ${min}-${max}, got: ${value}`,
      { param, min, max, value }
    );
  }

  static invalidValue(param, allowedValues, value) {
    return new QueryError(
      400,
      `Invalid value for parameter '${param}'. Allowed: [${allowedValues.join(
        ", "
      )}], got: ${value}`,
      { param, allowedValues, value }
    );
  }
}

/**
 * Validate mandatory parameter
 * @param {object} req - Request object (HTTP) or message object (WebSocket)
 * @param {string} param - Parameter name
 * @param {RegExp} regexp - Optional validation regex
 * @param {object} options - Additional validation options
 */
function mandatory(req, param, regexp = null, options = {}) {
  const source = req.query || req; // Support both HTTP and WebSocket

  if (source[param] === undefined || source[param] === null) {
    throw QueryError.missing(param);
  }

  if (source[param] === "") {
    throw QueryError.empty(param);
  }

  if (regexp && !regexp.test(source[param])) {
    throw QueryError.malformed(param, options.expected || regexp.toString());
  }

  // Additional validations
  if (options.minLength && source[param].length < options.minLength) {
    throw QueryError.malformed(param, `minimum length ${options.minLength}`);
  }

  if (options.maxLength && source[param].length > options.maxLength) {
    throw QueryError.malformed(param, `maximum length ${options.maxLength}`);
  }

  if (options.allowedValues && !options.allowedValues.includes(source[param])) {
    throw QueryError.invalidValue(param, options.allowedValues, source[param]);
  }
}

/**
 * Validate optional parameter
 * @param {object} req - Request object (HTTP) or message object (WebSocket)
 * @param {string} param - Parameter name
 * @param {RegExp} regexp - Optional validation regex
 * @param {object} options - Additional validation options
 */
function optional(req, param, regexp = null, options = {}) {
  const source = req.query || req; // Support both HTTP and WebSocket

  if (source[param] === undefined || source[param] === null) {
    return;
  }

  if (source[param] === "") {
    source[param] = null;
    return;
  }

  if (regexp && !regexp.test(source[param])) {
    throw QueryError.malformed(param, options.expected || regexp.toString());
  }

  // Additional validations
  if (options.minLength && source[param].length < options.minLength) {
    throw QueryError.malformed(param, `minimum length ${options.minLength}`);
  }

  if (options.maxLength && source[param].length > options.maxLength) {
    throw QueryError.malformed(param, `maximum length ${options.maxLength}`);
  }

  if (options.allowedValues && !options.allowedValues.includes(source[param])) {
    throw QueryError.invalidValue(param, options.allowedValues, source[param]);
  }
}

/**
 * Validate optional integer parameter
 * @param {object} req - Request object (HTTP) or message object (WebSocket)
 * @param {string} param - Parameter name
 * @param {object} options - Validation options (min, max)
 */
function optionalInt(req, param, options = {}) {
  const source = req.query || req; // Support both HTTP and WebSocket

  optional(req, param, /^\d+$/);

  if (source[param]) {
    const number = parseInt(source[param], 10);
    if (isNaN(number)) {
      throw QueryError.malformed(param, "valid integer");
    }

    if (options.min !== undefined && number < options.min) {
      throw QueryError.outOfRange(
        param,
        options.min,
        options.max || "infinity",
        number
      );
    }

    if (options.max !== undefined && number > options.max) {
      throw QueryError.outOfRange(param, options.min || 0, options.max, number);
    }

    source[param] = number;
  }
}

/**
 * Validate mandatory integer parameter
 * @param {object} req - Request object (HTTP) or message object (WebSocket)
 * @param {string} param - Parameter name
 * @param {object} options - Validation options (min, max)
 */
function mandatoryInt(req, param, options = {}) {
  const source = req.query || req; // Support both HTTP and WebSocket

  mandatory(req, param, /^\d+$/);

  const number = parseInt(source[param], 10);
  if (isNaN(number)) {
    throw QueryError.malformed(param, "valid integer");
  }

  if (options.min !== undefined && number < options.min) {
    throw QueryError.outOfRange(
      param,
      options.min,
      options.max || "infinity",
      number
    );
  }

  if (options.max !== undefined && number > options.max) {
    throw QueryError.outOfRange(param, options.min || 0, options.max, number);
  }

  source[param] = number;
}

/**
 * Validate timestamp parameter
 * @param {object} req - Request object (HTTP) or message object (WebSocket)
 * @param {string} param - Parameter name
 * @param {boolean} required - Whether the parameter is required
 */
function timestamp(req, param, required = false) {
  if (required) {
    mandatoryInt(req, param, { min: 0 });
  } else {
    optionalInt(req, param, { min: 0 });
  }

  const source = req.query || req;
  if (source[param]) {
    // Validate that it's a reasonable timestamp (not too far in past/future)
    const now = Math.floor(Date.now() / 1000);
    const tenYears = 10 * 365 * 24 * 60 * 60; // 10 years in seconds

    if (source[param] < now - tenYears || source[param] > now + tenYears) {
      throw QueryError.outOfRange(
        param,
        now - tenYears,
        now + tenYears,
        source[param]
      );
    }
  }
}

// Supported resolutions for validation
const SUPPORTED_RESOLUTIONS = [
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
  "D",
  "1D",
  "3D",
  "W",
  "1W",
  "M",
  "1M",
];

// Middleware functions for Express (HTTP)
const httpMiddleware = {
  symbol(req, res, next) {
    try {
      mandatory(req, "symbol", /^\S+$/, {
        maxLength: 20,
        expected: "non-empty string without spaces",
      });
      next();
    } catch (error) {
      next(error);
    }
  },

  query(req, res, next) {
    try {
      mandatory(req, "query", /^.+$/, {
        minLength: 1,
        maxLength: 50,
        expected: "non-empty string",
      });
      next();
    } catch (error) {
      next(error);
    }
  },

  limit(req, res, next) {
    try {
      optionalInt(req, "limit", { min: 1, max: 1000 });
      next();
    } catch (error) {
      next(error);
    }
  },

  from(req, res, next) {
    try {
      timestamp(req, "from", false);
      next();
    } catch (error) {
      next(error);
    }
  },

  to(req, res, next) {
    try {
      timestamp(req, "to", false);
      next();
    } catch (error) {
      next(error);
    }
  },

  resolution(req, res, next) {
    try {
      mandatory(req, "resolution", /^[A-Z0-9]+$/, {
        allowedValues: SUPPORTED_RESOLUTIONS,
        expected: "valid resolution",
      });
      next();
    } catch (error) {
      next(error);
    }
  },
};

// Validation functions for WebSocket messages
const wsValidators = {
  symbol(params) {
    mandatory(params, "symbol", /^\S+$/, {
      maxLength: 20,
      expected: "non-empty string without spaces",
    });
  },

  query(params) {
    mandatory(params, "query", /^.+$/, {
      minLength: 1,
      maxLength: 50,
      expected: "non-empty string",
    });
  },

  limit(params) {
    optionalInt(params, "limit", { min: 1, max: 1000 });
  },

  from(params) {
    timestamp(params, "from", false);
  },

  to(params) {
    timestamp(params, "to", false);
  },

  resolution(params) {
    mandatory(params, "resolution", /^[A-Z0-9]+$/, {
      allowedValues: SUPPORTED_RESOLUTIONS,
      expected: "valid resolution",
    });
  },

  search(params) {
    this.query(params);
    optional(params, "type", /^[a-zA-Z]+$/, { maxLength: 20 });
    optional(params, "exchange", /^[A-Z]+$/, { maxLength: 20 });
    this.limit(params);
  },

  history(params) {
    this.symbol(params);
    this.from(params);
    this.to(params);
    this.resolution(params);

    // Validate time range
    if (params.from && params.to && params.from >= params.to) {
      throw QueryError.malformed("time_range", "from must be less than to");
    }
  },

  subscribe(params) {
    this.symbol(params);
    this.resolution(params);
  },

  unsubscribe(params) {
    this.symbol(params);
  },
};

module.exports = {
  Error: QueryError,

  // HTTP middleware (original functions)
  ...httpMiddleware,

  // WebSocket validators
  validators: wsValidators,

  // Utility functions
  validate: {
    mandatory,
    optional,
    optionalInt,
    mandatoryInt,
    timestamp,
  },

  // Constants
  SUPPORTED_RESOLUTIONS,
};
