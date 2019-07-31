'use strict';

const Winston = require('winston');

var myCustomLevels = {
  levels: {
    error: 1,
    warn: 2,
    info: 3,
    debug: 4
  },
  colors: {
    error: 'red',
    info: 'green',
    debug: 'blue',
    warn: 'red'
  }
};


function logLevelName(level) {
  var result = Object.keys(myCustomLevels.levels).filter((key, value) => {
    return value === level;
  });

  if (result.length > 0) {
    return result[0];
  } else {
    return undefined;
  }
}

// eslint-disable-next-line no-unused-vars
const logLevel = process.env.NODE_ENV === 'test' ? 0 : 3;

function Logger(level) {
  const logger = Winston.createLogger(
    {
      level: logLevelName(level),
      levels: myCustomLevels.levels,
      format: Winston.format.combine(
        Winston.format.colorize({ all: true }),
        Winston.format.timestamp({ format: 'YYYY-MM-DD HH:MM:SS' }),
        Winston.format.splat(),
        Winston.format.printf(info => {
          return `${info.timestamp} ${info.level}: ${info.message}`;
        })
      ),
      transports: [
        new Winston.transports.Console()
      ]
    }
  );
  return logger;
}

module.exports = Logger;