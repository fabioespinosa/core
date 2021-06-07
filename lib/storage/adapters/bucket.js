'use strict';

var inherits = require('util').inherits;
var StorageAdapter = require('../adapter');
var levelup = require('levelup');
var leveldown = require('leveldown');
var kfs = require('kfs');
var path = require('path');
var assert = require('assert');
var utils = require('../../utils');
var mkdirp = require('mkdirp');
const AWS = require('aws-sdk');
const { PassThrough } = require('stream')

/**
 * Implements an LevelDB/KFS storage adapter interface
 * @extends {StorageAdapter}
 * @param {String} storageDirPath - Path to store the level db
 * @constructor
 * @license AGPL-3.0
 */
function BucketStorageAdapter(storageDirPath) {
  if (!(this instanceof BucketStorageAdapter)) {
    return new BucketStorageAdapter(storageDirPath);
  }

  this._validatePath(storageDirPath);

  this._path = storageDirPath;
  this._db = levelup(leveldown(path.join(this._path, 'contracts.db')), {
    maxOpenFiles: BucketStorageAdapter.MAX_OPEN_FILES
  });
  this._fs = kfs(path.join(this._path, 'sharddata.kfs'));

  this.bucket = new AWS.S3({
    endpoint: new AWS.Endpoint('s3.wasabisys.com'),
    credentials: new AWS.Credentials({
      accessKeyId: '',
      secretAccessKey: ''
    }),
    signatureVersion: 'v4'
  });

  this._isOpen = true;

  this._lastTimeSize = null;
  this._storageSize = 0;
}

BucketStorageAdapter.SIZE_START_KEY = '0';
BucketStorageAdapter.SIZE_END_KEY = 'z';
BucketStorageAdapter.MAX_OPEN_FILES = 1000;
BucketStorageAdapter.S3_SHARD_BUCKETNAME = 'sharddata';

inherits(BucketStorageAdapter, StorageAdapter);

/**
 * Validates the storage path supplied
 * @private
 */
BucketStorageAdapter.prototype._validatePath = function (storageDirPath) {
  if (!utils.existsSync(storageDirPath)) {
    mkdirp.sync(storageDirPath);
  }

  assert(utils.isDirectory(storageDirPath), 'Invalid directory path supplied');
};

/**
 * Implements the abstract {@link StorageAdapter#_get}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._get = function (key, callback) {
  var self = this;

  this._db.get(key, { fillCache: false }, function (err, value) {
    if (err) {
      return callback(err);
    }

    var result = JSON.parse(value);
    var fskey = result.fskey || key;

    self._objectExists(BucketStorageAdapter.S3_SHARD_BUCKETNAME, fskey, function (err, exists) {
      if (err) {
        return callback(err);
      }

      function _getShardStreamPointer(callback) {
        const getReadStream = () => {
          const stream = self.bucket.getObject({
            Bucket: BucketStorageAdapter.S3_SHARD_BUCKETNAME,
            Key: fskey
          });

          return stream.createReadStream();
        }

        const getWriteStream = () => {
          const pass = new PassThrough();

          self.bucket.upload({ Bucket: BucketStorageAdapter.S3_SHARD_BUCKETNAME, Key: fskey, Body: pass }).send();
          return pass;
        }

        var getStream = exists ? getReadStream() : getWriteStream();

        if (!exists) {
          fskey = utils.ripemd160(key, 'hex');
          result.fskey = fskey;
        }

        result.shard = getStream;

        callback(null, result);
      }

      _getShardStreamPointer(callback);
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_peek}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._peek = function (key, callback) {
  this._db.get(key, { fillCache: false }, function (err, value) {
    if (err) {
      return callback(err);
    }

    callback(null, JSON.parse(value));
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_put}
 * @private
 * @param {String} key
 * @param {Object} item
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._put = function (key, item, callback) {
  var self = this;

  item.shard = null; // NB: Don't store any shard data here

  item.fskey = utils.ripemd160(key, 'hex');

  self._db.put(key, JSON.stringify(item), {
    sync: true
  }, function (err) {
    if (err) {
      return callback(err);
    }

    callback(null);
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_del}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._del = function (key, callback) {
  var self = this;
  var fskey = key;

  self._peek(key, function (err, item) {
    if (!err && item.fskey) {
      fskey = item.fskey;
    }

    self._db.del(key, function (err) {
      if (err) {
        return callback(err);
      }

      self._fs.unlink(fskey, function (err) {
        if (err) {
          return callback(err);
        }

        callback(null);
      });
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._flush = function (callback) {
  this._fs.flush(callback);
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._size = function (key, callback) {
  var self = this;

  if (typeof key === 'function') {
    callback = key;
    key = null;
  }

  this._db.db.approximateSize(
    BucketStorageAdapter.SIZE_START_KEY,
    BucketStorageAdapter.SIZE_END_KEY,
    function (err, contractDbSize) {
      if (err) {
        return callback(err);
      }

      function handleStatResults(err, kfsUsedSpace) {
        if (err) {
          return callback(err);
        }

        callback(null, kfsUsedSpace, contractDbSize);
      }

      self._getBucketSize(BucketStorageAdapter.S3_SHARD_BUCKETNAME, handleStatResults);
    }
  );
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
BucketStorageAdapter.prototype._keys = function (options) {
  return this._db.createKeyStream(options);
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._open = function (callback) {
  var self = this;

  if (!this._isOpen) {
    return this._db.open(function (err) {
      if (err) {
        return callback(err);
      }

      self._isOpen = true;
      callback(null);
    });
  }

  callback(null);
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._close = function (callback) {
  var self = this;

  if (this._isOpen) {
    return this._db.close(function (err) {
      if (err) {
        return callback(err);
      }

      self._isOpen = false;
      callback(null);
    });
  }

  callback(null);
};

/**
 * Look up if the data exists on datashardDB
 * 
 * @param {*} fskey File Key on contracts database 
 */
BucketStorageAdapter.prototype.existDataShard = function (fsKey, callback) {
  const self = this;

  self._fs.exists(fsKey, function (err, exists) {
    if (err) {
      return callback(err);
    }
    callback(null, exists)
  });
}

BucketStorageAdapter.prototype._getBucketSize = function (bucket, prefix, callback) {
  if (typeof prefix === 'function') {
    callback = prefix;
    prefix = '';
  }

  let numObjects = 0;
  let totalBytes = 0;

  const client = this.bucket;

  (function listNextChunk(nextToken) {
    var params = {
      Bucket: bucket,
      Prefix: prefix.toString(),
      ContinuationToken: nextToken
    };

    client.listObjectsV2(params, function (err, data) {
      if (err) return callback(err);

      data.Contents.forEach(function (obj) {
        numObjects++;
        totalBytes += obj.Size;
      });

      if (data.NextContinuationToken) {
        return listNextChunk(data.NextContinuationToken);
      }

      return callback(null, totalBytes, {
        numObjects: numObjects,
        bytes: totalBytes
      });
    });
  })();
}

BucketStorageAdapter.prototype._objectExists = function (bucket, key, callback) {
  this.bucket.headObject({
    Bucket: bucket,
    Key: key
  }).on('success', function () {
    callback(null, true);
  }).on('error', function (err) {
    if (err.code === 'NotFound') {
      return callback(null, false);
    } else {
      return callback(err);
    }
  }).send();
}

module.exports = BucketStorageAdapter;
