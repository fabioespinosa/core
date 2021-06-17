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
function BucketStorageAdapter(storageDirPath, credentials) {
  if (!(this instanceof BucketStorageAdapter)) {
    return new BucketStorageAdapter(storageDirPath);
  }

  this._validatePath(storageDirPath);
  this._validateCredentials(credentials);

  this._path = storageDirPath;
  this._credentials = credentials;
  this._db = levelup(leveldown(path.join(this._path, 'contracts.db')), {
    maxOpenFiles: BucketStorageAdapter.MAX_OPEN_FILES
  });

  this.bucket = new AWS.S3({
    endpoint: new AWS.Endpoint(this._credentials.endpoint),
    credentials: new AWS.Credentials({
      accessKeyId: this._credentials.accessKeyId,
      secretAccessKey: this._credentials.secretAccessKey
    }),
    signatureVersion: this._credentials.signatureVersion || 'v4'
  });

  this.SHARD_BUCKET_NAME = this._credentials.bucketName || BucketStorageAdapter.S3_SHARD_BUCKETNAME + '.' + credentials.bucketName;


  if (this.SHARD_BUCKET_NAME !== this._credentials.bucketName) {
    this.bucket.createBucket({
      Bucket: this.SHARD_BUCKET_NAME,
      CreateBucketConfiguration: {
        LocationConstraint: this._credentials.bucketLocation || 'eu-central-1'
      }
    }, (err) => {
      if (err) {
        throw Error('Bucket not available. Please, create it manually. Reason: ' + err.message);
      }
    });
  }

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

BucketStorageAdapter.prototype._validateCredentials = function (credentials) {
  assert(credentials.accessKeyId, 'Invalid S3 Access Key Id');
  assert(credentials.secretAccessKey, 'Invalid S3 Secret Access Key');
}

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

    self._objectExists(self.SHARD_BUCKET_NAME, fskey, function (err, exists) {
      if (err) {
        return callback(err);
      }

      function _getShardStreamPointer(callback) {
        const getReadStream = () => {
          const stream = self.bucket.getObject({
            Bucket: self.SHARD_BUCKET_NAME,
            Key: fskey
          });

          return stream.createReadStream();
        }

        const getWriteStream = () => {
          const pass = new PassThrough();

          self.bucket.upload({ Bucket: self.SHARD_BUCKET_NAME, Key: fskey, Body: pass }).send((err) => {
            pass.emit('error', err);
          });
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

      self._deleteObject(self.SHARD_BUCKET_NAME, fskey, callback);
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
BucketStorageAdapter.prototype._flush = function (callback) {
  // No flush implementation for S3 Bucket
  callback(null);
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

      function handleStatResults(err, bucketUsedSpace) {
        if (err) {
          return callback(err);
        }

        if (key) {
          callback(null, Math.ceil(bucketUsedSpace / kfs.constants.B), contractDbSize);
        } else {
          callback(null, bucketUsedSpace, contractDbSize);
        }
      }

      self._getBucketSize(self.SHARD_BUCKET_NAME, handleStatResults);
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
BucketStorageAdapter.prototype.existDataShard = function (fskey, callback) {
  this._objectExists(self.SHARD_BUCKET_NAME, fskey, callback);
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
      if (err) {
        return callback(err);
      }

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

BucketStorageAdapter.prototype._deleteObject = function (bucket, object, callback) {
  const params = { Bucket: bucket, Key: object }

  this.bucket.deleteObject(params, callback);
}

module.exports = BucketStorageAdapter;
