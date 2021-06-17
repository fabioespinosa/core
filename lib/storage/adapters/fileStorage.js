'use strict';

var inherits = require('util').inherits;
var StorageAdapter = require('../adapter');
var path = require('path');
var assert = require('assert');
var utils = require('../../utils');
var mkdirp = require('mkdirp');
var fs = require('fs');
var stream = require('stream');

/**
 * Implements an FileSystem storage adapter interface
 * @extends {StorageAdapter}
 * @param {String} storageDirPath - Path to store the level db
 * @constructor
 * @license AGPL-3.0
 */
function FileStorageAdapter(storageDirPath) {
  if (!(this instanceof FileStorageAdapter)) {
    return new FileStorageAdapter(storageDirPath);
  }

  this._validatePath(storageDirPath);

  this._path = storageDirPath || '/file_storage';
  // this._db = levelup(leveldown(path.join(this._path, 'contracts.db')), {
  //   maxOpenFiles: FileStorageAdapter.MAX_OPEN_FILES,
  // });
  // this._fs = kfs(path.join(this._path, 'sharddata.kfs'));
  this._isOpen = true;
}

// FileStorageAdapter.SIZE_START_KEY = '0';
// FileStorageAdapter.SIZE_END_KEY = 'z';
// FileStorageAdapter.MAX_OPEN_FILES = 1000;

inherits(FileStorageAdapter, StorageAdapter);

/**
 * Validates the storage path supplied
 * @private
 */
FileStorageAdapter.prototype._validatePath = function(storageDirPath) {
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
FileStorageAdapter.prototype._get = function(key, callback) {
  fs.readFile(`${this._path}/${key}`, function(err, value) {
    if (err) {
      return callback(err);
    }
    try {
      const parsed_shard = JSON.parse(value);
      callback(null, parsed_shard);
    } catch (err) {
      return callback(err);
    }
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_peek}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
// We assume is the same as _get
FileStorageAdapter.prototype._peek = FileStorageAdapter.prototype._get;

/**
 * Implements the abstract {@link StorageAdapter#_put}
 * @private
 * @param {String} key
 * @param {Object} item
 * @param {Function} callback
 */
FileStorageAdapter.prototype._put = function(key, item, callback) {
  fs.writeFile(
    `${this._path}/${key}`,
    JSON.stringify(item),
    {},
    function(err) {
      if (err) {
        return callback(err);
      }
      callback(null);
    }
  );
};

/**
 * Implements the abstract {@link StorageAdapter#_del}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._del = function(key, callback) {
  fs.unlink(`${this._path}/${key}`, callback);
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._flush = function(callback) {
  // I suppose we have to flush all the shards:
  const path_to_flush = this._path;
  fs.readdir(path_to_flush, (err, files) => {
    if (err) {
      throw err;
    }

    for (const file of files) {
      fs.unlinkSync(path.join(path_to_flush, file), (err) => {
        if (err) {
          throw err;
        }
      });
    }
    callback(null);
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * It gets size of a key (if passed) or the whole db.
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
FileStorageAdapter.prototype._size = function(key, callback) {
  const db_path = this._path;
  if (key) {
    fs.readFile(`${db_path}/${key}`, function(err) {
      if (err) {
        // We throw an error if file does not exist
        return callback(err);
      }
      callback(null, fs.statSync(`${db_path}/${key}`).size);
    });
  } else {
    // Calculate size of whole db
    fs.readdir(db_path, (err, files) => {
      if (err) {
        callback(err);
      }
      let total_size = 0;
      for (const file of files) {
        total_size += fs.statSync(`${db_path}/${file}`).size;
      }
      callback(null, total_size);
    });
  }
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
FileStorageAdapter.prototype._keys = function() {
  const path_keys = this._path;
  return stream.Readable({
    read() {
      const stream = this;
      fs.readdir(path_keys, function(err, shards) {
        if (err) {
          throw err;
        }
        shards.forEach((shard) => {
          stream.push(shard);
        });
        stream.emit('end');
      });
    },
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._open = function(callback) {
  callback('this method is not applicable to File Storage');
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._close = function(callback) {
  callback('this method is not applicable to File Storage');
};

module.exports = FileStorageAdapter;
