'use strict';

var inherits = require('util').inherits;
var StorageAdapter = require('../adapter');
var path = require('path');
var assert = require('assert');
var utils = require('../../utils');
var mkdirp = require('mkdirp');
var fs = require('fs');
var stream = require('stream');
var rimraf = require('rimraf');
var diskusage = require('diskusage');
var async = require('async');

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

  // Set folders for storage:
  this._path = storageDirPath || '/file_storage';
  this._contracts_path = path.join(this._path, 'contracts');
  this._shards_path = path.join(this._path, 'shards');

  // Create the storage folders if they not already exist:
  this._validatePath(this._path);
  this._validatePath(this._contracts_path);
  this._validatePath(this._shards_path);

  this._isOpen = true;
}

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
 * 1. Reads the contract
 * 2. Gets the key for the shard from the saved contract in the 'fskey' attribute
 * 3. Checks if the shard with the given key exists
 * 4. If it does exist, it assigns the stream to the contract
 * 4. If it doesn't exist, it assigns the write stream to the retrieved contract
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._get = function(key, callback) {
  const contract_path = path.join(this._contracts_path, key);
  const shards_path = this._shards_path;
  fs.readFile(contract_path, function(err, value) {
    if (err) {
      return callback(err);
    }
    try {
      const result = JSON.parse(value);
      const fskey = result.fskey || key;
      const shard_path = path.join(shards_path, fskey);
      // Check if shard exists:
      fs.stat(shard_path, function(err) {
        if (err === null) {
          // Shard exists, assign the stream to the parsed contract object:
          result.shard = fs.createReadStream(shard_path);
          callback(null, result);
        } else if (err.code === 'ENOENT') {
          // Shard doesn't exist, create write stream:
          const new_fs_key = utils.rmd160(key, 'hex');
          const new_shard_path = path.join(shards_path, new_fs_key);
          result.fskey = new_fs_key;
          result.shard = fs.createWriteStream(new_shard_path);
          callback(null, result);
        } else {
          callback(err);
        }
      });
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
// Assumed: We only get the contract, not the shard.
FileStorageAdapter.prototype._peek = function(key, callback) {
  const contract_path = path.join(this._contracts_path, key);
  fs.readFile(contract_path, function(err, value) {
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
FileStorageAdapter.prototype._put = function(key, item, callback) {
  item.shard = null;
  item.fskey = utils.rmd160(key, 'hex');
  const contract_path = path.join(this._contracts_path, key);
  fs.writeFile(contract_path, JSON.stringify(item), {}, function(err) {
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
FileStorageAdapter.prototype._del = function(key, callback) {
  const contract_path = path.join(this._contracts_path, key);
  const shards_path = this._shards_path;
  let fskey = key;
  this._peek(key, function(err, item) {
    if (!err && item.fskey) {
      fskey = item.fskey;
    }
    const shard_path = path.join(shards_path, fskey);
    fs.unlink(contract_path, function(err) {
      // Ignore error if contract doesn't exist, and still go for deleting shard:
      if (err && err.code !== 'ENOENT') {
        callback(err);
      }
      fs.unlink(shard_path, function(err) {
        // Ignore error if shard doesn't exist:
        if (err && err.code !== 'ENOENT') {
          callback(err);
        }
        callback(null);
      });
    });

    // Possible optimization, do it in parallel:
    // async.parallel(
    //   [fs.unlink(contract_path, callback), fs.unlink(shard_path, callback)],
    //   function (err) {
    //     if (err && err.code !== 'ENOENT') {
    //       callback(err);
    //     }
    //     callback(null);
    //   }
    // );
    // Or using promises: await Promise.all([<delete contract>, <delete shard>])
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._flush = function(callback) {
  // I suppose we have to flush all the contracts and shards:
  // Delete all folders and re-create the folder that contains them:
  const contracts_path = this._contracts_path;
  const shards_path = this._shards_path;
  const validatePath = this._validatePath;
  rimraf(contracts_path, function(err) {
    if (err) {
      callback(err);
    }
    rimraf(shards_path, function(err) {
      if (err) {
        callback(err);
      }
      // Re-create folders:
      validatePath(contracts_path);
      validatePath(shards_path);
      callback(null);
    });
  });

  // Or do it one by one:
  // fs.readdir(path_to_flush, (err, files) => {
  //   if (err) {
  //     throw err;
  //   }

  //   for (const file of files) {
  //     fs.unlinkSync(path.join(path_to_flush, file), (err) => {
  //       if (err) {
  //         throw err;
  //       }
  //     });
  //   }
  //   callback(null);
  // });
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * It gets size of a key (if passed) or the whole db.
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
FileStorageAdapter.prototype._size = function(key, callback) {
  // If key passed, its checking just one shard
  if (key) {
    const contract_path = path.join(this._contracts_path, key);
    fs.readFile(contract_path, function(err) {
      if (err) {
        return callback(err);
      }
      callback(null, fs.statSync(contract_path).size);
    });
  } else {
    // Calculate size of contracts + shards.
    // NOTE: Will return how much is used in the whole node
    diskusage.check(this._path, function(err, info) {
      if (err) {
        return callback(err);
      }
      callback(null, info.total - info.free);
    });
  }
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
FileStorageAdapter.prototype._keys = function() {
  // Using streams -perhaps premature optimization depending on the number of contracts-
  const path_keys = this._contracts_path;
  return stream.Readable({
    read() {
      const stream = this;
      fs.readdir(path_keys, function(err, contract_keys) {
        if (err) {
          throw err;
        }
        contract_keys.forEach((contract_key) => {
          stream.push(contract_key);
        });
        stream.emit('end');
      });
    },
  });

  // Or simply reading the directory, needs callback:
  // fs.readdir(this._contracts_path, function (err, contract_keys) {
  //   if (err) {
  //     callback(err);
  //   }
  //   callback(null, contract_keys);
  // });
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._open = function(callback) {
  callback({ message: 'this method is not applicable to File Storage' });
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._close = function(callback) {
  callback({ message: 'this method is not applicable to File Storage' });
};

module.exports = FileStorageAdapter;
