'use strict';

const inherits = require('util').inherits;
const StorageAdapter = require('../adapter');
const SolidBucket = require('solid-bucket');

function BucketStorageAdapter(options) {
  if (!(this instanceof BucketStorageAdapter)) {
    return new BucketStorageAdapter(options);
  }
}

inherits(BucketStorageAdapter, StorageAdapter);

BucketStorageAdapter.prototype._get = function (key, callback) {

}

BucketStorageAdapter.prototype._peek = function (key, callback) {

}

BucketStorageAdapter.prototype._put = function (key, item, callback) {

}

BucketStorageAdapter.prototype._del = function (key, callback) {

}

BucketStorageAdapter.prototype._flush = function (callback) {

}

BucketStorageAdapter.prototype._size = function (key, callback) {

}

BucketStorageAdapter.prototype._keys = function (options) {

}

BucketStorageAdapter.prototype._open = function (callback) {

}

BucketStorageAdapter.prototype._close = function (callback) {

}

module.exports = BucketStorageAdapter;
