'use strict';

const inherits = require('util').inherits;
const StorageAdapter = require('../adapter');
const AWS = require('aws-sdk');

function BucketStorageAdapter(options) {
  if (!(this instanceof BucketStorageAdapter)) {
    return new BucketStorageAdapter(options);
  }
  this._config = options;
  this._isOpen = false;
  this._open(() => { });
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
  if (this._isOpen) {
    return callback();
  }
  const endpoint = new AWS.Endpoint(this._config.endpoint);
  const credentials = new AWS.Credentials({
    accessKeyId: this._config.accessKeyId,
    secretAccessKey: this._config.secretAccessKey
  })
  var s3 = new AWS.S3({ endpoint, credentials, signatureVersion: 'v4' });
  this.bucket = s3;
  this._isOpen = true;
  callback();
}

BucketStorageAdapter.prototype._close = function (callback) {

}

module.exports = BucketStorageAdapter;
