'use strict';

var FileStorageAdapter = require('../../../lib/storage/adapters/fileStorage');
var StorageItem = require('../../../lib/storage/item');
var expect = require('chai').expect;
var utils = require('../../../lib/utils');
var AuditStream = require('../../../lib/audit-tools/audit-stream');
var rimraf = require('rimraf');
var path = require('path');
var TMP_DIR = path.join(__dirname, '../../test_file_storage');
var mkdirp = require('mkdirp');
var fs = require('fs');

var store = null;
var hash = utils.rmd160('test');
var audit = new AuditStream(12);
var item = new StorageItem({
  hash: hash,
  shard: Buffer.from('test'),
});

describe('FileStorageAdapter', function() {
  before(function() {
    if (utils.existsSync(TMP_DIR)) {
      rimraf.sync(TMP_DIR);
    }
    mkdirp.sync(TMP_DIR);
    audit.end(Buffer.from('test'));
    store = new FileStorageAdapter(TMP_DIR);
  });

  describe('@constructor', function() {
    it('should create instance without the new keyword', function() {
      expect(FileStorageAdapter(TMP_DIR)).to.be.instanceOf(FileStorageAdapter);
    });
  });

  describe('#_validatePath', function() {
    it('should not make a directory that already exists', function() {
      expect(function() {
        var tmp = TMP_DIR;
        mkdirp.sync(tmp);
        FileStorageAdapter.prototype._validatePath(tmp);
      }).to.not.throw(Error);
    });
  });

  describe('#_put', function() {
    it('should store the item', function(done) {
      store._put(hash, item, function(err) {
        fs.readFile(`${TMP_DIR}/${hash}`, function(err, file) {
          expect(file).to.not.be.undefined;
        });
        expect(err).equal(null);
        done();
      });
    });
  });

  describe('#_get', function() {
    it('should return the stored item', function(done) {
      store._get(hash, function(err, item) {
        expect(err).to.equal(null);
        expect(item).to.be.instanceOf(Object);
        done();
      });
    });
  });

  it('should return error if a hash that does not exist is retrieved', function(done) {
    store._get('343243_a_hash_that_doesnt_exist', function(err) {
      expect(err.message).to.includes('ENOENT: no such file or directory');
      done();
    });
  });

  describe('#_peek', function() {
    it('should return the stored item', function(done) {
      store._peek(hash, function(err, item) {
        expect(err).to.equal(null);
        expect(item).to.be.instanceOf(Object);
        done();
      });
    });
  });

  describe('#_keys', function() {
    it('should stream all of the keys', function(done) {
      var keyStream = store._keys();
      keyStream.on('data', function(key) {
        expect(key.toString()).to.equal(
          '5e52fee47e6b070565f74372468cdc699de89107'
        );
        keyStream.destroy();
        done();
      });
    });
  });

  describe('#_size', function() {
    it('should return the size of the store on disk', function(done) {
      store._size(null, function(err, shardSize) {
        expect(shardSize).to.be.greaterThan(0);
        done();
      });
    });

    it('should return the size of a single item', function(done) {
      store._size(
        '5e52fee47e6b070565f74372468cdc699de89107',
        function(err, shardSize) {
          expect(shardSize).to.be.greaterThan(0);
          done();
        }
      );
    });
    it('should return an error for a key that doesnt exist', function(done) {
      store._size('sdfds_not_existing_key', function(err) {
        expect(err.message).to.includes('ENOENT: no such file or directory');
        done();
      });
    });
  });
  describe('#_del', function() {
    it('should delete the shard if it exists', function(done) {
      store._del(hash, function(err) {
        expect(err).to.equal(null);
        fs.readFile(`${TMP_DIR}/${hash}`, (err) => {
          expect(err.message).to.includes('ENOENT: no such file or directory');
        });
        done();
      });
    });

    it('should return an error for a key that doesnt exist', function(done) {
      store._del('sdfds_not_existing_key', function(err) {
        expect(err.message).to.includes('ENOENT: no such file or directory');
        done();
      });
    });
  });

  describe('#_flush', function() {
    it('delete all files after flush', function(done) {
      store._flush(function() {
        fs.readdir(TMP_DIR, (err, shards) => {
          expect(shards.length).to.equal(0);
          done();
        });
      });
    });
  });
});

after(function() {
  rimraf.sync(TMP_DIR);
});
