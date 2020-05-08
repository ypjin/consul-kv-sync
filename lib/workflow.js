'use strict';

const jptr = require('json-ptr');
const Promise = require('bluebird');
const readFile = Promise.promisify(require('fs').readFile);
const _ = require('lodash');
const log = require('./logger');
const kv = require('consul-kv-object');

/*
 * Creates a workflow that will synchronize one or more JSON files with consul's key-value store.
 * @param client - a consul client, requires that kv functions have been promisified
 * @param {Array} files - list of files to synchronize
 */
module.exports = (client, files) => {
  let workflow = { stats: { put: 0, deleted: 0 }, config: null };

  /*
   * Reads json pointers for the given file name and validates each file.
   * Also figures out what the prefix is for the consul keys we are updating.
   * @param {String} fileName - the file to read
   * @returns {Promise} when resolved, an array of pointer-values for the file.
   */
  const readPointers = (fileName) => {
    return readFile(fileName, 'utf8')
      .then(JSON.parse)
      .then((contents) => {
        log.debug(`Read ${fileName}:`);
        log.debug(contents);
        let keys = _.keys(contents);
        if (keys.length == 0) {
          throw new Error(`Each configuration file must have at least a single top-level node identifying the service.`);
        }
        let pointers = jptr.list(contents);
        let prefix = pointers[1].pointer.substring(1);
        if (workflow.prefix) {
          if (prefix !== workflow.prefix) {
            throw new Error(`Each file must have the same top-level node. Expected "${fileName}'" to have top-level node "${workflow.prefix}", but it has "${prefix}".`);
          }
        } else {
          workflow.prefix = prefix;
        }
        return pointers;
      });
  };

  /*
   * Filters and reduces the array of pointers into an object
   * with the keys being the pointers and the values being the values.
   * Only items with non-object or array values are retrieved.
   * @param [Array] flattened - the array of pointers to work on
   * @returns [Promise] when fulfilled, a hash containing the keys and values
   */
  const reduce = (flattened) => {
    let reduced = _.reduce(_.filter(flattened, (x) => {
      return _.isString(x.value) || _.isFinite(x.value);
    }), function (acc, item) {
      acc[item.pointer.substring(1)] = item.value;
      return acc;
    }, {});
    workflow.config = reduced;
    return reduced;
  };

  /*
   * Retrieves the list of keys that currently exist in consul.
   */
  const getExistingKeys = () => {
    log.debug('Getting all keys');
    return client.kv.keysAsync('')
      .catch((err) => {
        if (err.message === 'not found') {
          return [];
        }
        throw err;
      })
      .then((keys) => {
        log.debug('Keys: ', keys);
        workflow.existing = keys;
      });
  };

  /*
   * PUTs all of the keys and values into consul. Tracks which ones have
   * been PUT so we can delete the remaining keys.
   */
  const put = () => {
    log.debug('put keys in the provided files to consul...');
    return Promise.all(_.map(workflow.config, (value, key) => {
      workflow.stats.put++;
      workflow.existing = _.filter(workflow.existing, (item) => {
        return item !== key;
      });
      log.debug(`Setting "${key}" to "${value}"`);
      return client.kv.setAsync({
        key: key,
        value: '' + value
      });
    }));
  };

  /*
   * ADDs all of the keys and values not existing in consul into consul. 
   */
  const add = () => {
    log.debug('add keys missing in consul...');
    return Promise.all(_.map(workflow.config, (value, key) => {
      let foundItem = _.find(workflow.existing, (item) => {
        return item == key;
      });
      if(foundItem && Object.keys(foundItem).length > 0) {
        log.debug(`-- "${key}" exists in consul`);
      } else {
        workflow.stats.put++;
        log.debug(`Setting "${key}" to "${value}"`);
        return client.kv.setAsync({
          key: key,
          value: '' + value
        });
      }
    }));
  };

  /*
   * DELETE any existing keys that have not been PUT.
   */
  const del = () => {
    log.debug('delete keys in consul but not in the provided file(s)...');
    return Promise.map(workflow.existing, function(key) {
      workflow.stats.deleted++;
      log.debug(`Deleting "${key}"`);
      return client.kv.delAsync(key);
    }).then(() => {
      delete workflow.existing;
    });
  };


  /*
   * Retrieves the list of keys that currently exist in consul.
   */
  const getKeysUnder = (keypath, json) => {
    log.debug('Getting all keys under ' + keypath);

    if(json) {
      // https://github.com/lekoder/consul-to-json
      var allKVs = Promise.promisifyAll(kv(client.kv, {concurrency: 3 }));
      allKVs.getAsync(keypath).then(function(abranch) {
        log.info(JSON.stringify(abranch, null, 4));
      });
      return
    }

    return client.kv.keysAsync(keypath)
      .catch((err) => {
        if (err.message === 'not found') {
          return [];
        }
        throw err;
      })
      .then((keys) => {
        log.info('Keys: ', keys);
      });
  };

  /*
   * Executes the workflow
   */
  workflow.exec = () => {
    log.debug('sync keys in consul with the provided files');
    return Promise.map(files, readPointers)
      .then(_.flatten)
      .then(reduce)
      .then(getExistingKeys)
      .then(put)
      .then(del);
  };

  /*
   * Update certain keys 
   */
  workflow.update = () => {
    log.debug('update keys in consul');
    return Promise.map(files, readPointers)
      .then(_.flatten)
      .then(reduce)
      .then(getExistingKeys) //not necessary but to avoid put() throwing error
      .then(put)
  }

    /*
   * add keys not existig in consul only (no update, no delete)
   */
  workflow.add = () => {
    log.debug('add keys in the provided file(s) but not in consul');
    return Promise.map(files, readPointers)
      .then(_.flatten)
      .then(reduce)
      .then(getExistingKeys) //not necessary but to avoid put() throwing error
      .then(add)
  }

  workflow.get = (keypath, json) => {
    return getKeysUnder(keypath, json)
  }


  return workflow;
};
