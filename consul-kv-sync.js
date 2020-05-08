#!/usr/bin/env node
'use strict';

const program = require('commander');
const pkg = require('./package');
const log = require('./lib/logger');
const clientFactory = require('./lib/client');
const workflowFactory = require('./lib/workflow');

function collectCA(value, items) {
  items.push(value);
}

program.version(pkg.version)
  .usage('[options] <file ...>')
  .description('Synchronizes one or more JSON manifests with consul\'s key value store.')
  .option('-H, --host <host>', 'Consul API url. Environment variable: CONSUL_HOST. Default: consul.service.consul')
  .option('-p, --port <port>', 'Consul API port. Environment variable: CONSUL_PORT. Default: 8500')
  .option('-s, --secure', 'Enable HTTPS. Environment variable: CONSUL_SECURE.')
  .option('--ca <ca>', 'Path to trusted certificate in PEM format. Specify multiple times for multiple certificates.', collectCA, [])
  .option('-v, --verbose', 'If present, verbose output provided.')
  .option('-u, --update', 'extra keys in consul will not be deleted (update only)')
  .option('-g, --get [keypath]', 'fetch keys under the provided keypath')
  .option('-a, --add', 'only keys missing in consul will be added (no modification, no deletion)')
  .option('--json', 'output keys and values in json (used with -g)')
  .on('--help', function() {
    console.log('  Examples:');
    console.log('');
    console.log('    $ consul-kv-sync my-service-global.json my-service-dev.json');
    console.log('    $ CONSUL_HOST=consul.local consul-kv-sync my-service-global.json my-service-dev.json');
    console.log('    $ consul-kv-sync --host localhost --port 8500 --secure \\');
    console.log('        --ca root-ca.pem --ca intermediate-ca.pem \\');
    console.log('        my-service-global.json my-service-dev.json');
    console.log('');
  });

program.parse(process.argv);
if (program.verbose) {
  log.transports.console.level = 'debug';
}

let client = clientFactory(program);
let workflow = workflowFactory(client, program.args);

if (program.get) {
  if (program.get === true) { // no keypath provided to get all keys
    program.get = "";
  }
  workflow.get(program.get, program.json);
  return
}

if (!program.args.length) {
  program.outputHelp();
  process.exit(1);
}

var updateMethod = "exec";
if (program.update) {
  updateMethod = "update";
} else if (program.add) {
  updateMethod = "add";
}

workflow[updateMethod]().then(() => {
  log.info('Sync completed. ' + workflow.stats.put + ' items set, ' + workflow.stats.deleted + ' items deleted.');
  log.info('Config:');
  log.info(workflow.config);
})
.catch((err) => {
  if (program.verbose) {
    log.error(err);
  } else {
    log.error(err.message);
  }
  process.exit(99);
});
