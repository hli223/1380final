#!/usr/bin/env node

const util = require('./distribution/util/util.js');
const args = require('yargs').argv;
// const { PorterStemmer } = require('natural');
// console.log(PorterStemmer);
// const stemmer = require('lancaster-stemmer')
// const { PorterStemmer } = require('natural');
// console.log('successfully imported natural');
// console.log(natural);
// console.log(stemmer);


// Default configuration
global.nodeConfig = global.nodeConfig || {
  ip: '127.0.0.1',
  port: 8080,
  onStart: () => {
    console.log('Node started!');
  },
};

/*
    As a debugging tool, you can pass ip and port arguments directly.
    This is just to allow for you to easily startup nodes from the terminal.

    Usage:
    ./distribution.js --ip '127.0.0.1' --port 1234
  */
if (args.ip) {
  global.nodeConfig.ip = args.ip;
}

if (args.port) {
  global.nodeConfig.port = parseInt(args.port);
}


if (args.config) {
  let nodeConfig = util.deserialize(args.config);
  global.nodeConfig.ip = nodeConfig.ip ? nodeConfig.ip : global.nodeConfig.ip;
  global.nodeConfig.port = nodeConfig.port ?
    nodeConfig.port : global.nodeConfig.port;
  global.nodeConfig.onStart = nodeConfig.onStart ?
    nodeConfig.onStart : global.nodeConfig.onStart;
}

const distribution = {
  util: require('./distribution/util/util.js'),
  local: require('./distribution/local/local.js'),
  node: require('./distribution/local/node.js'),
};

// distribution.stemmer = PorterStemmer;

global.distribution = distribution;

// global.natural = natural;

distribution['all'] = {};
distribution['all'].status =
  require('./distribution/all/status')({ gid: 'all' });
distribution['all'].comm =
  require('./distribution/all/comm')({ gid: 'all' });
distribution['all'].gossip =
  require('./distribution/all/gossip')({ gid: 'all' });
distribution['all'].groups =
  require('./distribution/all/groups')({ gid: 'all' });
distribution['all'].routes =
  require('./distribution/all/routes')({ gid: 'all' });
distribution['all'].mem =
  require('./distribution/all/mem')({ gid: 'all' });
distribution['all'].store =
  require('./distribution/all/store')({ gid: 'all' });

const { JSDOM } = require('jsdom');
global.JSDOM = JSDOM;
const { PorterStemmer } = require('natural');
global.stemmer = PorterStemmer;
module.exports = global.distribution;
global.fetch = require('node-fetch');
global.promisify = require('./distribution/util/promisify');


/* The following code is run when distribution.js is run directly */
if (require.main === module) {
  distribution.node.start(global.nodeConfig.onStart);
}
