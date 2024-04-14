const http = require('http');

const serialization = require('../util/serialization');
const id = require('../util/id');

const node = global.nodeConfig;

const mem = require('./mem');
const groups = require('./groups');
const store = require('./store');
const gossip = require('./gossip');

/*

Service  Description                           Methods
status   statusrmation about the current node  get
routes   A mapping from names to functions     get, put
comm     A message communication interface     send

*/
const status = {};
global.myStates = {
  mem: mem,
  groups: groups,
  store: store,
  gossip: gossip,
};
global.myStates.counts = 0;
status.get = function(key, cb) {
  let res = null;
  let err = null;
  global.counts += 1;
  if (key === 'nid') {
    res = id.getNID(node);
  } else if (key === 'sid') {
    res = id.getSID(node);
  } else if (key === 'ip') {
    res = node.ip;
  } else if (key === 'port') {
    res = node.port;
  } else if (key == 'counts') {
    res = global.myStates.counts;
  } else {
    err = new Error('Wrong key.');
  }
  if (cb !== null) {
    cb(err, res);
  }
};


const routes = {};
routes.get = function(service, cb) {
  let err = null;
  let res = null;
  global.myStates.counts += 1;
  if (service === 'status') {
    res = status;
  } else if (service === 'routes') {
    res = routes;
  } else if (service === 'comm') {
    res = comm;
  } else if (global.myStates.hasOwnProperty(service)) {
    res = global.myStates[service];
  } else {
    err = new Error(`Service ${service} does not exist`);
  }
  if (cb !== null) {
    cb(err, res);
  }
};

routes.put = function(service, key, cb) {
  let err = null;
  let res = null;
  global.myStates.counts += 1;
  global.myStates[key] = service;
  if (cb !== null) {
    cb(err, res);
  }
};

const comm = {
  send: function(message, remote, callback) {
    status.messageCount++;
    if (remote.node === 'NODE_INFO') {
      remote.node = global.config;
    }
    // console.log("remote: ",remote)

    const requestOptions = {
      hostname: remote.node.ip,
      port: remote.node.port,
      path: `/${remote.service}/${remote.method}`,
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
    };

    const req = http.request(requestOptions, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        try {
          // The result from http server
          const deserializedData = serialization.deserialize(data);
          // console.log('data: ', data);
          // console.log('deserializedData:  ', deserializedData);
          callback(deserializedData[0], deserializedData[1]);
        } catch (error) {
          console.error(`Error handling response: ${error}`); // For debugging
          callback(error);
        }
      });
    });

    const serializedMessage = serialization.serialize(message);
    // console.log("serializedMessage", serializedMessage)
    req.write(serializedMessage);
    req.end();

    req.on('error', (error) => {
      console.error(`HTTP request error: ${error}`);
      callback(error, null);
    });
  },
};

global.myStates.status = status;
global.myStates.routes = routes;
global.myStates.comm = comm;

module.exports = {
  status: status,
  routes: routes,
  comm: comm,
  mem: mem,
  groups: groups,
  store: store,
  gossip: gossip,
};
