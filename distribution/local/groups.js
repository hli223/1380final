const id = require('../util/id');
const groups = {
  localGroup: {},
};

groups.get = function(groupName, callback) {
  callback = callback || function() { };
  if (groupName in this.localGroup) {
    callback(null, this.localGroup[groupName]);
    return this.localGroup[groupName];
  } else {
    callback(new Error('Group not found'), null);
  }
};

groups.put = function(groupName, nodes, callback) {
  callback = callback || function() {};
  global.distribution[groupName] = {};
  let context = {};
  context.gid = groupName || 'all';
  global.distribution[groupName].comm = require('../all/comm.js')(context);
  global.distribution[groupName].status = require('../all/status.js')(context);
  global.distribution[groupName].groups = require('../all/groups.js')(context);
  global.distribution[groupName].routes =
    require('../all/routes.js')(context);
  // global.distribution[cgroupName].gossip
  // = require("../all/gossip.js")(context)
//   console.log('putting!', groupName, nodes);
  if (nodes === null) {
    this.localGroup[groupName] = {};
    callback(null, {});
  } else {
    this.localGroup[groupName] = nodes;
    callback(null, nodes);
  }
};
groups.add = function(groupName, node, callback) {
  callback = callback || function() { };
  if (groupName in this.localGroup) {
    this.localGroup[groupName][id.getSID(node)] = node;

    callback(null, 'Node added to group successfully');
  } else {
    callback(new Error('Group not found'), null);
    // if groupName does not exist, no effect
  }
};
groups.rem = function(groupName, nodeSID, callback) {
  callback = callback || function() { };
  if (groupName in this.localGroup) {
    if (nodeSID in this.localGroup[groupName]) {
      delete this.localGroup[groupName][nodeSID];

      callback(null, 'Successfully deletes');
    } else {
      // if nodeSID does not exist, no effect

      callback(new Error('sid not found'), null);
    }
  } else {
    callback(new Error('Group not found'), null);
  }
  // if groupName does not exist, no effect
};

groups.del = function(groupName, callback) {
  if (groupName in this.localGroup) {
    let deleted = this.localGroup[groupName];
    delete this.localGroup[groupName];
    callback(null, deleted);
  } else {
    callback(new Error('Group not found'), null);
  }
};

module.exports = groups;