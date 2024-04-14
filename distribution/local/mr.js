// const id = require('../util/id');
const mr = {};

mr.deleteService = function(serviceName, callback) {
  callback = callback || function() { };
  if (serviceName in global.distribution[context.gid]) {
    delete global.distribution[context.gid][serviceName];
    callback(null, serviceName+' deleted!');
  } else {
    callback(new Error(serviceName+' not found'), null);
  }
};


module.exports = mr;
