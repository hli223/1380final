const assert = require('assert');
var crypto = require('crypto');

// The ID is the SHA256 hash of the JSON representation of the object
function getID(obj) {
  const hash = crypto.createHash('sha256');
  hash.update(JSON.stringify(obj));
  return hash.digest('hex');
}

// The NID is the SHA256 hash of the JSON representation of the node
function getNID(node) {
  node = {ip: node.ip, port: node.port};
  return getID(node);
}

// The SID is the first 5 characters of the NID
function getSID(node) {
  return getNID(node).substring(0, 5);
}


function idToNum(id) {
  let n = parseInt(id, 16);
  assert(!isNaN(n), 'idToNum: id is not in KID form!');
  return n;
}

function naiveHash(kid, nids) {
  nids.sort();
  return nids[idToNum(kid) % nids.length];
}

function consistentHash(kid, nids) {
  nids = nids.sort();
  const kidNum = idToNum(kid);
  for (let i = 0; i < nids.length; i++) {
    if (idToNum(nids[i]) > kidNum) {
      return nids[i];
    }
  }
  return nids[0];
}


function rendezvousHash(kid, nids) {
  let highestScore = -1;
  let selectedNID = null;
  nids.forEach((nid) => {
    const combinedID = getID(kid+nid);
    const score = idToNum(combinedID);
    if (score > highestScore) {
      highestScore = score;
      selectedNID = nid;
    }
  });
  return selectedNID;
}

module.exports = {
  getNID: getNID,
  getSID: getSID,
  getID: getID,
  idToNum: idToNum,
  naiveHash: naiveHash,
  consistentHash: consistentHash,
  rendezvousHash: rendezvousHash,
};