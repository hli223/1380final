const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const distribution = require('../distribution');
const id = distribution.util.id;
const promisify = require('../distribution/util/promisify');

const groupsTemplate = require('../distribution/all/groups');


const crawlUrlGroup = {};
const downloadTextGroup = {};
const invertedIdxGroup = {};
const sourceSinkGroup = {};
const test1Group = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const nodes = [];
for (let i = 1; i <= 6; i++) {
  nodes.push({ ip: '127.0.0.1', port: startPort + i });
}


beforeAll( (done) => {
  /* Stop the nodes if they are running */

  nodes.forEach(node => {
    crawlUrlGroup[id.getSID(node)] = node;
    downloadTextGroup[id.getSID(node)] = node;
    invertedIdxGroup[id.getSID(node)] = node;
    sourceSinkGroup[id.getSID(node)] = node;
    test1Group[id.getSID(node)] = node;
  });

  let cntr = 0;


  const startNodes = async (cb) => {
    console.log('start spawning nodes...')
    const spawnPromise = promisify(distribution.local.status.spawn);
    for (const node of nodes) {
        await spawnPromise(node);
    }
    console.log('node started!')
    cb();
  };

  distribution.node.start((server) => {
    localServer = server;

    const crawlUrlConfig = { gid: 'crawlUrl' };
    startNodes(() => {
      groupsTemplate(crawlUrlConfig).put(crawlUrlConfig, crawlUrlGroup, (e, v) => {
        const downloadTextConfig = { gid: 'downloadText' };
        groupsTemplate(downloadTextConfig).put(downloadTextConfig, downloadTextGroup, (e, v) => {
          const invertedIdxConfig = { gid: 'invertedIdx' };
          groupsTemplate(invertedIdxConfig).
            put(invertedIdxConfig, invertedIdxGroup, (e, v) => {
              const sourceSinkConfig = { gid: 'sourceSink' };
              groupsTemplate(sourceSinkConfig).
                put(sourceSinkConfig, sourceSinkGroup, (e, v) => {
                  const test1Config = { gid: 'test1' };
                  groupsTemplate(test1Config).
                    put(test1Config, test1Group, (e, v) => {
                      done();
                    });
                });
            });
        });
      });
    });
  });
}, 400000);

afterAll((done) => {
  const remote = { service: 'status', method: 'stop' };

  const stopNodes = promisify(distribution.local.comm.send);

  Promise.all(nodes.map(node => {
    remote.node = node;
    return stopNodes([], remote);
  })).then(() => {
    console.log('all nodes stopped!');
    localServer.close();
    done();
  });
});


function sanityCheck(mapper, reducer, dataset, expected, done) {
  let mapped = dataset.map((o) =>
    mapper(Object.keys(o)[0], o[Object.keys(o)[0]]));
  /* Flatten the array. */
  mapped = mapped.flat();
  let shuffled = mapped.reduce((a, b) => {
    let key = Object.keys(b)[0];
    if (a[key] === undefined) a[key] = [];
    a[key].push(b[key]);
    return a;
  }, {});
  let reduced = Object.keys(shuffled).map((k) => reducer(k, shuffled[k]));

  try {
    expect(reduced).toEqual(expect.arrayContaining(expected));
  } catch (e) {
    done(e);
  }
}

// ---all.mr---

test('(25 pts) all.mr:crawlUrl', (done) => {
  let m1 = (key, value) => {
    let words = value.split(/(\s+)/).filter((e) => e !== ' ');
    let out = {};
    out[words[1]] = parseInt(words[3]);
    return out;
  };

  let r1 = (key, values) => {
    let out = {};
    out[key] = values.reduce((a, b) => Math.max(a, b), -Infinity);
    return out;
  };

  let dataset = [
    {'000': '006701199099999 1950 0515070049999999N9 +0000 1+9999'},
    {'106': '004301199099999 1950 0515120049999999N9 +0022 1+9999'},
    {'212': '004301199099999 1950 0515180049999999N9 -0011 1+9999'},
    {'318': '004301265099999 1949 0324120040500001N9 +0111 1+9999'},
    {'424': '004301265099999 1949 0324180040500001N9 +0078 1+9999'},
  ];

  let expected = [{'1950': 22}, {'1949': 111}];

  /* Sanity check: map and reduce locally */
  sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    const storeGetPromise = promisify(distribution.crawlUrl.store.get);
    storeGetPromise(null)
      .then((v) => {
        try {
          expect(v.length).toBe(dataset.length);
        } catch (e) {
          done(e);
        }

        const mrExecPromise = promisify(distribution.crawlUrl.mr.exec);
        mrExecPromise({keys: v, map: m1, reduce: r1})
          .then((v) => {
            try {
              expect(v).toEqual(expect.arrayContaining(expected));
              done();
            } catch (e) {
              done(e);
            }
          });
      });
  };

  // We send the dataset to the cluster
  const storePutPromise = promisify(distribution.crawlUrl.store.put);
  Promise.all(dataset.map(async (o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    await storePutPromise(value, key);
  })).then(() => {
    doMapReduce(done);
  });
});


