const startPort = 8000;
global.nodeConfig = {ip: '127.0.0.1', port: startPort};
const { url } = require('inspector');
const distribution = require('../distribution');
const id = distribution.util.id;

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
for (let i = 1; i <= 3; i++) {
  nodes.push({ip: '127.0.0.1', port: startPort + i});
}


beforeAll((done) => {
  /* Stop the nodes if they are running */

  nodes.forEach(node => {
    crawlUrlGroup[id.getSID(node)] = node;
    downloadTextGroup[id.getSID(node)] = node;
    invertedIdxGroup[id.getSID(node)] = node;
    sourceSinkGroup[id.getSID(node)] = node;
    test1Group[id.getSID(node)] = node;
  });

  let cntr = 0;
  const startNodes = (cb) => {
    nodes.forEach(node => {
      distribution.local.status.spawn(node, (e, v) => {
        // Handle the callback
        cntr++;
        if (cntr === nodes.length) {
          console.log('all nodes started!');
          cb();
        }
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const crawlUrlConfig = {gid: 'crawlUrl'};
    startNodes(() => {
      groupsTemplate(crawlUrlConfig).put(crawlUrlConfig, crawlUrlGroup, (e, v) => {
        const downloadTextConfig = {gid: 'downloadText'};
        groupsTemplate(downloadTextConfig).put(downloadTextConfig, downloadTextGroup, (e, v) => {
          const invertedIdxConfig = {gid: 'invertedIdx'};
          groupsTemplate(invertedIdxConfig).
              put(invertedIdxConfig, invertedIdxGroup, (e, v) => {
                const sourceSinkConfig = {gid: 'sourceSink'};
                groupsTemplate(sourceSinkConfig).
                    put(sourceSinkConfig, sourceSinkGroup, (e, v) => {
                      const test1Config = {gid: 'test1'};
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
});

afterAll((done) => {
  let cntr = 0;
  const remote = {service: 'status', method: 'stop'};
  nodes.forEach(node => {
    remote.node = node;
    distribution.local.comm.send([], remote, (e, v) => {
      // Handle the callback
      cntr++;
      if (cntr === nodes.length) {
        console.log('all nodes stopped!');
        localServer.close();
        done();
      }
    });
  });
});

test('(25 pts) downloadText workflow', (done) => {
  let m1 = (key, string) => {
    const regex = new RegExp('abc');
    let out = {};
    const outputKey = global.distribution.util.id.getID(string);
    try {
      if (regex.test(string)) {
        out[outputKey] = key;
      }
    } catch (error) {
      console.error('Matching error: ', error);
      out[outputKey] = 'Error matching string: ' + string + error;
    }
    return out;
  };


  const downloadText = (cb) => {
    distribution.crawlUrl.store.get(null, (e, urlKeys) => {
        if (Object.keys(e).length > 0) {
          console.log('errors fetching urlKeys', e);
          done();
        }

      

    //   distribution.downloadText.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
    //     try {
    //       const regex = new RegExp('abc');
    //       expect(v.length).toBe(dataset.filter((o) =>
    //         regex.test(Object.values(o)[0])).length);
    //       console.log('e, v for string matching: ', e, v);
    //       done();
    //     } catch (e) {
    //       done(e);
    //     }
    //   });
    });
  };
  downloadText();

}, 1600000);

