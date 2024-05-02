const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const { url } = require('inspector');
const distribution = require('../distribution');
const id = distribution.util.id;
// const ipAddresses = ['18.225.175.3', '3.142.135.227']
const ipAddresses = ['127.0.0.1', '127.0.0.1']

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

for (let i = 1; i <= 1; i++) {
  nodes.push({ ip: '127.0.0.1', port: startPort + i });

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
  let cntr = 0;
  const remote = { service: 'status', method: 'stop' };
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
  let m1 = async (key, url) => {
    let out = {};
    if (!url) {
      return {};
    }
    let contentKey = 'content-' + global.distribution.util.id.getID(url);
    try {
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
      const response = await global.fetch(url);

      if (!response.ok) {
        out[contentKey] = 'HTTP error! status: ' + response.status;
        return out;
      }
      var htmlContent = await response.text();
      htmlContent = htmlContent.replace("\u00a9", "&copy;")
      // htmlContent = htmlContent.replace(/\u201C/g, ' ');
      // htmlContent = htmlContent.replace(/\u201D/g, ' ');
      const dom = new global.JSDOM(htmlContent);
      htmlContent = dom.window.document.body.textContent;
      let lines = htmlContent.split('\n');
      htmlContent = lines.join(' ').replace(/\s{2,}/g, ' ').replace(/[^a-zA-Z0-9]/g, ' ');
      out[contentKey] = { url: url, htmlContent: htmlContent };

    } catch (e) {
      console.error(url + 'Fetch error: ', e);
      out[contentKey] = { url: url, htmlContent: 'Error fetching URL: ' + url + ' ' + e };
    }
    return out;
  };
  const testStartTime = Date.now();

  const downloadText = async (cb) => {
    let urlKeys;
    try {
      urlKeys = await global.promisify(distribution.crawlUrl.store.get)(null);
      console.log('Retrieved all url keys, number of keys: ', urlKeys.length);
    } catch (e) {
      console.error('Error fetching urlKeys', e);
      done(e);
    }

    let execMr = global.promisify(distribution.crawlUrl.mr.exec)

    let batchSize = 20;

    let totalNumKeys = urlKeys.length;
    for (let i = 0; i < totalNumKeys; i += batchSize) {
      if (i + batchSize > totalNumKeys) {
        batchSize = totalNumKeys - i;
      }
      let batch = urlKeys.slice(i, i + batchSize);
      console.log('batch: ', batch, i, i + batchSize, totalNumKeys);
      try {
        await execMr({ keys: batch, map: m1, reduce: null, storeGroup: 'downloadText', intermediateStore: 'store' });//intermediateStore specify where to store intermediate values (store for disk, mem for memory)
      } catch (err) {
        console.error('downloadText errorr: ', err, err.stack);
        done(err);
      }
    }
    if (totalNumKeys % batchSize !== 0) {
      let lastBatch = urlKeys.slice(-totalNumKeys % batchSize);
      console.log('lastBatch: ', lastBatch, totalNumKeys % batchSize);
      try {
        await execMr({ keys: lastBatch, map: m1, reduce: null, storeGroup: 'downloadText', intermediateStore: 'store' });
      } catch (err) {
        console.error('downloadText errorr: ', err.stack);
        done(err);
      }
    }

  };
  downloadText().then(() => {
    const testEndTime = Date.now();
    const testDuration = testEndTime - testStartTime;
    console.log(`Test execution time (excluding setup and teardown): ${testDuration}ms`);
    done();
  }).catch(err => {
    const testEndTime = Date.now();
    const testDuration = testEndTime - testStartTime;
    console.log(`Test execution time (excluding setup and teardown): ${testDuration}ms`);
    console.error('Error in downloadText: ', err);
    done(err);
  });

}, 5000000);

