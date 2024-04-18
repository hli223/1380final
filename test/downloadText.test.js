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
  let m1 = async (key, url) => {
    let out = {};
    try {
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
        const response = await global.fetch(url);

        let contentKey = 'content'+global.distribution.util.id.getID(url);
        if (!response.ok) {
            out[contentKey] = 'HTTP error! status: '+response.status;
            return out;
        }
        var htmlContent = await response.text();
        // htmlContent = global.convertToText(htmlContent);
        htmlContent = htmlContent.replace("\u00a9", "&copy;")
        // htmlContent = htmlContent.replace(/\u201C/g, ' ');
        // htmlContent = htmlContent.replace(/\u201D/g, ' ');
        htmlContent = htmlContent.replace(/[^a-zA-Z0-9\s]/g, ' ');
        out[contentKey] = htmlContent;

    } catch (e) {
        console.error(url+'Fetch error: ', e);
        out[contentKey] = 'Error fetching URL: '+url + ' ' + e;
    }
    return out;
  };


  const downloadText = (cb) => {
    distribution.crawlUrl.store.get(null, (e, urlKeys) => {
        if (Object.keys(e).length > 0) {
          console.log('errors fetching urlKeys', e);
          done();
        }
    console.log('Retrieved all url keys, number of keys: ', urlKeys.length, urlKeys.slice(0, 1));
    distribution.crawlUrl.mr.exec({keys: urlKeys.slice(0, 1), map: m1, reduce: null, storeGroup:'downloadText'}, (e, v) => {
        if (e!==null && Object.keys(e).length > 0) {
            console.log('downloadText errorr: ', e);
            done(e);
            return;
        }
        console.log('download Text success!', v);
        done();
    });


    });
  };
  downloadText();

}, 5000);

