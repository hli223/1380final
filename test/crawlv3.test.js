const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const distribution = require('../distribution');
const id = distribution.util.id;
const fs = require('fs');

const groupsTemplate = require('../distribution/all/groups');


const crawlUrlGroup = {};
const downloadTextGroup = {};
const crawlUrlVisitedGroup = {};
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
for (let i = 1; i <= 10; i++) {
  nodes.push({ ip: '127.0.0.1', port: startPort + i });
}


beforeAll( (done) => {
  /* Stop the nodes if they are running */

  nodes.forEach(node => {
    crawlUrlGroup[id.getSID(node)] = node;
    downloadTextGroup[id.getSID(node)] = node;
    crawlUrlVisitedGroup[id.getSID(node)] = node;
    sourceSinkGroup[id.getSID(node)] = node;
    test1Group[id.getSID(node)] = node;
  });

  let cntr = 0;


  const startNodes = async (cb) => {
    console.log('start spawning nodes...')
    const spawnPromise = global.promisify(distribution.local.status.spawn);
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
          const crawlUrlVisitedConfig = { gid: 'crawlUrlVisited' };
          groupsTemplate(crawlUrlVisitedConfig).
            put(crawlUrlVisitedConfig, crawlUrlVisitedGroup, (e, v) => {
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

  const stopNodes = global.promisify(distribution.local.comm.send);

  Promise.all(nodes.map(node => {
    remote.node = node;
    return stopNodes([], remote);
  })).then(() => {
    console.log('all nodes stopped!');
    localServer.close();
    done();
  });
}, 400000);

test('(25 pts) crawler workflow', (done) => {

    let m1 = async (gid, baseUrl) => {
        let out = {}
        try {
            if (baseUrl.slice(-1) !== '/' && !baseUrl.endsWith('.txt') && !baseUrl.endsWith('.html')) {
            baseUrl += '/'
            }
        } catch (e) {
            console.log('error in m1: ' + baseUrl + ' ', e);
            return { url: ['error in m1: ' + baseUrl + ' ', e] };
        }
        try {
            process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
            const response = await global.fetch(baseUrl);
            if (!response.ok) {
            return { ...out, [baseUrl]: `HTTP error! status: ${response.status}` };
            }
            var htmlContent = await response.text();
            htmlContent = htmlContent.replace("\u00a9", "&copy;")
            const dom = new global.JSDOM(htmlContent);
            const document = dom.window.document;

            console.log('baseUrl in map is: ', baseUrl, gid);
            const anchors = document.querySelectorAll('a');

            

            let promises = [];
            let urls = [];
            anchors.forEach( (anchor) => {
                const href = anchor.getAttribute('href');
                if (href) {
                    var absoluteUrl = new URL(href, baseUrl).toString();
                    if (absoluteUrl.endsWith('index.html')) {
                    absoluteUrl = new URL(absoluteUrl + '/../').toString();
                    }
                    if (!baseUrl.startsWith(absoluteUrl) 
                    && (absoluteUrl.startsWith('https://www.usenix.org/conference/') 
                && absoluteUrl.includes('/presentation/'))){
                    urls.push(absoluteUrl);
                    }
                }
            });
            out[baseUrl] = urls;
            return out;
        } catch (error) {
            console.error(baseUrl + ' Fetch error: ', error);
            out = { ...out, [baseUrl]: 'Error fetching URL: ' + baseUrl + ' ' + error };
            return out
        }
        return out


    }
    var baseUrl = 'https://www.usenix.org/publications/proceedings?page='
    let promises = [];
    let urlKeys = []
    let execMr = global.promisify(distribution.crawlUrl.mr.exec)
    for (let i = 1; i<=100; i++) {
        let url = baseUrl + i;
        let urlKey = id.getID(url);
        urlKeys.push(urlKey);
        promises.push(
            global.promisify(distribution.crawlUrl.store.put)(url, urlKey)
        );
    }
    Promise.all(promises).then(async () => {
        for (let urlKey of urlKeys) {
            try {
                await execMr({ keys: [urlKey], map: m1, reduce: null, notStore: true, returnMapResult: true, notShuffle: true });
            } catch (error) {
                console.error('Error in execMr: ', error);
            }
        }
        done();
    }).catch((error) => {
        done(error);
    });




    




}, 800000);