const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const distribution = require('../distribution');
const id = distribution.util.id;
const promisify = require('../distribution/util/promisify');
const fs = require('fs');

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
}, 400000);

test('(25 pts) crawler workflow', (done) => {
  let m1 = async (key, baseUrl) => {
    await new Promise(resolve => setTimeout(resolve, 2000));
    if (baseUrl === undefined) {
      return {};
    }
    try {
      if (baseUrl.slice(-1) !== '/' && !baseUrl.endsWith('.txt') && !baseUrl.endsWith('.html')) {
        baseUrl += '/'
      }
    } catch (e) {
      console.log('error in m1: ' + key + ' ' + baseUrl + ' ', e);
      return { url: ['error in m1: ' + key + ' ' + baseUrl + ' ', e] };
    }

    let out = {};
    try {
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
      const response = await global.fetch(baseUrl);
      if (!response.ok) {
        // throw new Error(`HTTP error! status: ${response.status}`);
        return { ...out, [baseUrl]: `HTTP error! status: ${response.status}` };
      }
      var htmlContent = await response.text();
      htmlContent = htmlContent.replace("\u00a9", "&copy;")
      let urls = [];
      const dom = new global.JSDOM(htmlContent);
      const document = dom.window.document;

      console.log('baseUrl in map is: ', baseUrl);
      const anchors = document.querySelectorAll('a');

      anchors.forEach((anchor) => {
        const href = anchor.getAttribute('href');
        if (href) {
          var absoluteUrl = new URL(href, baseUrl).toString();
          if (absoluteUrl.endsWith('index.html')) {
            absoluteUrl = new URL(absoluteUrl + '/../').toString();
          }
          if (!baseUrl.startsWith(absoluteUrl) 
          && (absoluteUrl.startsWith('https://www.usenix.org/conference/') 
        && absoluteUrl.includes('/presentation/')) || absoluteUrl.startsWith('https://www.usenix.org/publications/proceedings?page=')) {
            urls.push(absoluteUrl);
          }


        }
      });
      out[baseUrl] = urls;
    } catch (error) {
      console.error(baseUrl + ' Fetch error: ', error);
      out = { ...out, [url]: 'Error fetching URL: ' + baseUrl + ' ' + error };
      // out = {}
    }
    return out;
  };


  function crawl(urlsInCurrentLevel) {
    console.log('level[currDepth]: ', urlsInCurrentLevel, currDepth);
    let urlsToBeStore = [];
    const urlKeys = [];
    const keyUrlsMap = {}
    urlsInCurrentLevel.forEach((url) => {
      if (url.endsWith('/')) {
        url = url.slice(0, -1);
      }
      // if (url.length > 0 && !(visited.has(url) || url.length < baseUrl.length && baseUrl.includes(url)) && url.includes(baseUrl)) {
      //   visited.add(url);
        const urlKey = 'url-' + id.getID(url);
        urlKeys.push(urlKey);
        keyUrlsMap[urlKey] = url;
        urlsToBeStore.push({ url: url, key: urlKey });
      // }
    });

    let cntr = 0;
    console.log('keyUrlsMap: ', keyUrlsMap)
    console.log('urlsToBeStore: ', urlsToBeStore, currDepth);
    if (urlsToBeStore.length === 0) {
      // console.log('allUrls: ', currDepth, visited.size, visited);
      // fs.writeFileSync('visited.txt', Array.from(visited).join('\n'));
      done();
      return;
    }
    urlsToBeStore.forEach((o) => {
      let key = o.key;
      let value = o.url;
      promisify(distribution.crawlUrl.store.put)(value, key)
        .then((v) => {
          cntr++;
          console.log('put urlsToBeStore:', value, key, v)
          if (cntr === urlsToBeStore.length) {
            console.log('urlsToBeStore store done! check urlsToBeStore', currDepth, urlsToBeStore, urlKeys)
            fs.writeFileSync('latest_urlKeys.json', JSON.stringify(urlKeys));
            levelCrawl(urlKeys);
          }
        })
        .catch((e) => {
          done(e);
        });
    });

  }
  const levelCrawl = (urlKeys) => {
    // console.log('start level crawl, level: ', currDepth, urlKeys);
    if (urlKeys === undefined || urlKeys.length === 0) {
      done();
    }
    promisify(distribution.crawlUrl.mr.exec)({ keys: urlKeys, map: m1, reduce: null, notStore: true, returnMapResult: true })
      .then((v) => {
        //v is a set
        if (v instanceof Error || (v && Object.keys(v).length > 0)) {
          console.log('map reduce errorr: ', v);
          done(v);
          return;
        }
        let totalElements = 0;
        v.forEach((urls) => {
          totalElements += urls.length;
        });
        console.log('Total elements in v: ', totalElements);
        try {
          currDepth++;
          const newUrls = []
          console.log('map reduce done!', v)
          v.forEach((urls) => {
            newUrls.push(...urls);
          });
          // levels.push(newUrls);
          console.log('start crawl() again')
          crawl(newUrls);
        } catch (e) {
          console.log('error in levelcrawl: ', e);
          done(e);
        }
      })
      .catch((e) => {
        done(e);
      });
  }
  var currDepth = 0;
  // const levels = []
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/books.txt'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg';
  // baseUrl = 'https://www.gutenberg.org/ebooks/'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/?C=N;O=D'//problemetic
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/?C=D;O=A'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/8/12380/?C=M;O=A'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/7/?C=N;O=D/'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/mesaerion-the-best-science-fiction-stories-1800-1849_983/index.html';//problemetic
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/category/books/default_15/';
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/the-book-of-mormon_571/index.html'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/the-book-of-mormon_571/'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/4/tag/truth/index.html';
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/1'
  var baseUrl = 'https://www.usenix.org/publications/proceedings'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/category/books/science-fiction_16'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/4/tag/authors/page/1'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/2/static/book1.txt'//text cannot be downloaded
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/level_2a'//this one is downloadable
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/category/books_1'
  // baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/'
  const crawlWithBaseUrl = () => {
    if (baseUrl.endsWith('/')) {
      baseUrl = baseUrl.slice(0, -1);
    }
    if (baseUrl.endsWith('index.html')) {
      baseUrl = new URL(baseUrl + '/../').toString();
    }
    // levels.push([baseUrl]);
    console.log('baseURL is ', baseUrl);
    crawl([baseUrl]);

  }
  try {
    var latestUrlKeys = require('../latest_urlkeys.json');
    if (!latestUrlKeys || latestUrlKeys.length === 0) {//starting from scratch
      console.log('latestUrlKeys is not defined, starting from scratch');
      crawlWithBaseUrl();
    } else {
      console.log('latestUrlKeys is defined, resuming from latestUrlKeys');
      // levels.push(latestUrlKeys);
      const batchSize = 10;
      if (latestUrlKeys.length > batchSize) {
        for (let i = 0; i < latestUrlKeys.length; i += batchSize) {
          const batch = latestUrlKeys.slice(i, i + batchSize);
          levelCrawl(batch);
        }
      } else {
        levelCrawl(latestUrlKeys);
      }
      // levelCrawl(latestUrlKeys);
    }
  } catch (e) {//starting from scratch
    console.log('error in reading latest_urlkeys.json: ', e);
    crawlWithBaseUrl();
  }


}, 800000);
