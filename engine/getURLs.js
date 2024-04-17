// Example usage:
const cheerio = require('cheerio');
const fs = require('fs');
const url = process.argv[2];
let baseURL = '';
if (url.slice(-5)==='.html') {
  baseURL = url.slice(0, -10);
} else if (url.slice(-1)!=='/') {
  baseURL = url + '/';
} else {
  baseURL = url;
}
process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;

  // Empty the urls.txt file if it exists
if (fs.existsSync('urls.txt')) {
  fs.truncateSync('urls.txt', 0);
}

async function crawl(url) {
  const visited = new Set();
  const queue = [{ url, depth: 0, parent: null}];
  console.log('start running', queue.length)

  while (queue.length > 0) {
    const { url, depth, parent } = queue.shift();

    if (visited.has(url) || url.length < baseURL.length && baseURL.includes(url)) {
      continue;
    } 

    visited.add(url);
    console.log(url);

    fs.appendFileSync('urls.txt', url + '\n');

    const response = await fetch(url);
    const html = await response.text();

    const $ = cheerio.load(html);

    $('a').each((i, el) => {
      const href = $(el).attr('href');
      if (href) {
        // console.log(href, parent)
        const absoluteUrl = new URL(href, url).toString();
        queue.push({ url: absoluteUrl, depth: depth + 1, parent: url });
      }
    });
  }
}

crawl(baseURL);

