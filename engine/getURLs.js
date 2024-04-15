#!/usr/bin/env node

const readline = require('readline');
const {JSDOM} = require('jsdom');
const {URL} = require('url');

// Example usage:

const url = process.argv[2];
let baseURL = '';
if (url.slice(-5)=='.html') {
  baseURL = url.slice(0, -10);
} else if (url.slice(-1)!='/') {
  baseURL = url + '/';
} else {
  baseURL = url;
}

const rl = readline.createInterface({
  input: process.stdin,
});

// TODO some code
// console.log(baseURL)

const extractUrls = (htmlContent, baseUrl) => {
  const dom = new JSDOM(htmlContent);
  const document = dom.window.document;

  const urls = [];
  const anchors = document.querySelectorAll('a');

  anchors.forEach((anchor) => {
    const href = anchor.getAttribute('href');
    if (href) {
      const absoluteUrl = new URL(href, baseUrl).toString();
      urls.push(absoluteUrl);
    }
  });

  return urls;
};


const printedUrls = new Set();

rl.on('line', (line) => {
  // TODO some code
  const urls = extractUrls(line, baseURL);
  urls.forEach((url) => {
    if (!printedUrls.has(url)) {
      console.log(url);
      printedUrls.add(url);
    }
  });
});

