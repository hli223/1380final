#!/bin/bash
T_FOLDER=${T_FOLDER:-t}
R_FOLDER=${R_FOLDER:-}

cd "$(dirname "$0")/..$R_FOLDER" || exit 1


url="https://cs.brown.edu/courses/csci1380/sandbox/2/"
url="https://cs.brown.edu/courses/csci1380/sandbox/1/"

# curl -k $url | engine/getURLs.js $url
node engine/getURLs.js $url

# if $DIFF <(cat "$T_FOLDER"/d/d9.txt | c/getURLs.js $url | sort) <(sort "$T_FOLDER"/d/d10.txt) > /dev/null;
# then
#     echo "$0 success: URL sets are identical"
# else
#     echo "$0 failure: URL sets are not identical"
# fi