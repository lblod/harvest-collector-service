const $rdflib = require('rdflib')
const fs = require('fs')
const file = "/Users/bittichn/app-lblod-harvester/data/files/0b8c1fd0-37df-11ec-940a-6b702a3d8296.html";
const content = fs.readFileSync(file, 'utf-8');
const contentType = "text/html";
const baseUrl = "http://placeholder.com";
let store = $rdflib.graph();
$rdflib.parse(content, store, baseUrl, contentType);
$rdflib.serialize(undefined, store, undefined, 'text/turtle', function (err, str) {
    // do whatever you want, the data is in the str variable.
})
