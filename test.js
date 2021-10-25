var $rdf = require('rdflib');
var fs = require('fs');

var filename = "/Users/bittichn/app-lblod-harvester/data/files/0cbba7c0-3250-11ec-a882-85a611d4f64c.html";
var rdfData = fs.readFileSync(filename).toString();
var store = $rdf.graph();
var contentType = 'text/html';
var baseUrl = "http://placeholder.com";
const NAVIGATION_PREDICATES = [
    'http://lblod.data.gift/vocabularies/besluit/linkToPublication'
];

try {
    $rdf.parse(rdfData, store, baseUrl, contentType);
    var stmts = store.statementsMatching(undefined, undefined, undefined);
    const objects = stmts.filter(stmt => NAVIGATION_PREDICATES.includes(stmt.predicate.value))
                        .map(stmt => stmt.object.value);
    const urls = [...new Set(objects)];
    console.log(urls);
} catch (err) {
    console.log(err);
}



