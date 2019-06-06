import { uuid, query, update, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
import { readFile } from 'fs-extra';
import { JSDOM } from 'jsdom';
import { URL } from 'url';
import path from 'path';
import { analyse } from '@lblod/marawa/rdfa-context-scanner';

const HARVEST_STATUS_NOT_STARTED = 'http://data.lblod.info/id/harvest-statuses/not-started';
const HARVEST_STATUS_ONGOING = 'http://data.lblod.info/id/harvest-statuses/ongoing';
const HARVEST_STATUS_SUCCESS = 'http://data.lblod.info/id/harvest-statuses/success';
const HARVEST_STATUS_FAILED = 'http://data.lblod.info/id/harvest-statuses/failed';

const FILE_BASE_DIR = '/share/';

const NAVIGATION_PREDICATES = [
  'http://data.lblod.info/vocabularies/besluitPublicatie/linkToPublications'
];
const SERVICE_URI = 'http://github.com/lblod/harvest-collector-service';

/**
 * Get all downloaded file addresses that have not yet been harvested.
 *
 * @return Array of file address URIs
*/
async function findAllFileAddressesToHarvest() {
  const result = await query(`
    PREFIX download: <http://mu.semte.ch/vocabularies/ext/download/>
    PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    PREFIX tmo: <http://www.semanticdesktop.org/ontologies/2008/05/20/tmo#>

    SELECT ?fileAddress
    WHERE {
      GRAPH ?g {
        ?download a download:DownloadTask ;
                    tmo:taskState <http://data.lblod.info/id/download-task-statuses/success> ;
                    tmo:taskSource ?fileAddress .

        FILTER NOT EXISTS { ?fileAddress harvest:status ?harvestStatus }
      }
    }
  `);

  console.log(`Found ${result.results.bindings.length} file addresses to harvest`);

  return result.results.bindings.map(b => b['fileAddress'].value);
}

/**
 * Harvests a single file address
*/
async function harvestFileAddress(fileAddress) {
  console.log(`Start harvesting file address <${fileAddress}>`);
  const collection = await ensureHarvestCollection(fileAddress);
  await processFileAddress(fileAddress, collection);
}

/**
 * Update the status to 'success' for harvest collections that only contain successfully harvested parts
*/
async function finishHarvestCollections() {
  await finishSuccessCollections();
  await finishFailedCollections();
}


// PRIVATE

/**
 * Ensure the given file address is attached to a harvest collection
 *
 * @return URI of the harvest collection
*/
async function ensureHarvestCollection(fileAddress) {
  const result = await query(`
    PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?collection
    WHERE {
      GRAPH ?g {
        ?collection a harvest:HarvestCollection ;
                      dct:hasPart <${fileAddress}> .
      }
    } LIMIT 1
  `);

  if (!result.results.bindings.length) {
    const collectionId = uuid();
    const collectionUri = `http://data.lblod.info/id/harvest-collections/${collectionId}`;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>
      PREFIX dct: <http://purl.org/dc/terms/>

      INSERT {
        GRAPH ?h {
          <${collectionUri}> a harvest:HarvestCollection ;
            mu:uuid ${sparqlEscapeString(collectionId)} ;
            harvest:status <${HARVEST_STATUS_ONGOING}> ;
            dct:hasPart <${fileAddress}> .
        }
      } WHERE {
        GRAPH ?g {
          <${fileAddress}> mu:uuid ?uuid .
          BIND(?g as ?h)
        }
      }
    `);
    // BIND(?g as ?h) is a hack for Virtuoso's "Function string_output_string needs a string output as argument 1..." error

    console.log(`Created new harvest collection <${collectionUri}> containing file address <${fileAddress}>`);
    return collectionUri;
  } else {
    const collectionUri = result.results.bindings[0]['collection'].value;
    console.log(`Attaching file address <${fileAddress}> to existing harvest collection <${collectionUri}>`);
    return collectionUri;
  }
}

/**
 * Harvest a file address and create additional resources to be downloaded/harvested if needed.
 * I.e. try to interpret the downloaded content as HTML, find navigation properties and trigger
 * new downloads for those URLs.
*/
async function processFileAddress(fileAddress, collection) {
  await updateHarvestStatus(fileAddress, HARVEST_STATUS_ONGOING);

  try {
    const result = await query(`
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

      SELECT ?file
      WHERE {
        GRAPH ?g {
          ?virtualFile nie:dataSource <${fileAddress}> .
          ?file nie:dataSource ?virtualFile .
        }
      } LIMIT 1
    `);

    if (result.results.bindings.length) {
      const filePath = result.results.bindings[0]['file'].value.replace('share://', FILE_BASE_DIR);
      const content = await readFile(filePath, 'utf8');
      const dom = new JSDOM(content);
      const topNode = dom.window.document.querySelector('body');
      const rdfaBlocks = analyse(topNode);
      console.log(`Found ${rdfaBlocks.length} blocks with an RDFa context`);

      let urls = [];
      for (let rdfaBlock of rdfaBlocks) {
        const newUrls = rdfaBlock.context.filter(function(t) {
          return NAVIGATION_PREDICATES.includes(t.predicate) && !urls.includes(t.object);
        }).map(t => t.object);
        urls = urls.concat(newUrls);
      }

      urls = await ensureFullUrls(urls, fileAddress);
      console.log(`Found ${urls.length} additional URLs that need to be harvested: ${JSON.stringify(urls)}`);
      await triggerNewDownloads(urls, fileAddress, collection);

      await updateHarvestStatus(fileAddress, HARVEST_STATUS_SUCCESS);
    } else {
      console.log(`No physical file found for file address <${fileAddress}>`);
      await updateHarvestStatus(fileAddress, HARVEST_STATUS_FAILED);
    }
  } catch (e) {
    console.log(`Something went wrong while processing file address <${fileAddress}>`);
    console.log(e);
    await updateHarvestStatus(fileAddress, HARVEST_STATUS_FAILED);
  }
}

/**
 * Make sure all URLs in the URL set are full URLs (i.e. starting with http(s)://)
 * Relative paths are resolved against the download URL of the given file address
 *
 * @return An array of full URLs
 */
async function ensureFullUrls(urls, fileAddress) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?url
    WHERE {
      <${fileAddress}> nie:url ?url .
    }
  `);

  if (!result.results.bindings.length)
    throw new Error(`File address <${fileAddress}> doesn't have a URL`);
  const parentUrl = new URL(result.results.bindings[0]['url'].value);

  return urls.map(function(url) {
    if (url.includes('://')) {
      return url; // URL is already a full URL
    } else if (url.startsWith('/')) {
      return `${parentUrl.protocol}//${parentUrl.host}${url}`;
    } else if (url.startsWith('./') || url.startsWith('../')) {
      return path.normalize(`${parentUrl.href}/${url}`);
    } else if (url.startsWith('#')) {
      return `${parentUrl}${url}`;
    } else {
      console.log(`Unable to ensure full URL for '${url}'`);
      return url;
    }
  });
}

/**
 * Triggers a new file address download for each URL in the set of URLs.
 * Each generated file address is attached to the given harvest collection
 * and has a reference to the file address it is derived from.
*/
async function triggerNewDownloads(urls, originatingfileAddress, collection) {
  for (let url of urls) {
    const fileAddressId = uuid();
    const fileAddressUri = `http://data.lblod.info/id/file-addresses/${fileAddressId}`;
    const downloadTaskId = uuid();
    const downloadTaskUri = `http://data.lblod.info/id/download-tasks/${downloadTaskId}`;

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX download: <http://mu.semte.ch/vocabularies/ext/download/>
      PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>
      PREFIX tmo: <http://www.semanticdesktop.org/ontologies/2008/05/20/tmo#>
      PREFIX prov: <http://www.w3.org/ns/prov#>

      INSERT {
        GRAPH ?h {
          <${fileAddressUri}> a nfo:WebDataObject ;
            mu:uuid ${sparqlEscapeString(fileAddressId)} ;
            nie:url <${url}> ;
            prov:wasGeneratedBy <${SERVICE_URI}> ;
            nie:isLogicalPartOf <${originatingfileAddress}> ;
            harvest:status <${HARVEST_STATUS_NOT_STARTED}> .
          <${downloadTaskUri}> a download:DownloadTask ;
            mu:uuid ${sparqlEscapeString(downloadTaskId)} ;
            tmo:taskSource <${fileAddressUri}> .
          <${collection}> dct:hasPart <${fileAddressUri}> .
        }
      } WHERE {
        GRAPH ?g {
          <${collection}> mu:uuid ?uuid .
        }

        BIND(?g as ?h)
      }
    `);
  }
  // BIND(?g as ?h) is a hack for Virtuoso's "Function string_output_string needs a string output as argument 1..." error
}

/**
 * Update the harvest collection status to 'success' if the collection only contain successfully harvested parts
*/
async function finishSuccessCollections() {
  const result = await query(`
    PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?collection (GROUP_CONCAT(DISTINCT ?childStatus; SEPARATOR=", ") AS ?status)
    WHERE {
      GRAPH ?g {
        ?collection a harvest:HarvestCollection ;
              harvest:status <${HARVEST_STATUS_ONGOING}> ;
              dct:hasPart ?fileAddress .
        ?fileAddress harvest:status ?childStatus .
      }
    }
    GROUP BY ?collection
    HAVING (COUNT(DISTINCT ?childStatus) = 1)
  `);

  const collections = result.results.bindings
        .filter(b => b['status'].value == HARVEST_STATUS_SUCCESS)
        .map(b => b['collection'].value);

  console.log(`Finished ${collections.length} harvest collections successfully`);

  for (let collection of collections) {
    await updateHarvestStatus(collection, HARVEST_STATUS_SUCCESS);
  }
}

/**
 * Update the harvest collection status to 'failed' if the harvesting of at least one part failed
*/
async function finishFailedCollections() {
  const result = await query(`
    PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?collection
    WHERE {
      GRAPH ?g {
        ?collection a harvest:HarvestCollection ;
              harvest:status <${HARVEST_STATUS_ONGOING}> ;
              dct:hasPart ?fileAddress .
        ?fileAddress harvest:status <${HARVEST_STATUS_FAILED}> .
      }
    }
  `);

  const collections = result.results.bindings.map(b => b['collection'].value);

  console.log(`Finished ${collections.length} harvest collections with a failure state`);

  for (let collection of collections) {
    await updateHarvestStatus(collection, HARVEST_STATUS_FAILED);
  }
}

/**
 * Updates the harvest status of the resource with the given URI.
*/
async function updateHarvestStatus(uri, status) {
  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX harvest: <http://mu.semte.ch/vocabularies/ext/harvest/>

    DELETE {
      GRAPH ?g {
        <${uri}> harvest:status ?status .
      }
    } WHERE {
      GRAPH ?g {
        <${uri}> harvest:status ?status .
      }
    }

    ;

    INSERT {
      GRAPH ?h {
        <${uri}> harvest:status <${status}> .
      }
    } WHERE {
      GRAPH ?g {
        <${uri}> mu:uuid ?uuid .
        BIND(?g as ?h)
      }
    }
  `);
  // BIND(?g as ?h) is a hack for Virtuoso's "Function string_output_string needs a string output as argument 1..." error
}

export {
  findAllFileAddressesToHarvest,
  harvestFileAddress,
  finishHarvestCollections
}
