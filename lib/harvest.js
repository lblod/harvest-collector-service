import { uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { readFile } from 'fs-extra';
import { JSDOM } from 'jsdom';
import { URL } from 'url';
import path from 'path';
import { analyse } from '@lblod/marawa/rdfa-context-scanner';

const HARVEST_STATUS_NOT_STARTED = 'http://lblod.data.gift/collecting-statuses/not-started';
const HARVEST_STATUS_ONGOING = 'http://lblod.data.gift/collecting-statuses/ongoing';
const HARVEST_STATUS_WAITING_FOR_NEXT_DOWNLOAD = 'http://lblod.data.gift/collecting-statuses/waiting-for-next-download';
const HARVEST_STATUS_COMPLETED = 'http://lblod.data.gift/collecting-statuses/completed';
const HARVEST_STATUS_FAILED = 'http://lblod.data.gift/collecting-statuses/failed';

const REMOTE_READY_STATUS = 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached';
const REMOTE_SUCCESS_STATUS = 'http://lblod.data.gift/file-download-statuses/success';
const REMOTE_COLLECTED_STATUS = 'http://lblod.data.gift/file-download-statuses/collected';

const TASK_READY_STATUS = 'http://lblod.data.gift/harvesting-statuses/ready-for-collecting';
const TASK_STATUS_READY_FOR_IMPORTING = 'http://lblod.data.gift/harvesting-statuses/ready-for-importing'
const TASK_STATUS_FAILED = 'http://lblod.data.gift/harvesting-statuses/failed'

const FILE_BASE_DIR = '/share/';

const NAVIGATION_PREDICATES = [
  'http://data.lblod.info/vocabularies/lblod/linkToPublication'
];
const SERVICE_URI = 'http://github.com/lblod/harvest-collector-service';

/**
 * Get all downloaded files that have not yet been harvested.
 *
 * @return Array of file address URIs
*/
async function findAllFilesToHarvest() {
  const result = await query(`
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?remoteDataObject
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?harvestingCollection a harvesting:HarvestingCollection ;
          adms:status ?harvestStatus ;
          dct:hasPart ?remoteDataObject .
        ?remoteDataObject adms:status ${sparqlEscapeUri(REMOTE_SUCCESS_STATUS)} .
      }

      FILTER( ?harvestStatus IN (${sparqlEscapeUri(HARVEST_STATUS_NOT_STARTED)}, ${sparqlEscapeUri(HARVEST_STATUS_WAITING_FOR_NEXT_DOWNLOAD)})) .
      FILTER NOT EXISTS { ?remoteDataObject adms:status ${sparqlEscapeUri(REMOTE_COLLECTED_STATUS)} } .
    }
  `);
  console.log(`Found ${result.results.bindings.length} files to harvest`);

  return result.results.bindings.map(b => b['remoteDataObject'].value);
}

/**
 * Harvests a single file
*/
async function harvestFile(file) {
  console.log(`Start harvesting file <${file}>`);
  const collection = await getHarvestingCollection(file);
  await processFile(file, collection);
}

// PRIVATE

/**
 * Get the URI of a harvest collection
 *
 * @return URI of the harvest collection
*/
async function getHarvestingCollection(file) {
  const result = await query(`
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?harvestingCollection
    WHERE {
      GRAPH <http://mu.semte.ch/graphs/public> {
        ?harvestingCollection a harvesting:HarvestingCollection ;
            dct:hasPart <${file}> .
      }
    } LIMIT 1
  `);

  const collectionUri = result.results.bindings[0]['harvestingCollection'].value;
  return collectionUri;
}

/**
 * Harvest a file and create additional resources to be downloaded/harvested if needed.
 * I.e. try to interpret the downloaded content as HTML, find navigation properties and trigger
 * new downloads for those URLs.
*/
async function processFile(file, collection) {
  await updateHarvestStatus(collection, HARVEST_STATUS_ONGOING);

  try {
    const physicalFile = await getPhysicalFile(file);
    if (physicalFile) {
      const urls = await getLinkedUrls(physicalFile, file);
      console.log(`Found ${urls.length} additional URLs that need to be harvested: ${JSON.stringify(urls)}`);

      await updateHarvestStatus(file, REMOTE_COLLECTED_STATUS);

      if (urls.length) {
        // There are navigation links on the page, triggering now download for those
        await triggerNewDownloads(urls, file, collection);
        await updateHarvestStatus(collection, HARVEST_STATUS_WAITING_FOR_NEXT_DOWNLOAD);
      } else if (!(await collectionWaitingForNextDownload(collection))) {
        // No navigation links found and the collection isn't waiting for next download, the task is done
        const task = await getTask(collection);
        await updateHarvestStatus(collection, HARVEST_STATUS_COMPLETED);
        await updateHarvestStatus(task, TASK_STATUS_READY_FOR_IMPORTING);
      }
    } else {
      console.log(`No physical file found for file <${file}>`);
      const task = await getTask(collection);
      await updateHarvestStatus(collection, HARVEST_STATUS_FAILED);
      await updateHarvestStatus(task, TASK_STATUS_FAILED);
    }
  } catch (e) {
    console.log(`Something went wrong while processing file <${file}>`);
    console.log(e);
    const task = await getTask(collection);
    await updateHarvestStatus(collection, HARVEST_STATUS_FAILED);
    await updateHarvestStatus(task, TASK_STATUS_FAILED);
  }
}

/**
 * Make sure all URLs in the URL set are full URLs (i.e. starting with http(s)://)
 * Relative paths are resolved against the download URL of the given file address
 *
 * @return An array of full URLs
 */
async function ensureFullUrls(urls, file) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?url
    WHERE {
      <${file}> nie:url ?url .
    }
  `);

  if (!result.results.bindings.length)
    throw new Error(`File <${file}> doesn't have a URL`);

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
 * Triggers a new file download for each URL in the set of URLs.
 * Each generated file is attached to the given harvest collection
 * and has a reference to the file it is derived from.
*/
async function triggerNewDownloads(urls, originatingfile, collection) {
  for (let url of urls) {
    const remoteDataObjectId = uuid();
    const remoteDataObjectUri = `http://data.lblod.info/id/remote-data-objects/${remoteDataObjectId}`;
    const timestamp = new Date();

    await update(`
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX adms: <http://www.w3.org/ns/adms#>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX rpioHttp: <http://redpencil.data.gift/vocabularies/http/>
      PREFIX http: <http://www.w3.org/2011/http#>

      INSERT DATA {
        GRAPH <http://mu.semte.ch/graphs/public> {
          ${sparqlEscapeUri(collection)} dct:hasPart ${sparqlEscapeUri(remoteDataObjectUri)} .

          ${sparqlEscapeUri(remoteDataObjectUri)} a nfo:RemoteDataObject, nfo:FileDataObject;
            rpioHttp:requestHeader <http://data.lblod.info/request-headers/accept/text/html>;
            mu:uuid ${sparqlEscapeString(remoteDataObjectId)};
            nie:url ${sparqlEscapeUri(url)};
            dct:creator ${sparqlEscapeUri(SERVICE_URI)};
            adms:status ${sparqlEscapeUri(REMOTE_READY_STATUS)};
            dct:created ${sparqlEscapeDateTime(timestamp)};
            dct:modified ${sparqlEscapeDateTime(timestamp)}.

          <http://data.lblod.info/request-headers/accept/text/html> a http:RequestHeader;
            http:fieldValue "text/html";
            http:fieldName "Accept";
            http:hdrName <http://www.w3.org/2011/http-headers#accept>.
        }
      }
    `);
  }
}

/**
 * Updates the harvest status of the resource with the given URI.
*/
async function updateHarvestStatus(uri, status) {
  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>

    DELETE {
      GRAPH ?g {
        <${uri}> adms:status ?status .
      }
    } WHERE {
      GRAPH ?g {
        <${uri}> adms:status ?status .
      }
    }

    ;

    INSERT {
      GRAPH ?h {
        <${uri}> adms:status <${status}> .
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

/**
 * Gets the task associated to a collection
*/
async function getTask(collection) {
  const taskResult = await query(`
    PREFIX prov: <http://www.w3.org/ns/prov#>
    SELECT ?task
    WHERE {
      GRAPH ?g {
        ?task prov:generated ${sparqlEscapeUri(collection)}.
      }
    }
  `);
  return taskResult.results.bindings[0]['task'].value;
}

/**
 * Gets the physical file associated to a virtual file
*/
async function getPhysicalFile(file) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?physicalFile
    WHERE {
      GRAPH ?g {
        ?physicalFile nie:dataSource <${file}> .
      }
    } LIMIT 1
  `);

  if (result.results.bindings.length)
    return result.results.bindings[0]['physicalFile'].value;

  return null;
}

/**
 * Gets the rdfa blocks obtained when parsing the content of a file
*/
async function getRdfaBlocks(physicalFile) {
  const filePath = physicalFile.replace('share://', FILE_BASE_DIR);
  const content = await readFile(filePath, 'utf8');
  const dom = new JSDOM(content);
  const topNode = dom.window.document.querySelector('body');
  return analyse(topNode);
}

/**
 * Gets the URLs linking to other documents present in the content of a given file
*/
async function getLinkedUrls(physicalFile, file) {
  const rdfaBlocks = await getRdfaBlocks(physicalFile);
  console.log(`Found ${rdfaBlocks.length} RDFa blocks`);

  let urls = [];
  for (let rdfaBlock of rdfaBlocks) {
    const newUrls = rdfaBlock.context.filter(function(t) {
      return NAVIGATION_PREDICATES.includes(t.predicate) && !urls.includes(t.object);
    }).map(t => t.object);
    urls = urls.concat(newUrls);
  }

  return await ensureFullUrls(urls, file);
}

/**
 * Returns whether the passed URI is a RemoteDataObject or not
 *
 * @param String uri Uri of the object to test
*/
async function isRemoteDataObject(uri) {
  const result = await query(`
    SELECT ?g
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#RemoteDataObject> .
      }
    } LIMIT 1
  `);

  if (result.results.bindings.length)
    return uri;

  return undefined;
}

async function collectionWaitingForNextDownload(collection) {
  const result = await query(`
    PREFIX adms: <http://www.w3.org/ns/adms#>

    SELECT ?g
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(collection)} adms:status ${sparqlEscapeUri(HARVEST_STATUS_WAITING_FOR_NEXT_DOWNLOAD)}.
      }
    }
  `);
  return result.results.bindings.length > 0;
}


export {
  findAllFilesToHarvest,
  harvestFile,
  isRemoteDataObject
}
