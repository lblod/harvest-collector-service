import { uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { readFile } from 'fs-extra';
import { JSDOM } from 'jsdom';
import { URL } from 'url';
import path from 'path';
import { analyse } from '@lblod/marawa/rdfa-context-scanner';

const HARVESTING_GRAPH = process.env.HARVESTING_GRAPH || 'http://mu.semte.ch/graphs/harvesting';

const REMOTE_READY_STATUS = 'http://lblod.data.gift/file-download-statuses/ready-to-be-cached';
const REMOTE_SUCCESS_STATUS = 'http://lblod.data.gift/file-download-statuses/success';
const REMOTE_COLLECTED_STATUS = 'http://lblod.data.gift/file-download-statuses/collected';

const TASK_STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
const TASK_STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';

const FILE_BASE_DIR = '/share/';

const NAVIGATION_PREDICATES = [
  'http://lblod.data.gift/vocabularies/besluit/linkToPublication'
];
const SERVICE_URI = 'http://github.com/lblod/harvest-collector-service';

/**
 * Get all downloaded files ready for harvesting in an array of files that have not yet been harvested.
 *
 * @param Array remoteFiles Uris of the files to check
 *
 * @return Array of file address URIs
*/
async function ensureFilesAreReadyForHarvesting(remoteFiles) {
  const result = await query(`
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?remoteDataObject
    WHERE {
      GRAPH ${sparqlEscapeUri(HARVESTING_GRAPH)} {
        ?harvestingCollection a harvesting:HarvestingCollection ;
          dct:hasPart ?remoteDataObject .
        ?remoteDataObject adms:status ${sparqlEscapeUri(REMOTE_SUCCESS_STATUS)} .
      }

      FILTER( ?remoteDataObject IN (${remoteFiles.map(file => sparqlEscapeUri(file)).join(',')})) .
      FILTER NOT EXISTS { ?remoteDataObject adms:status ${sparqlEscapeUri(REMOTE_COLLECTED_STATUS)} } .
    }
  `);
  console.log(`Found ${result.results.bindings.length} files to harvest`);

  return result.results.bindings.map(b => b['remoteDataObject'].value);
}

/**
 * Harvests a single remoteDataObject
*/
async function harvestRemoteDataObject(remoteDataObject) {
  console.log(`Start harvesting remoteDataObject <${remoteDataObject}>`);
  const collection = await getHarvestingCollection(remoteDataObject);
  await processRemoteDataObject(remoteDataObject, collection);
}

// PRIVATE

/**
 * Get the URI of a harvest collection
 *
 * @return URI of the harvest collection
*/
async function getHarvestingCollection(remoteDataObject) {
  const result = await query(`
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?harvestingCollection
    WHERE {
      GRAPH ${sparqlEscapeUri(HARVESTING_GRAPH)} {
        ?harvestingCollection a harvesting:HarvestingCollection ;
            dct:hasPart <${remoteDataObject}> .
      }
    } LIMIT 1
  `);

  const collectionUri = result.results.bindings[0]['harvestingCollection'].value;
  return collectionUri;
}

/**
 * Harvest a remote data object and create additional resources to be downloaded/harvested if needed.
 * I.e. try to interpret the downloaded content as HTML, find navigation properties and trigger
 * new downloads for those URLs.
*/
async function processRemoteDataObject(remoteDataObject, collection) {
  try {
    const physicalFile = await getPhysicalFile(remoteDataObject);
    if (physicalFile) {
      const urls = await getLinkedUrls(physicalFile, remoteDataObject);
      console.log(`Found ${urls.length} additional URLs that need to be harvested: ${JSON.stringify(urls)}`);

      await updateHarvestStatus(remoteDataObject, REMOTE_COLLECTED_STATUS);

      if (urls.length) {
        // We found new links to be harvested in the document
        console.log(`Preparing new downloads for urls ${urls}`);
        await prepareNewDownloads(urls, collection);
      } else if (await hasAllCollectedRemoteDataObjects(collection)) {
        // No new links and there are no remote data objects waiting to be processed by this service
        console.log(`All files have been processed for collection ${collection}, wrapping up.`);
        const task = await getTask(collection);
        await appendCollectedFilesToTaskResultsContainer(task, collection);
        await updateHarvestStatus(task, TASK_STATUS_SUCCESS);
      }
    } else {
      console.log(`No physical file found for remoteDataObject <${remoteDataObject}>`);
      const task = await getTask(collection);
      await updateHarvestStatus(task, TASK_STATUS_FAILED);
    }
  } catch (e) {
    console.log(`Something went wrong while processing remoteDataObject <${remoteDataObject}>`);
    console.log(e);
    const task = await getTask(collection);
    await updateHarvestStatus(task, TASK_STATUS_FAILED);
  }
}

/**
 * Make sure all URLs in the URL set are full URLs (i.e. starting with http(s)://)
 * Relative paths are resolved against the download URL of the given remoteDataObject
 *
 * @return An array of full URLs
 */
async function ensureFullUrls(urls, remoteDataObject) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?url
    WHERE {
      <${remoteDataObject}> nie:url ?url .
    }
  `);

  if (!result.results.bindings.length)
    throw new Error(`Remote data object <${remoteDataObject}> doesn't have a URL`);

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
 * Triggers a new file download for each URL in the set of URLs that has not alread been collected.
 * Each generated file is attached to the given harvest collection
 * and has a reference to the file it is derived from.
*/
async function prepareNewDownloads(urls, collection) {
  for (let url of urls) {
    if (!(await hasBeenCollected(url, collection))) {
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
          GRAPH ${sparqlEscapeUri(HARVESTING_GRAPH)} {
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
}

/**
 * Checks if the url has already been collected in the current collection
*/
async function hasBeenCollected(url, collection) {
  const result = await query(`
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?remoteDataObject
    WHERE {
      GRAPH ${sparqlEscapeUri(HARVESTING_GRAPH)} {
        ${sparqlEscapeUri(collection)} dct:hasPart ?remoteDataObject .
        ?remoteDataObject nie:url ${sparqlEscapeUri(url)} .
      }
    } LIMIT 1
  `);

  return result.results.bindings.length;
}

/**
 * Updates the harvest status of the resource with the given URI if needed
*/
async function updateHarvestStatus(uri, status) {
  await update(`
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>

    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified.
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${sparqlEscapeUri(status)}.
       ?subject dct:modified ${sparqlEscapeDateTime(new Date())}.
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${sparqlEscapeUri(uri)} as ?subject)

        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified. }
      }
    }
  `);
}

/**
 * Gets the task associated to a collection
*/
async function getTask(collection) {
  const taskResult = await query(`
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    SELECT DISTINCT ?task
    WHERE {
      GRAPH ?g {
        ?task task:inputContainer ?container.
        ?container task:hasHarvestingCollection ${sparqlEscapeUri(collection)}.
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
 * Gets the URLs linking to other documents present in the content of a given remoteDataObject
*/
async function getLinkedUrls(physicalFile, remoteDataObject) {
  const rdfaBlocks = await getRdfaBlocks(physicalFile);
  console.log(`Found ${rdfaBlocks.length} RDFa blocks`);

  let urls = [];
  for (let rdfaBlock of rdfaBlocks) {
    const newUrls = rdfaBlock.context.filter(function(t) {
      return NAVIGATION_PREDICATES.includes(t.predicate) && !urls.includes(t.object);
    }).map(t => t.object);
    urls = urls.concat(newUrls);
  }

  return await ensureFullUrls(urls, remoteDataObject);
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

async function hasAllCollectedRemoteDataObjects(collection) {
  const result = await query(`
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>

    SELECT ?file
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(collection)} dct:hasPart ?file .
      }
      FILTER NOT EXISTS { ?file adms:status ${sparqlEscapeUri(REMOTE_COLLECTED_STATUS)} } .
    }
  `);

  return result.results.bindings.length == 0;
}

async function appendCollectedFilesToTaskResultsContainer(task, collection){
  const id = uuid();
  const containerUri = `http://redpencil.data.gift/id/dataContainers/${id}`;
  const selectRequest = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?g ?remoteDataObject ?fileName ?size
    WHERE {
      GRAPH ?g {
       BIND(${sparqlEscapeUri(collection)} as ?collection)
       ?collection dct:hasPart ?remoteDataObject.
       ?physicalFile nie:dataSource ?remoteDataObject.
       ?physicalFile nfo:fileName ?fileName.
       ?physicalFile nfo:fileSize ?size.
      }
    }
  `;
  const result = await query(selectRequest);

  if (result.results.bindings.length) {
    let triples = '';
    result.results.bindings.forEach(result => {
      triples = `
        ${triples}
        GRAPH ${sparqlEscapeUri(result['g'].value)} {
          ${sparqlEscapeUri(containerUri)} a nfo:DataContainer.
          ${sparqlEscapeUri(containerUri)} mu:uuid ${sparqlEscapeString(id)}.
          ${sparqlEscapeUri(task)} task:resultsContainer ${sparqlEscapeUri(containerUri)}.
          ${sparqlEscapeUri(containerUri)} task:hasFile ${sparqlEscapeUri(result['remoteDataObject'].value)}.
          ${sparqlEscapeUri(result['remoteDataObject'].value)} a nfo:FileDataObject.
          ${sparqlEscapeUri(result['remoteDataObject'].value)} nfo:fileName ${sparqlEscapeString(result['fileName'].value)}.
          ${sparqlEscapeUri(result['remoteDataObject'].value)} nfo:fileSize ${sparqlEscapeString(result['size'].value)}.
        }
      `
    });

    const insertRequest = `
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      INSERT DATA {
        ${triples}
      }
    `;
    await update(insertRequest);
  }
}

export {
  ensureFilesAreReadyForHarvesting,
  harvestRemoteDataObject,
  isRemoteDataObject
}
