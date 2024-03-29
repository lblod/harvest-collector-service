import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { readFile } from 'fs-extra';
import { sparqlEscapeDateTime, sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { TASK_TYPE, PREFIXES } from '../constants';
import { ProcessingQueue } from './processing-queue';
import  streamify from 'streamify-string';
import streamToArray from 'stream-to-array';
import rdfParser from 'rdf-parse';
import { attachClonedAuthenticationConfiguraton, deleteCredentials, hasAuth } from './credential-helpers';


const INTERNAL_QUEUE = new ProcessingQueue('Internal Queue');

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
      } else if (await isHarvestingCollectionDone({
        collection: collection,
        remoteDataObject: remoteDataObject
      })) {
        // No new links and there are no remote data objects waiting to be processed by this service
        console.log(`All files have been processed for collection ${collection}, wrapping up.`);
        const task = await getTask(collection);
        await appendCollectedFilesToTaskResultsContainer(task, collection);

        await deleteCredentials(collection);
        await updateHarvestStatus(task, TASK_STATUS_SUCCESS);
      }
    } else {
      console.error(`No physical file found for remoteDataObject <${remoteDataObject}>`);
      const task = await getTask(collection);

      await deleteCredentials(collection);
      await updateHarvestStatus(task, TASK_STATUS_FAILED);
    }
  } catch (e) {
    console.error(`Something went wrong while processing remoteDataObject <${remoteDataObject}>`);
    console.error(e);
    const task = await getTask(collection);

    await deleteCredentials(collection);
    await updateHarvestStatus(task, TASK_STATUS_FAILED);
  }
}

async function getParentUrl(remoteDataObject) {
  const result = await query(`
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT ?url
    WHERE {
      <${remoteDataObject}> nie:url ?url .
    }
  `);

  if (!result.results.bindings.length)
    throw new Error(`Remote data object <${remoteDataObject}> doesn't have a URL`);

  return result.results.bindings[0]['url'].value;
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

      const createNewRemoteDataObjectQuery = `
      ${PREFIXES}
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
      `;
      // Making sure we attach the authentication configuration only when needed
      if (await hasAuth(collection)) {
        await update(createNewRemoteDataObjectQuery);
        await attachClonedAuthenticationConfiguraton(remoteDataObjectUri, collection);
      } else {
        await update(createNewRemoteDataObjectQuery);
      }
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
    PREFIX tasko: <http://lblod.data.gift/id/jobs/concept/TaskOperation/>
    SELECT DISTINCT ?task
    WHERE {
      GRAPH ?g {
        ?task
          a task:Task ;
          task:operation tasko:collecting ;
          task:inputContainer ?container.
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


async function getFile(physicalFile) {
  const filePath = physicalFile.replace('share://', FILE_BASE_DIR);
  const content = await readFile(filePath, 'utf8');
  return content;
}

// Workaround to avoid extracting the same url multiple times because a `jsessionid`
// is set in the url. This is only relevant of urls using a Java backend.
// todo check in the future if that's still the case, other this could be completely removed.
// Not that we also cleanup the hash, e.g http://foo.com#blabla, this shouldn't affect
// extraction. We keep the other query parameters that are necessary for extraction.
function cleanUrl(url) {
  const uri = new URL(url);
  uri.hash = '';
  return uri.toString().replace(/;jsessionid=[a-zA-Z;0-9]*/i, "");
}

/**
 * Gets the URLs linking to other documents present in the content of a given remoteDataObject
*/
async function getLinkedUrls(physicalFile, remoteDataObject) {
  const contentType = 'text/html';
  const baseUrl = await getParentUrl(remoteDataObject);

  const content = await getFile(physicalFile);
  const textStream = streamify(content);
  const rdfaStream = rdfParser.parse(textStream, { contentType: contentType, baseIRI: baseUrl });
  const rdfa = await streamToArray(rdfaStream);
  const objects = rdfa.filter(r => r.predicate?.value === 'http://lblod.data.gift/vocabularies/besluit/linkToPublication')
    .map(r => r.object.value)
    .map(cleanUrl);
  const urls = [...new Set(objects)];

  return urls;
}

/**
 * Returns whether the passed URI is a RemoteDataObject or not
 *
 * @param String uri Uri of the object to test
*/
async function isRelevantRemoteDataObject(uri) {
  const result = await query(`
    ${PREFIXES}
    ASK {
      BIND(${sparqlEscapeUri(uri)} as ?remoteDataObject)

     ?task a ${sparqlEscapeUri(TASK_TYPE)};
       task:inputContainer ?container.

     ?container task:hasHarvestingCollection ?collection.
     ?collection a hrvst:HarvestingCollection.
     ?collection dct:hasPart ?remoteDataObject.

     ?remoteDataObject a nfo:RemoteDataObject.
    }
  `);

  return result.boolean ? uri : undefined;
}


async function appendCollectedFilesToTaskResultsContainer(task, collection) {
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
    const results = await Promise.all(result.results.bindings.map(async result => {
      const insertRequest = `
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        INSERT DATA {
          GRAPH ${sparqlEscapeUri(result['g'].value)} {
            ${sparqlEscapeUri(containerUri)} a nfo:DataContainer.
            ${sparqlEscapeUri(containerUri)} mu:uuid ${sparqlEscapeString(id)}.
            ${sparqlEscapeUri(task)} task:resultsContainer ${sparqlEscapeUri(containerUri)}.
            ${sparqlEscapeUri(containerUri)} task:hasFile ${sparqlEscapeUri(result['remoteDataObject'].value)}.
            ${sparqlEscapeUri(result['remoteDataObject'].value)} a nfo:FileDataObject.
            ${sparqlEscapeUri(result['remoteDataObject'].value)} nfo:fileName ${sparqlEscapeString(result['fileName'].value)}.
            ${sparqlEscapeUri(result['remoteDataObject'].value)} nfo:fileSize ${sparqlEscapeString(result['size'].value)}.
          }
        }

      `;
      return await update(insertRequest);

    }));
  }
}

async function handleDownloadFailure(remoteDatasMaxFailure) {
  const collections = await Promise.all(remoteDatasMaxFailure.map(async rd => {
    const c = await getHarvestingCollection(rd);
    return {
      remoteDataObject: rd,
      collection: c
    };
  }));
  setTimeout(async () => {
    for (let collection of collections) {

      if (collection && await isHarvestingCollectionDone(collection)) {
        const task = await getTask(collection.collection);
        const taskStatus = await getTaskStatus(task);
        if (taskStatus === TASK_STATUS_SUCCESS || taskStatus === TASK_STATUS_FAILED) {
          console.log('nothing to do.');
          return;
        }
        if (await hasRemoteObjectCollected(collection.collection)) {
          await appendCollectedFilesToTaskResultsContainer(task, collection.collection);
          await updateHarvestStatus(task, TASK_STATUS_SUCCESS);
        } else {
          console.warn(`Collection ${collection.collection} has no collected url, thus fail the job`);
          await updateHarvestStatus(task, TASK_STATUS_FAILED);
        }
      } else {
        console.log("still ongoing, nothing else to do now");
        INTERNAL_QUEUE.addJob(async () => handleDownloadFailure(remoteDatasMaxFailure), async (error) => {
          console.error(`Something went wrong.`, error);
        });
      }
    }
  }, 300000);



}

async function isHarvestingCollectionDone(collection) {
  //Note: ?remoteDataObject is considerded done for the collection,
  // when it has status: http://lblod.data.gift/file-download-statuses/collected
  //So, don't get confused by the http://lblod.data.gift/file-download-statuses/success,
  // as this is not the final status, from the perspective of a collection.
  const q = `
    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

   select ?remoteDataObject
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(collection.collection)} a harvesting:HarvestingCollection ;
            dct:hasPart ?remoteDataObject .
?remoteDataObject  adms:status  ?status.
    FILTER  ( ?status  IN (<http://lblod.data.gift/file-download-statuses/ready-to-be-cached>,
                           <http://lblod.data.gift/file-download-statuses/ongoing>,
                           <http://lblod.data.gift/file-download-statuses/success>)).
    FILTER  ( ?remoteDataObject NOT IN (${sparqlEscapeUri(collection.remoteDataObject)})).

      }
    }
  `;
  const result = await query(q);
  return result.results.bindings.length === 0;

}
async function hasRemoteObjectCollected(collection) {
  const q = `

    PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

   select ?remoteDataObject
    WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(collection)} a harvesting:HarvestingCollection ;
            dct:hasPart ?remoteDataObject .
?remoteDataObject  adms:status  ?status.
    FILTER  ( ?status IN (<http://lblod.data.gift/file-download-statuses/collected>)).

      }
    }

  `;
  const result = await query(q);
  return result.results.bindings.length;

}

async function getTaskStatus(task) {
  const q = `
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX adms: <http://www.w3.org/ns/adms#>
    PREFIX dct: <http://purl.org/dc/terms/>
    select ?status {
    ${sparqlEscapeUri(task)} adms:status ?status.
    }

  `;
  const result = await query(q);
  return result.results.bindings[0]['status'].value;

}

export {
  ensureFilesAreReadyForHarvesting,
  harvestRemoteDataObject,
  handleDownloadFailure,
  isRelevantRemoteDataObject
};
