import { updateSudo as update } from '@lblod/mu-auth-sudo';
import bodyParser from 'body-parser';
import flatten from 'lodash.flatten';
import { app, errorHandler, sparqlEscapeUri } from 'mu';
import {
    PREFIXES, STATUS_BUSY, STATUS_FAILED, STATUS_READY_TO_BE_CACHED, STATUS_SCHEDULED, TASK_COLLECTING, TASK_TYPE
} from './constants';
import { Delta } from './lib/delta';
import { ensureFilesAreReadyForHarvesting, handleDownloadFailure, harvestRemoteDataObject, isRemoteDataObject } from './lib/harvest';
import { appendTaskError, isTask, loadTask, updateTaskStatus } from './lib/task';



const SUCCESS = 'http://lblod.data.gift/file-download-statuses/success';
const FAILURE = 'http://lblod.data.gift/file-download-statuses/failure';

app.use(bodyParser.json({ type: function (req) { return /^application\/json/.test(req.get('content-type')); } }));


app.post("/on-download-failure", async (req, res, next) => {
  const remoteDatasMaxFailure = await getRemoteFileUris(req.body, FAILURE);
  if (!remoteDatasMaxFailure.length) {
  } else {
    await handleDownloadFailure(remoteDatasMaxFailure);
  }
  return res.status(200).send();
});

/**
 * Harvests downloaded files that have not been harvested before.
 * The harvesting may generate new remote data to be download and harvested.
 * All related files are collected in a harvest collection.
*/
app.post('/delta', async function (req, res, next) {
  //Delta may contain mutiple blobs of information. Hence this block of code does two things

  //Handle new tasks
  const entries = new Delta(req.body).getInsertsFor('http://www.w3.org/ns/adms#status', STATUS_SCHEDULED);
  await startCollectingTasks(entries);


  //Handle the follow up tasks, i.e. make sure to stop once all URLs downloaded.
  const remoteFiles = await getRemoteFileUris(req.body, SUCCESS);
  if (!remoteFiles.length) {
    console.log("Delta does not contain a new remote data object with status 'success'. Nothing should happen.");
    return res.status(204).send();
  }

  try {
    console.log(`Start harvesting new files ${remoteFiles}`);
    const remoteDataObjects = await ensureFilesAreReadyForHarvesting(remoteFiles);

    for (let remoteDataObject of remoteDataObjects) {
      await harvestRemoteDataObject(remoteDataObject);
    }

    console.log(`We're done! Let's wait for the next harvesting round...`);
    return res.status(202).end();

  } catch (e) {
    console.log(`Something went wrong.`);
    console.log(e);
    return next(new Error(e.message));
  }
});

/**
 * Returns the inserted succesfully downloaded remote file URIs
 * from the delta message. An empty array if there are none.
 *
 * @param Object delta Message as received from the delta notifier
*/
async function getRemoteFileUris(delta, status) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  const successes = inserts.filter(triple => isTriggerTriple(triple, status)).map(t => t.subject.value);
  const remoteDataObjects = (await Promise.all(successes.map(uri => isRemoteDataObject(uri)))).filter(object => object != undefined);
  return remoteDataObjects;
}

/**
 * Returns whether the passed triple is a trigger for an import process
 *
 * @param Object triple Triple as received from the delta notifier
*/
function isTriggerTriple(triple, status) {
  return triple.predicate.value == 'http://www.w3.org/ns/adms#status'
    && triple.object.value == status;
};


async function startCollectingTasks(entries){
  for (let entry of entries) {
    if(! await isTask(entry) ) continue;
    const task = await loadTask(entry);

    try {
      if(isCollectingTask(task)){
        await updateTaskStatus(task, STATUS_BUSY);
        await scheduleRemoteDataObjectsForDownload(task);
      }
    }
    catch (e){
      console.error(e);
      await appendTaskError(task, e.message);
      await updateTaskStatus(task, STATUS_FAILED);
    }
  }
}

function isCollectingTask(task){
   return task.operation == TASK_COLLECTING;
}

async function scheduleRemoteDataObjectsForDownload(task){
  const deleteStatusQuery = `
    ${PREFIXES}

    DELETE {
      GRAPH ?g {
        ?remoteDataObject adms:status ?status.
      }
    }
    WHERE {
     BIND(${sparqlEscapeUri(task.task)} as ?task)
     GRAPH ?g {
       ?remoteDataObject adms:status ?status.
     }

     ?task a ${ sparqlEscapeUri(TASK_TYPE) };
       task:inputContainer ?container.

     ?container task:hasHarvestingCollection ?collection.
     ?collection a hrvst:HarvestingCollection.
     ?collection dct:hasPart ?remoteDataObject.
     ?remoteDataObject a nfo:RemoteDataObject.
  }
  `;

  await update(deleteStatusQuery);

  const updateQuery = `
    ${PREFIXES}

    INSERT {
      GRAPH ?g {
        ?remoteDataObject adms:status ${sparqlEscapeUri(STATUS_READY_TO_BE_CACHED)}.
      }
    }
    WHERE {
     BIND(${sparqlEscapeUri(task.task)} as ?task)

     ?task a ${ sparqlEscapeUri(TASK_TYPE) };
       task:inputContainer ?container.

     ?container task:hasHarvestingCollection ?collection.
     ?collection a hrvst:HarvestingCollection.
     ?collection dct:hasPart ?remoteDataObject.

     GRAPH ?g {
       ?remoteDataObject a nfo:RemoteDataObject.
     }
  }
  `;

  await update(updateQuery);
}


app.use(errorHandler);
