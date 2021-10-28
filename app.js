import { updateSudo as update } from '@lblod/mu-auth-sudo';
import bodyParser from 'body-parser';
import flatten from 'lodash.flatten';
import { app, errorHandler, sparqlEscapeUri } from 'mu';
import {
  FILE_DOWNLOAD_FAILURE, FILE_DOWNLOAD_SUCCESS, PREFIXES,
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_READY_TO_BE_CACHED,
  STATUS_SCHEDULED,
  TASK_COLLECTING,
  TASK_TYPE
} from './constants';
import { Delta } from './lib/delta';
import { ensureFilesAreReadyForHarvesting, handleDownloadFailure, harvestRemoteDataObject, isRelevantRemoteDataObject } from './lib/harvest';
import { ProcessingQueue } from './lib/processing-queue';
import { appendTaskError, isTask, loadTask, updateTaskStatus } from './lib/task';

const queue = new ProcessingQueue();

app.use(bodyParser.json({ type: function (req) { return /^application\/json/.test(req.get('content-type')); } }));

app.post("/on-download-failure", (req, res, next) => {
  queue.addJob(async () => onFailure(req.body), async (error) => {
    console.error(`Something went wrong.`, error);
  });
  return res.status(200).send();
});

async function onFailure(data){
  const remoteDatasMaxFailure = await getRemoteFileUris(data, FILE_DOWNLOAD_FAILURE);
  if (!remoteDatasMaxFailure.length) {
  } else {
    queue.addJob(async () => handleDownloadFailure(remoteDatasMaxFailure), async (error) => {
      console.error(`Something went wrong.`, error);
    });
  }
}


async function processDelta(data) {
  //Delta may contain mutiple blobs of information. Hence this block of code does two things
  //Handle new tasks
  const entries = new Delta(data).getInsertsFor('http://www.w3.org/ns/adms#status', STATUS_SCHEDULED);
  await startCollectingTasks(entries);
  //Handle the follow up tasks, i.e. make sure to stop once all URLs downloaded.
  const remoteFiles = await getRemoteFileUris(data, FILE_DOWNLOAD_SUCCESS);
  if (!remoteFiles.length) {
    console.log("Delta does not contain a new remote data object with status 'success'. Nothing should happen.");
    return;
  }
    console.log(`Start harvesting new files ${remoteFiles}`);
    const remoteDataObjects = await ensureFilesAreReadyForHarvesting(remoteFiles);

    for (let remoteDataObject of remoteDataObjects) {
      await harvestRemoteDataObject(remoteDataObject);
    }

    console.log(`We're done! Let's wait for the next harvesting round...`);
}

/**
 * Harvests downloaded files that have not been harvested before.
 * The harvesting may generate new remote data to be download and harvested.
 * All related files are collected in a harvest collection.
*/
app.post('/delta', async function (req, res, next) {
  queue.addJob(async () => processDelta(req.body), async (error) => {
    console.error(`Something went wrong.`, error);
  });
  return res.status(202).end();
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
  const remoteDataObjects = (await Promise.all(successes.map(uri => isRelevantRemoteDataObject(uri)))).filter(object => object != undefined);
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


async function startCollectingTasks(entries) {
  for (let entry of entries) {
    const task = await loadTask(entry);

    if (!task) continue;

    try {
      if (isCollectingTask(task)) {
        await updateTaskStatus(task, STATUS_BUSY);
        await scheduleRemoteDataObjectsForDownload(task);
      }
    }
    catch (e) {
      console.error(e);
      if (task) {
        await appendTaskError(task, e.message);
        await updateTaskStatus(task, STATUS_FAILED);
      }
      //TODO: log general error if no task
    }
  }
}

function isCollectingTask(task) {
  return task.operation == TASK_COLLECTING;
}

async function scheduleRemoteDataObjectsForDownload(task) {
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

     ?task a ${sparqlEscapeUri(TASK_TYPE)};
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

     ?task a ${sparqlEscapeUri(TASK_TYPE)};
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
