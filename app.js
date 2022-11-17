import { updateSudo as update, querySudo as query } from '@lblod/mu-auth-sudo';
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
import { appendTaskError, loadTask, updateTaskStatus, getScheduledTasks } from './lib/task';
import { CronJob } from 'cron';
import { CRON_FREQUENCY, DELETE_BATCH_SIZE } from './config'

const queue = new ProcessingQueue('Main Queue');

app.use(bodyParser.json({ type: function (req) { return /^application\/json/.test(req.get('content-type')); } }));

// ---------- CRON JOB ----------

/**
 * Engage harvesting flow by preparing scheduled tasks and their remote files.
 * The delta flow picks up the rest as soon as the files are downloaded.
*/
new CronJob(CRON_FREQUENCY, function() {
  console.log(`Collecting scheduled tasks triggered by cron job at ${new Date().toISOString()}`);
  queue.addJob(async () => processScheduledTasks(), async (error) => {
    console.error(`Something went wrong.`, error);
  });
}, null, true);

// ---------- API ----------

app.post("/on-download-failure", (req, res, next) => {
  queue.addJob(async () => onFailure(req.body), async (error) => {
    console.error(`Something went wrong.`, error);
  });
  return res.status(200).send();
});

/**
 * Harvests downloaded files that have not been harvested before via deltas.
 * The harvesting may generate new remote data to be download and harvested.
 * All related files are collected in a harvest collection.
*/
app.post('/delta', async function (req, res) {
  queue.addJob(async () => processDelta(req.body), async (error) => {
    console.error(`Something went wrong.`, error);
  });
  return res.status(202).end();
});

// ---------- LOGIC ----------

async function onFailure(data) {
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

async function processScheduledTasks() {
  // Handle scheduled tasks
  const scheduledTasks = await getScheduledTasks();

  // Tackling only one at a time, they can be huge, we want to space them out
  console.log(`Received ${scheduledTasks ? scheduledTasks.length : 0} task(s) to collect`);
  if (scheduledTasks && scheduledTasks.length) {
    console.log(`Starting to collect the first one: ${scheduledTasks[0]}`);
    await startCollectingTasks([scheduledTasks[0]]);
  }

  // It's all that needs to be done here, once the remote files will be downloaded
  // we'll fall back in the delta flow
  console.log(`We're done! Let's wait for the delta flow to pick up downloaded remote files!`);
}

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
  const count = await countRemoteDataObjects(task);
  console.log(`Schedueling ${count} remote data objects for task ${task.task}`);

  let offset = 0;
  while (offset < count) {
    const deleteStatusQuery = `
      ${PREFIXES}
      DELETE {
        GRAPH ?g {
          ?remoteDataObject adms:status ?status.
        }
      }
      WHERE {
        SELECT ?g ?remoteDataObject ?status
        WHERE {
          GRAPH ?g {
            ?remoteDataObject adms:status ?status.
          }
          BIND(${sparqlEscapeUri(task.task)} as ?task)
          ?task a ${sparqlEscapeUri(TASK_TYPE)};
            task:inputContainer ?container.
          ?container task:hasHarvestingCollection ?collection.
          ?collection a hrvst:HarvestingCollection.
          ?collection dct:hasPart ?remoteDataObject.
          ?remoteDataObject a nfo:RemoteDataObject.
        }
        LIMIT ${DELETE_BATCH_SIZE}
      }
    `;

    await update(deleteStatusQuery);
    offset += DELETE_BATCH_SIZE;
    console.log(`Deleted ${offset < count ? offset : count}/${count} remote file statuses`);
  }

  // Inserting statuses one by one to keep deltas flowing smoothly to the download-url service
  // It's the same as in prepareNewDownloads (./lib/harvest.js)
  const insertBatchSize = 1;
  offset = 0;
  while (offset < count) {
    const insertStatusQuery = `
      ${PREFIXES}

      INSERT {
        GRAPH ?g {
          ?remoteDataObject adms:status ${sparqlEscapeUri(STATUS_READY_TO_BE_CACHED)}.
        }
      }
      WHERE {
        SELECT ?g ?remoteDataObject
        WHERE {
          BIND(${sparqlEscapeUri(task.task)} as ?task)
  
          ?task a ${sparqlEscapeUri(TASK_TYPE)};
            task:inputContainer ?container.
    
          ?container task:hasHarvestingCollection ?collection.
          ?collection a hrvst:HarvestingCollection.
          ?collection dct:hasPart ?remoteDataObject.
    
          GRAPH ?g {
            ?remoteDataObject a nfo:RemoteDataObject;
              mu:uuid ?remoteDataObjectUuid .
          }
        }
        ORDER BY ?remoteDataObjectUuid
        LIMIT ${insertBatchSize}
        OFFSET ${offset}
      }
    `;

    await update(insertStatusQuery);
    offset += insertBatchSize;
    console.log(`Inserted ${offset < count ? offset : count}/${count} remote file statuses`);
  }
}

async function countRemoteDataObjects(task) {
  const queryResult = await query(`
    ${PREFIXES}

    SELECT (COUNT(?remoteDataObject) as ?count)
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
  `);

  return parseInt(queryResult.results.bindings[0].count.value);
}

app.use(errorHandler);
