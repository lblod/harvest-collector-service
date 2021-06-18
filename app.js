import { app, errorHandler, uuid, query, update, sparqlEscapeString, sparqlEscapeDateTime, sparqlEscapeUri } from 'mu';
import { ensureFilesAreReadyForHarvesting, handleDownloadFailure, harvestRemoteDataObject, isRemoteDataObject } from './lib/harvest';
import flatten from 'lodash.flatten';
import bodyParser from 'body-parser';
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



app.use(errorHandler);
