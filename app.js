import { app, errorHandler, uuid, query, update, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
import { findAllFilesToHarvest, harvestFile, finishHarvestCollections, isRemoteDataObject } from './lib/harvest';
import flatten from 'lodash.flatten';
import bodyParser from 'body-parser';

app.use(bodyParser.json({ type: function(req) { return /^application\/json/.test(req.get('content-type')); } }));

/**
 * Harvests downloaded files that have not been harvested before.
 * The harvesting may generate new remote data to be download and harvested.
 * All related files are collected in a harvest collection.
*/
app.post('/delta', async function(req, res, next) {
  const remoteFiles = await getRemoteFileUris(req.body);
  if (!remoteFiles.length) {
    console.log("Delta does not contain a new remote data object with status 'success'. Nothing should happen.");
    return res.status(204).send();
  }

  try {
    console.log(`Start harvesting new files`);
    const files = await findAllFilesToHarvest();

    for (let file of files) {
      await harvestFile(file);
    }

    console.log(`We're done! Let's wait for the next harvesting round...`);        
    return res.status(202).end();

  } catch (e) {
    return next(new Error(e.message));
  }
});

/**
 * Returns the inserted succesfully downloaded remote file URIs
 * from the delta message. An empty array if there are none.
 *
 * @param Object delta Message as received from the delta notifier
*/
async function getRemoteFileUris(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  const successes = inserts.filter(isTriggerTriple).map(t => t.subject.value);
  const remoteDataObjects = (await Promise.all(successes.map(uri => isRemoteDataObject(uri)))).filter(object => object != undefined);
  return remoteDataObjects;
}

/**
 * Returns whether the passed triple is a trigger for an import process
 *
 * @param Object triple Triple as received from the delta notifier
*/
function isTriggerTriple(triple) {
  return triple.predicate.value == 'http://www.w3.org/ns/adms#status'
    && triple.object.value == 'http://lblod.data.gift/file-download-statuses/success';
};

app.use(errorHandler);
