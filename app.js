import { app, errorHandler, uuid, query, update, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
import { findAllFilesToHarvest, harvestFile, finishHarvestCollections } from './lib/harvest';

/**
 * Harvests downloaded files that have not been harvested before.
 * The harvesting may generate new file addresses to be download and harvested.
 * All related files are collected in a harvest collection.
*/
app.post('/harvest', async function(req, res, next) {
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

app.use(errorHandler);

