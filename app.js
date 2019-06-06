import { app, errorHandler, uuid, query, update, sparqlEscapeString, sparqlEscapeDateTime } from 'mu';
import { findAllFileAddressesToHarvest, harvestFileAddress, finishHarvestCollections } from './lib/harvest';

/**
 * Harvests downloaded file addresses that have not been harvested before.
 * The harvesting may generate new file addresses to be download and harvested.
 * All related file addresses are collected in a harvest collection.
*/
app.post('/harvest', async function(req, res, next) {
  try {
    console.log(`Start harvesting new file addresses`);
    const fileAddresses = await findAllFileAddressesToHarvest();
    
    for (let fileAddress of fileAddresses) {
      await harvestFileAddress(fileAddress);
    }

    console.log(`Harvesting done. Time to wrap up!`);    
    await finishHarvestCollections();

    console.log(`We're done! Let's wait for the next harvesting round...`);        

    return res.status(202).end();    
  } catch (e) {
    return next(new Error(e.message));
  }
});


app.use(errorHandler);

