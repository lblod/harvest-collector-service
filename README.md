# harvest-collector-service
Microservice that creates harvest collections by parsing downloaded HTML files and triggering downloads of additional file addresses by following navigational properties.

The following navigational properties currently trigger a new download:
* http://data.lblod.info/vocabularies/besluitPublicatie/linkToPublications

## Usage

### Docker-compose
Add the following snippet in your `docker-compose.yml`:
```
  harvest:
    image: lblod/harvest-collector-service
    volumes:
      - ./data/files:/share
```

The `/share` volume contains the downloaded files (as downloaded by the `lblod/download-url-service`).

### Delta configuration

```
  {
    match: {
      predicate: {
        type: 'uri',
        value: 'http://www.w3.org/ns/adms#status'
      },
      object: {
        type: 'uri',
        value: 'http://lblod.data.gift/file-download-statuses/success'
      }
    },
    callback: {
      method: 'POST',
      url: 'http://harvest-collector/delta',
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
```

## Model
The service harvests collections containing a set of remote data object that are related by following navigational properties.

Eg.
```
@prefix mu: <http://mu.semte.ch/vocabularies/core/> .
@prefix harvesting: <http://lblod.data.gift/vocabularies/harvesting/> .
@prefix dct: <http://purl.org/dc/terms/> .

<http://data.lblod.info/id/harvest-collections/326ce8f6-9567-4e1d-ab3d-cda23d143701> a harvesting:HarvestingCollection ;
  mu:uuid "326ce8f6-9567-4e1d-ab3d-cda23d143701" ;
  adms:status <http://lblod.data.gift/collecting-statuses/completed> ;
  dct:hasPart <http://data.lblod.info/id/remote-data-objects/2387b790-9f6d-11ea-ace4-6d0f856d8978> ;
  dct:hasPart <http://data.lblod.info/id/remote-data-objects/92aedad4-b961-4f34-8f79-93c8fc28cd94> ;
  dct:hasPart <http://data.lblod.info/id/remote-data-objects/511ce6bd-aaca-4ef8-ab7e-566cf9663380> .
```

A harvest collection has one of the following states:

```
http://lblod.data.gift/collecting-statuses/not-started
http://lblod.data.gift/collecting-statuses/ongoing
http://lblod.data.gift/collecting-statuses/waiting-for-next-download
http://lblod.data.gift/collecting-statuses/completed
http://lblod.data.gift/collecting-statuses/failed
```

## API
### POST /harvest
Trigger a new harvest round. Each harvest round consist of:
1. Creating a new harvest collection for new downloaded file addresses
2. Inspecting navigational properties in new downloaded files and triggering additional downloads attached to the same harvest collection. These additional downloads will be harvested in a following round (after the download has successfully finished)
3. Updating the state of harvest collections for which all files have been harvested

## Restrictions
The service expects HTML files containing at least a `body` tag.
