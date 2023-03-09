import { PREFIXES, BASIC_AUTH, OAUTH2 } from '../constants';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri, uuid } from 'mu';
import { parseResult } from './utils';

/**
 * Gets the remoteDataObject and the collection from Task.
 * @param {String} taskUri
 * @param {String} taskType
 * @param {Number} insertBatchSize
 * @param {Number} offset
 * @returns {Object} Object with remoteDataObjectUri and collection
 */
export async function getRemoteDataObjectAndCollectionFromTask(taskUri, taskType, insertBatchSize, offset) {
  const findRemoteDataObjectQuery = `
  ${PREFIXES}
  SELECT DISTINCT ?remoteDataObjectUri ?collection WHERE {
    GRAPH ?g {
      BIND(${sparqlEscapeUri(taskUri)} as ?task)

      ?task a ${sparqlEscapeUri(taskType)};
        task:inputContainer ?container.

      ?container task:hasHarvestingCollection ?collection.
      ?collection a hrvst:HarvestingCollection.
      ?collection dct:hasPart ?remoteDataObjectUri.

      ?remoteDataObjectUri a nfo:RemoteDataObject;
        mu:uuid ?remoteDataObjectUuid .
    }
  }
  ORDER BY ?remoteDataObjectUuid
  LIMIT ${insertBatchSize}
  OFFSET ${offset}
  `;
  const queryResult =  parseResult(await query(findRemoteDataObjectQuery))[0]
  return queryResult;
}

/**
 * Checks the collection for an authentication scheme attached.
 *
 * @param {String} collectionUri
 * @returns {Boolean} `true` || `false`
 */
export async function hasAuth(collectionUri) {

  const findAuthenticationSchemeQuery = `
  ${PREFIXES}
      ASK WHERE {
        GRAPH ?g {
        ${sparqlEscapeUri(collectionUri)} dgftSec:targetAuthenticationConfiguration ?authenticationConfiguration.
        ?authenticationConfiguration dgftSec:securityConfiguration/rdf:type ?secType .
        VALUES ?secType {
          <https://www.w3.org/2019/wot/security#BasicSecurityScheme>
          <https://www.w3.org/2019/wot/security#OAuth2SecurityScheme>
        }
       }
      }
    `;
    const boolean = await query(findAuthenticationSchemeQuery);
    return boolean;
}


/**
 * Gets CredentialsType from RemoteDataObject
 * @param {String} collectionUri
 * @returns credentialsType
 */

 async function getCredentialsType(collectionUri) {
  const credentialsTypeQuery = `
    PREFIX dgftSec: <http://lblod.data.gift/vocabularies/security/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>

    SELECT DISTINCT ?securityConfigurationType WHERE {
        ${sparqlEscapeUri(
          collectionUri
        )} dgftSec:targetAuthenticationConfiguration ?authenticationConf .
        ?authenticationConf dgftSec:securityConfiguration/rdf:type ?securityConfigurationType .
        VALUES ?securityConfigurationType {
          <https://www.w3.org/2019/wot/security#BasicSecurityScheme>
          <https://www.w3.org/2019/wot/security#OAuth2SecurityScheme>
      }
    }
  `;
  const credentialsType = await query(credentialsTypeQuery);
  return credentialsType.results.bindings[0]
    ? credentialsType.results.bindings[0].securityConfigurationType.value
    : null;
}

/**
 * Deletes credentials from collection when the task is done.
 * @param {String} collectionUri
 */

export async function deleteCredentials(collectionUri) {
  let credentialsType;
  let cleanOauth2Query = `
      ${PREFIXES}
      DELETE {
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets dgftOauth:clientId ?clientId ;
            dgftOauth:clientSecret ?clientSecret .
          ?configuration wotSec:SecurityScheme ?schemes .
          ?schemes wotSec:token ?token ;
            wotSec:flow ?flow .
        }
      } WHERE {

        ${sparqlEscapeUri(
          collectionUri
        )} dgftSec:targetAuthenticationConfiguration ?configuration .

        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets dgftOauth:clientId ?clientId ;
            dgftOauth:clientSecret ?clientSecret .
          ?configuration wotSec:SecurityScheme ?schemes .
          ?schemes wotSec:token ?token ;
            wotSec:flow ?flow .
        }
      }
      `;
  let cleanBasicAuthQuery = `
      ${PREFIXES}
      DELETE {
        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets meb:username ?user ;
            muAccount:password ?pass .
        }
      } WHERE {

        ${sparqlEscapeUri(
          collectionUri
        )} dgftSec:targetAuthenticationConfiguration ?configuration .

        GRAPH ?g {
          ?configuration dgftSec:secrets ?secrets .
          ?secrets meb:username ?user ;
            muAccount:password ?pass .
        }
      }
      `;

  if (!credentialsType)
    credentialsType = await getCredentialsType(
      collectionUri
    );

  switch (credentialsType) {
    case BASIC_AUTH:
      await update(cleanBasicAuthQuery);
      break;
    case OAUTH2:
      await update(cleanOauth2Query);
      break;
    default:
      return false;
  }
}

/**
 * Inserting the `AuthenticationConfiguration` from the collection to the remoteDataObject to allow the download step from `download-url-service`.
 *
 * @param {String} remoteDataObjectUri remoteDataObject
 * @param {String} collection collection
 * @returns newAuthConfUri
 *
 * Note: `AuthenticationConfiguration` credentials will be removed in the `download-url-service`.
 */
 export async function attachClonedAuthenticationConfiguraton(remoteDataObjectUri, collection) {
    const newAuthConfUri = `http://data.lblod.info/id/authentication-configurations/${uuid()}`;
    const getAuthInfoQuery = `
      ${PREFIXES}
      SELECT DISTINCT ?secType ?authenticationConfiguration WHERE {
       GRAPH ?g {
         ${sparqlEscapeUri(collection)} dgftSec:targetAuthenticationConfiguration ?authenticationConfiguration.
         ?authenticationConfiguration dgftSec:securityConfiguration/rdf:type ?secType .
         VALUES ?secType {
          <https://www.w3.org/2019/wot/security#BasicSecurityScheme>
          <https://www.w3.org/2019/wot/security#OAuth2SecurityScheme>
        }
       }
      }
    `;

    const authData = parseResult(await query(getAuthInfoQuery))[0];

    const newOauth2SecurityScheme = `http://data.lblod.info/id/oauth2-security-schemes/${uuid()}`;
    const newOauth2Creds = `http://data.lblod.info/id/oauth2-credentials/${uuid()}`;
    const newBasicSecurityScheme = `http://data.lblod.info/id/basic-security-schemes/${uuid()}`;
    const newBasicCreds = `http://data.lblod.info/id/basic-authentication-credentials/${uuid()}`;

    let cloneQuery;

    if(!authData){
      return null;
    }
    else if(authData.secType === BASIC_AUTH){
      cloneQuery = `
        ${PREFIXES}
        INSERT {
          GRAPH ?g {
            ${sparqlEscapeUri(remoteDataObjectUri)} dgftSec:targetAuthenticationConfiguration ${sparqlEscapeUri(newAuthConfUri)} .
            ${sparqlEscapeUri(newAuthConfUri)} dgftSec:secrets ${sparqlEscapeUri(newBasicCreds)} .
            ${sparqlEscapeUri(newBasicCreds)} meb:username ?user ;
              muAccount:password ?pass .
            ${sparqlEscapeUri(newAuthConfUri)} dgftSec:securityConfiguration ${sparqlEscapeUri(newBasicSecurityScheme)}.
            ${sparqlEscapeUri(newBasicSecurityScheme)} ?srcConfP ?srcConfO.
          }
        }
        WHERE {
          GRAPH ?g {
            ${sparqlEscapeUri(authData.authenticationConfiguration)} dgftSec:securityConfiguration ?srcConfg.
            ?srcConfg ?srcConfP ?srcConfO.
            ${sparqlEscapeUri(authData.authenticationConfiguration)} dgftSec:secrets ?srcSecrets.
            ?srcSecrets  meb:username ?user ;
              muAccount:password ?pass .
          }
        }`;
    }
    else if(authData.secType == OAUTH2){
      cloneQuery = `
        ${PREFIXES}
        INSERT {
          GRAPH ?g {
            ${sparqlEscapeUri(remoteDataObjectUri)} dgftSec:targetAuthenticationConfiguration ${sparqlEscapeUri(newAuthConfUri)} .
            ${sparqlEscapeUri(newAuthConfUri)} dgftSec:secrets ${sparqlEscapeUri(newOauth2Creds)} .
            ${sparqlEscapeUri(newOauth2Creds)} dgftOauth:clientId ?clientId ;
              dgftOauth:clientSecret ?clientSecret .
            ${sparqlEscapeUri(newAuthConfUri)} dgftSec:securityConfiguration ${sparqlEscapeUri(newOauth2SecurityScheme)}.
            ${sparqlEscapeUri(newOauth2SecurityScheme)} ?srcConfP ?srcConfO.
          }
        }
        WHERE {
          GRAPH ?g {
            ${sparqlEscapeUri(authData.authenticationConfiguration)} dgftSec:securityConfiguration ?srcConfg.
            ?srcConfg ?srcConfP ?srcConfO.
            ${sparqlEscapeUri(authData.authenticationConfiguration)} dgftSec:secrets ?srcSecrets.
            ?srcSecrets dgftOauth:clientId ?clientId ;
              dgftOauth:clientSecret ?clientSecret .
              OPTIONAL { ?srcConfig dgftOauth:resource ?resource . }
          }
        }`;
    }
    else {
      throw new Error(`Unsupported Security type ${authData.secType}`);
    }

    await update(cloneQuery);

    return newAuthConfUri;
  }
