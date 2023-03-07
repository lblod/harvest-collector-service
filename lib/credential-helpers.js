import { PREFIXES, BASIC_AUTH, OAUTH2 } from '../constants';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri, uuid } from 'mu';
import { parseResult } from './utils';

/**
 * Checks the remoteDataObject for an authentication scheme attached.
 *
 * @param {String} remoteDataObjectUri
 * @returns {Boolean} `true` || `false`
 */
export async function hasAuth(remoteDataObjectUri) {

  const findAuthenticationSchemeQuery = `
  ${PREFIXES}
      ASK WHERE {
        GRAPH ?g {
        ${sparqlEscapeUri(remoteDataObjectUri)} dgftSec:targetAuthenticationConfiguration ?authenticationConfiguration.
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
 * Passing the `AuthenticationConfiguration` from the first remoteDataObject to newRemoteDataObjects to allow the download step from `download-url-service`.
 *
 * This is essentially taking the generated `newRemoteDataObjectUri` authenticationCongiguration settings to make a clone from the `firstRemoteDataObjectUri`.
 * @param {String} newRemoteDataObjectUri new remoteDataObject
 * @param {String} firstRemoteDataObjectUri first remoteDataObject
 * @param {String} newAuthConfUri
 * @returns newAuthConfUri
 */
 export async function attachClonedAuthenticationConfiguraton(newRemoteDataObjectUri, remoteDataObjectUri, newAuthConfUri) {
    const getAuthInfoQuery = `
      ${PREFIXES}
      SELECT DISTINCT ?secType ?authenticationConfiguration WHERE {
       GRAPH ?g {
         ${sparqlEscapeUri(remoteDataObjectUri)} dgftSec:targetAuthenticationConfiguration ?authenticationConfiguration.
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
            ${sparqlEscapeUri(newRemoteDataObjectUri)} dgftSec:targetAuthenticationConfiguration ${sparqlEscapeUri(newAuthConfUri)} .
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
            ${sparqlEscapeUri(newRemoteDataObjectUri)} dgftSec:targetAuthenticationConfiguration ${sparqlEscapeUri(newAuthConfUri)} .
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
              dgftOauth:clientSecret ?clientSecret ;
              wotSec:token ?accessTokenUri .
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
