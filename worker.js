const hubspot = require('@hubspot/api-client');
const { queue } = require('async');
const _ = require('lodash');

const { filterNullValuesFromObject, goal } = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({ accessToken: '' });
let expirationDate;


const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        { propertyName, operator: 'GTE', value: `${date.valueOf()}` },
        { propertyName, operator: 'LTE', value: `${nowDate.valueOf()}` }
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

const refreshAccessToken = async (domain, hubId, tryCount) => {
  const { HUBSPOT_CID, HUBSPOT_CS } = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const { accessToken, refreshToken } = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

const fetchMeetingAttendees = async (meetingId, domain, hubId) => {
  try {
    const attendeesResponse = await hubspotClient.apiRequest({
      method: 'GET',
      path: `/crm/v3/objects/meetings/${meetingId}/associations/contacts`,
    });
    const attendees = attendeesResponse.body.results || [];
    return attendees.map(attendee => attendee.properties.email).filter(Boolean);
  } catch (err) {
    console.error('Failed to fetch attendees:', err);
    return [];
  }
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > 2000) {
    console.log('inserting actions to database', { apiKey: domain.apiKey, count: actions.length });

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, 100000000);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions)
  }

  return true;
};

const createSearchObject = (filters, properties, after, sortProperty) => ({
  filterGroups: [filters],
  sorts: [{ propertyName: sortProperty, direction: 'ASCENDING' }],
  properties,
  limit: 100,
  after,
});

const processEntities = async (
  domain,
  hubId,
  q,
  entityName,
  properties,
  entityProcessor
) => {
  console.log(`Start processing ${entityName}`);
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account._doc.lastPulledDates[entityName]);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const filters = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = createSearchObject(filters, properties, offsetObject.after, 'hs_lastmodifieddate');

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= 4) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, 5000 * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log(`Fetched a batch of ${entityName}`);

    for (const item of data) {
      await entityProcessor(item, lastPulledDate, q);
    }

    if (!offsetObject?.after) {
      hasMore = false;
    } else if (offsetObject?.after >= 9900) {
      offsetObject.after = 0;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates[entityName] = now;
  await saveDomain(domain);
};

const processContact = async (contact, lastPulledDate, q) => {
  if (!contact.properties || !contact.properties.email) return;

  const isCreated = new Date(contact.createdAt) > lastPulledDate;
  const userProperties = filterNullValuesFromObject({
    contact_name: `${contact.properties.firstname || ''} ${contact.properties.lastname || ''}`.trim(),
    contact_title: contact.properties.jobtitle,
    contact_source: contact.properties.hs_analytics_source,
    contact_status: contact.properties.hs_lead_status,
    contact_score: parseInt(contact.properties.hubspotscore) || 0,
  });

  q.push({
    actionName: isCreated ? 'Contact Created' : 'Contact Updated',
    actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
    includeInAnalytics: 0,
    identity: contact.properties.email,
    userProperties,
  });
};

const processCompany = async (company, lastPulledDate, q) => {
  if (!company.properties) return;

  const isCreated = !lastPulledDate || new Date(company.createdAt) > lastPulledDate;

  q.push({
    actionName: isCreated ? 'Company Created' : 'Company Updated',
    actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
    includeInAnalytics: 0,
    companyProperties: filterNullValuesFromObject({
      company_id: company.id,
      company_domain: company.properties.domain,
      company_industry: company.properties.industry,
    }),
  });
};

const processMeeting = async (meeting, lastPulledDate, q, fetchMeetingAttendees, domain, hubId) => {
  if (!meeting.properties) return;

  const isCreated = !lastPulledDate || new Date(meeting.createdAt) > lastPulledDate;
  const meetingProperties = filterNullValuesFromObject({
    meeting_id: meeting.id,
    meeting_title: meeting.properties.hs_meeting_title,
    meeting_timestamp: meeting.properties.hs_timestamp,
  });

  const attendees = await fetchMeetingAttendees(meeting.id, domain, hubId);
  attendees.forEach(contactEmail => {
    q.push({
      actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
      actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
      includeInAnalytics: 0,
      identity: contactEmail,
      userProperties: meetingProperties,
    });
  });
};

const contactEntity = {
  properties: [
    'firstname',
    'lastname',
    'jobtitle',
    'email',
    'hubspotscore',
    'hs_lead_status',
    'hs_analytics_source',
    'hs_latest_source',
  ],
  name: 'contacts',
  fnName: processContact,
};

const companyEntity = {
  properties: [
    'name',
    'domain',
    'country',
    'industry',
    'description',
    'annualrevenue',
    'numberofemployees',
    'hs_lead_status',
  ],
  name: 'companies',
  fnName: processCompany,
};

const meetingEntity = {
  properties: [
    'hs_meeting_title',
    'hs_timestamp',
  ],
  name: 'meetings',
  fnName: (meeting, lastPulledDate) => processMeeting(meeting, lastPulledDate, q, fetchMeetingAttendees, domain, hubId),
};

const entitiesToProcess = [contactEntity, companyEntity, meetingEntity];

const pullDataFromHubspot = async () => {
  console.log('Start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('Start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'refreshAccessToken' } });
    }

    const actions = [];
    const q = createQueue(domain, actions);

    for (entity of entitiesToProcess) {
      const { properties, name, fn } = entity;
      try {
        await processEntities(domain, account.hubId, q, name, properties, fn);
      } catch (err) {
        console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'processContacts', hubId: account.hubId } });
      }
    }

    try {
      await drainQueue(domain, actions, q);
    } catch (err) {
      console.log(err, { apiKey: domain.apiKey, metadata: { operation: 'drainQueue', hubId: account.hubId } });
    }

    await saveDomain(domain);

    console.log('Finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;

/**
*  1.- code quality and readability:
*  Code quality can be improved by using more descriptive variable names and comments to explain the purpose of the code blocks. 
*  Also, the code can be refactored to use more functions and modularize the logic. I just did a small refactor to separate the worker logic into the same file.
*  But this can be improved adding helper files, utils, etc.
*  
*  2.- Project architecture, and especially:
*  The project architecture can be improved by separating concerns into different modules and files, such as separating the worker logic into a separate file.
*  Also, the code can be organized into different modules based on functionality, such as API requests, data processing, and database operations.
*  This will make the code easier to maintain and understand.
*  
*  3.- Code performance.
*  The code performance can be improved by optimizing the data processing and API requests, such as batching requests and processing data in parallel.
*  This will reduce the overall execution time and improve the performance of the application. 
*
*  So far the app can be improved by: adding error handling, logging, and monitoring, adding tests to ensure the reliability of the application, improve the code quality, redeability and file structure.
*
*  by the way there's was some vulnerabilities in the package json.
**/