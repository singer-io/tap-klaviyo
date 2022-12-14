---
title: Klaviyo
excerpt: 
category: 62b332e637a6a3004298b8c1
slug: klaviyo
parentDoc: 62b3332a94695c005a9703a2
---

# Description

Klaviyo supports API endpoints that can query and export data and metadata regarding users' interactivity with authenticated clients’ digital products.

For a complete overview, refer to [Klaviyo API](https://developers.klaviyo.com/en)

# Supported Features

| **Feature Name**                                                                        | **Supported** | **Comment** |
| --------------------------------------------------------------------------------------- | ------------- | ----------- |
| [Full Import](https://docs.y42.com/docs/features#full-import)                           | Yes/No        |             |
| [Incremental Import](https://docs.y42.com/docs/features#partial-import)                     | No            |             |
| [Re-Sync](https://docs.y42.com/docs/features#re-sync)                                   | Yes/No        |             |
| [Start Date Selection](https://docs.y42.com/docs/features#start-date-selection)         | Yes/No        |             |
| [Import Empty Tables](https://docs.y42.com/docs/features#import-empty-table)            | Yes/No        |             |
| [Custom Data](https://docs.y42.com/docs/features#custom-data)                           | Yes/No        |             |
| [Retroactive Updating](https://docs.y42.com/docs/features#retroactive-updating)         | Yes/No        |             |
| [Dynamic Column Selection](https://docs.y42.com/docs/features#dynamic-column-selection) | No            |             |

***



# Sync Overview

**Performance Consideration**

- Every Stream has it's own [rate limirations](https://developers.klaviyo.com/en/reference/api-overview#rate-limits). But we handle them, and there should not be any problem during syncronization.

**Edge cases or known limitations**

- For this tables: `Lists Members`, if you have a lot of Members, there should be some delays because of the requests made and rate limitation, because we make request to get Lists and then we do one request for each member to get their info. If import of this table lasts more than 3 days and fails with timeout, please re-run the import or contact us.

- For most of the tables we eleminate duplicated records. 

***



# Connector

This connector was developed following the standards of Singer SDK.  
 For more details, see the [{Singer}]({https://www.singer.io/})

### Authentication

This sources uses API Key as a authentication method

### Rate limits & Pagination

- Every Stream has it's own [rate limirations](https://developers.klaviyo.com/en/reference/api-overview#rate-limits).
- For pagination we use a 'next' property in response. 

***



# Schema

This source is based on the [ Klaviyo API](https://developers.klaviyo.com/en/reference/api-overview), and is currently in the version 2.

## Supported Streams

NOTE: If a stream has a bookmark we will asume the stream supports incremental replication\`

| Name                                                                                   | Description                                                                                                                       | Stream Type |   |
| -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- | ----------- | - |
| [Global Exclusions](https://developers.klaviyo.com/en/reference/get-global-exclusions) | Gets all of the emails and phone numbers that have been excluded from a list along with the exclusion reasons and exclusion time. | FULL_TABLE  |   |
| [Lists](https://developers.klaviyo.com/en/reference/get-lists)                         | Returns a listing of all of the lists in an account.                                                                              | FULL_TABLE  |   |
| [Lists Members](https://developers.klaviyo.com/en/reference/get-members)               | Gets all of the emails, phone numbers, and push tokens for profiles in a given list or segment                                    | FULL_TABLE  |   |
| [Campaigns](https://developers.klaviyo.com/en/reference/get-campaigns)                 | Returns a list of all the campaigns you've created.                                                                               | FULL_TABLE  |   |
| [Profiles](https://developers.klaviyo.com/en/reference/get-profile-id)                 | Get a profile's Klaviyo ID given exactly one corresponding identifier: email, phone_number, or external_id                        | FULL_TABLE  |   |
| [Bounce Emails](https://developers.klaviyo.com/en/reference/metrics-timeline)          | Returns a batched timeline for Bounce metric.                                                                                     | timestamp   |   |
| [Click](https://developers.klaviyo.com/en/reference/metrics-timeline)                  | Returns a batched timeline for Click metric.                                                                                      | timestamp   |   |
| [Dropped Email](https://developers.klaviyo.com/en/reference/metrics-timeline)          | Returns a batched timeline for Dropped metric.                                                                                    | timestamp   |   |
| [Mark As Spam](https://developers.klaviyo.com/en/reference/metrics-timeline)           | Returns a batched timeline for mark as spam metric.                                                                               | timestamp   |   |
| [Receive](https://developers.klaviyo.com/en/reference/metrics-timeline)                | Returns a batched timeline for receive metric.                                                                                    | timestamp   |   |
| [Unsubscribe](https://developers.klaviyo.com/en/reference/metrics-timeline)            | Returns a batched timeline for unsubscribe metric.                                                                                | timestamp   |   |
