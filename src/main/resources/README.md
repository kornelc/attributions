This challenge is used by MediaMath for evaluating candidates to data engineering positions.

# Background
Advertisers want to understand how digital advertising generates interactions with their brands and websites: clicks, visits, purchases, etc. MediaMath collects such interactions and ties them to the ads that are displayed online through a process called attribution.

# The challenge

## Product specification
The goal of this challenge is to write a simple attribution application that produces a report that can then be fed into a database.

**Definitions:**

 - Event: a user's interaction with a website.
 - Impression: an occurrence of an ad displayed to a user on a website.
 - Attributed event: an event that happened chronologically after an impression and is considered to be the result of that impression. The advertiser and the user of both the impression and the event have to be the same for the event to be attributable. Example: a user buying an object after seeing an ad from an advertiser.

The goal of the application if to attribute events with relevant impressions by running the following two operations on the provided datasets.

### Attribution
The application will process input datasets of events and impressions to compute attributed events and output some simple statistics on them.

The statistics that the application should compute are:

 - The count of attributed events for each advertiser, grouped by event type.
 - The count of unique users that have generated attributed events for each advertiser, grouped by event type.

**Example:**
Consider the following series of events and impressions:

 | Timestamp | Advertiser ID | User ID | Should it be attributed?
------------- | ------------- | ------------- | ------------- | -------------
event | 1450631448  | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No, because there is no impression for the same advertiser and user before this event.
impression | 1450631450  | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No, only events get attributed.
event | 1450631452  | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | Yes, because there is an impression for the same advertiser and user before this event.
event | 1450631464  | 1 | 16340204-80e3-411f-82a1-e154c0845cae | No because there is no impression for this user before.
event | 1450631466  | 2 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No because there is no impression for this user and advertiser before (the advertiser is different).
event | 1450631468  | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | No, because the matching impression (with timestamp 1450631450) has already been matched with event 1450631452.

Here, the statistics generated should be:

 - Advertiser 1: 1 attributed event, 1 unique user
 - Advertiser 2: 0 attributed event, 0 unique user that has generated an attributed
 
As noted in the specification, the count of attributed events should be grouped by event type which has been left out of this example.

### De-duplication
Events are sometimes registered multiple times in the events dataset when they actually should be counted only once. For instance, a user might click on an ad twice by mistake.

When running the attribution process we want to de-duplicate these events: for a given user / advertiser / event type combination, we want to remove events that happen more than once every minute.

**Example with a series of events (more info on the schema below):**

Timestamp | Event ID  | Advertiser ID | User ID | Event Type
------------- | ------------- | ------------- | ------------- | -------------
1450631450  | 5bb2b119-226d-4bdf-95ad-a1cdf9659789 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | click
1450631452  | 23aa6216-3997-4255-9e10-7e37a1f07060 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | click
1450631464  | 61c3ed32-01f9-43c4-8f54-eee3857104cc | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | purchase
1450631466  | 20702cb7-60ca-413a-8244-d22353e2be49 | 1 | 60b74052-fd7e-48e4-aa61-3c14c9c714d5 | click

In this example, the events with timestamps 1450631450, 1450631452 and 1450631466 have less than 60 seconds of difference and are duplicates because they are of the same type (click), for the same advertiser and with the same user ID. The last two events (23aa6216-3997-4255-9e10-7e37a1f07060 and 20702cb7-60ca-413a-8244-d22353e2be49) should be removed and only the first one (5bb2b119-226d-4bdf-95ad-a1cdf9659789) should be kept after de-duplication.

The event with timestamp 1450631464 would not be removed as it's a different event type (purchase vs click) despite being the same advertiser and user.

## Inputs
Two datasets in CSV format are provided for this challenge. Their schemas are provided below.

### Events
This dataset contains a series of interactions of users with brands: [events.csv](events.csv)

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1  | timestamp | integer | Unix timestamp when the event happened.
2  | event_id | string (UUIDv4) | Unique ID for the event.
3  | advertiser_id | integer | The advertiser ID that the user interacted with.
4 | user_id | string (UUIDv4) | An anonymous user ID that generated the event.
5 | event_type | string | The type of event. Potential values: click, visit, purchase

### Impressions
This dataset contains a series of ads displayed to users online for different advertisers: [impressions.csv](impressions.csv)

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1  | timestamp | integer | Unix timestamp when the impression was served.
2  | advertiser_id | integer | The advertiser ID that owns the ad that was displayed.
3 | creative_id | integer | The creative (or ad) ID that was displayed.
4 | user_id | string (UUIDv4) | An anonymous user ID this ad was displayed to.

## Outputs
The attribution application must process the provided datasets and produce the following two CSV files as its output.

### Count of events
The first file must be named `count_of_events.csv` and will contain the **count of events for each advertiser, grouped by event type**.

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1  | advertiser_id | integer | The advertiser ID
2 | event_type | string | The type of event. Potential values: click, visit, purchase
3 | count | int | The count of events for this advertiser ID and event type.

### Count of unique users
The second file must be named `count_of_users.csv` and will contain the **count of unique users for each advertiser, grouped by event type**.

**Schema:**

Column number | Column name  | Type | Description
------------- | ------------- | ------------- | -------------
1 | advertiser_id | integer | The advertiser ID
2 | event_type | string | The type of event. Potential values: click, visit, purchase
3 | count | int | The count of unique users for this advertiser ID and event type.

If a user had generated multiple event types for a given advertiser, the user should be counted multiple times (once for each event type). The sum of the unique users by group can be higher than the unique users of the advertiser across groups.

# Rules of the game
This challenge is a chance for MediaMath engineers to see how you code and organize a project to implement a specification.

## Deliverables
The expected deliverable is a fully functional project that includes the following:

 - Code of the application.
 - Test suite for the application.
 - Documentation for launching a development environment and running the application.
 - Output files (`count_of_events.csv` and `count_of_users.csv`) generated by your application when you run it.

## Technical stack
The application must use the following technologies:

 - Java **or** Scala **or** Python
 - Hadoop **or** Spark

Except for these requirements, feel free to use whichever libraries, frameworks or tools you deem necessary. 

## Expectations
Your code will be reviewed by multiple engineers and can serve as the base for a discussion in interviews.
We want to see how you approach working on a complete project and strongly recommend that you work on this challenge alone.

Feel free to ask any question that might help you understand the specifications or requirements better (as you would in your work) if anything is unclear.

## Delivery
Your application can be sent to us through a GitHub repository (in which case you are welcome to fork this repository) or as a compressed archive containing all the deliverables. 
