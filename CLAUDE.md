# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project will aggregate complete news archives from the Library of Congress via their JSON API.

For downloading complete archival news records from the Library of Congress using their bulk API, you'll want to focus on the Chronicling America collection. Here are the key endpoints and parameters:

## Primary API Endpoints

**Base URL:** `https://chroniclingamerica.loc.gov/`

**Core Endpoints:**
- `/newspapers.json` - Get list of all newspaper titles
- `/batches.json` - Get information about digitization batches
- `/lccn/{lccn}/issues/{date}/ed-{edition}/seq-{sequence}.json` - Individual page metadata
- `/search/pages/results/` - Search endpoint with JSON output

## Key JSON Parameters for Bulk Downloads

**For Newspaper Discovery:**
```json
{
  "format": "json",
  "rows": 50,
  "page": 1
}
```

**For Batch Processing:**
```json
{
  "format": "json",
  "name": "{batch_name}"
}
```

**For Page-Level Downloads:**
```json
{
  "format": "json",
  "andtext": "",
  "date1": "1836",
  "date2": "1922",
  "sort": "date"
}
```

## Rate Limiting and Bulk Access

The API has rate limits, so for complete archival downloads you should:
- Implement delays between requests (typically 1-2 seconds)
- Use the batch endpoints to understand the full scope before downloading
- Consider requesting institutional access for large-scale harvesting

## Alternative Bulk Methods

**OAI-PMH Harvesting:**
- Endpoint: `https://chroniclingamerica.loc.gov/oai/`
- Supports metadata harvesting with resumption tokens
- Better for systematic bulk collection

**Direct File Access:**
- Many newspapers have downloadable PDF versions
- METS/ALTO XML files available for full text
- JP2 image files for page scans

Structure the API calls for a specific newspaper title and/or date range provided by the user. Give the user valid inputs to choose from.
In addition, provide a running estimate of the total archive size and time required to complete the download.

## Development Setup

This project is in early development. When implementing:

1. **Technology Stack**: Choose appropriate technologies based on requirements (Python for data processing, Node.js for API services, etc.)
2. **Project Structure**: Organize code into logical modules for API clients, data processing, storage, and user interface
3. **Configuration**: Use environment variables for API endpoints, rate limiting, and storage configurations
4. **Error Handling**: Implement robust error handling for network requests, rate limiting, and data validation
5. **Data Storage**: Design efficient storage for large-scale archival data (consider database choice based on query patterns)

## API Integration Guidelines

- Implement rate limiting to respect Library of Congress API limits
- Use batch processing patterns for systematic data collection
- Consider implementing resumable downloads for large datasets
- Validate API responses and handle partial failures gracefully
- Log API usage statistics for monitoring and optimization

# Working with Rate Limits

The API is accessible to the public with no API key or authentication required. Rate limiting is strongly encouraged. Requests that exceed the rate which loc.gov can successfully accommodate will be blocked to prevent a denial of service. It is possible that performance will be impacted by rates that do not exceed the rate limit.

Most programming languages have a library available to automate rate limiting, but it can even be accomplished simply by executing a delay before each network call.

Please note: When the API is experiencing especially heavy traffic load, rate limits may decrease and users may encounter 429 status codes or HTML pages with CAPTCHAs even when operating below the rates listed below. Users may want to add handling to pause when encountering 429 status codes and HTML CAPTCHA responses. 

The current rate limits are:

Rate Limit
Requests per minute	20
Time blocked, if rate is exceeded	1 hour
Working Within Page Limits

## Deep Paging Limitations

Due to the technical limitations of search engine technologies, it is not recommended that users page through a large number of result pages. The deeper you page into a search result, the more memory each page request takes; this process is called "deep paging."

With our current resources, 100,000 is the limit beyond which there will be adverse impacts on overall performance. As a result, paging past the 100,000th item in a search result is not supported at this time, and in some searches, responses may fail before 100,000 items.

One way to limit the size of a result set is by using more specific search terms - for instance, if you are searching for "Lincoln," changing the query parameter from ?q=Lincoln to ?q=Abraham+Lincoln. Another way is to use faceting to ensure that no multi-page result set has more than 100,000 items. This method is also useful to improve response times for result sets with fewer than 100,000 results.

Deep Paging Limits
Maximum items per query (Paging Limits)	100,000 items per query 
Items Per Page

In addition, too many results per page can impact the performance of queries. The default number of items per page is set for each collection or endpoint to address user experience with the website, rather than performance with the API, so it is expected that users of the API will request a larger number of items per page. That said, performance on individual page requests is also affected by the number of items per page. It is recommended that requests not exceed 1,000 items per page.

## Recommended Items Per Page
Maximum items per page (Recommended for most queries)	1,000 items per page 	?fo=json&at=results&c=1000&sp=1
Using Faceting to Limit Result Set Sizes

Each faceted field contains a list of facet filters representing either date ranges (for dates) or the values that occur most often for those fields (often the top ten most common values). Using the URLs for these facet filters will limit the results to only those which have the value (filter.title) selected for by the facet filter. On a results page on the website, these facets appear on the left column of a results page, with each facet filter value linked to the faceted query.

Some faceted fields use a controlled vocabulary (for instance, the Original Format field), while others do not (such as the Subject field). For some items, a faceted field may only have one value (the Online Format field), while other for other facets (such as the Contributors field) it may have one or more values. This second point means that the same item may show up in more than one faceted query for the same search.

Our recommendation for large data results is that rather than paging through one large query, it is more effective to find the facet values for a particular facet for the query and then request and paginate through each facet-filtered query, combining the results locally. In those collections where there are items that appear in more than one faceted search, you can deduplicate result items by the id field.

It may be best to choose for faceting a field that has a limited set of values and where there is less likely to be overlap. While many items do have multiple dates associated with them (for instance, serials and web archives), the dates facet is particularly amenable to this use because it is a limited range of values that should provide complete coverage. For any query, you can facet on a date range by just appending &dates={start_date}/{end_date} to the query. For most items in our collections, the dates are stored as a year.

For fields that have a large number of values, you can retrieve the facets using the /search/index/{field}/ endpoint. For instance, for the search https://www.loc.gov/search/?fa=partof:american+memory, you can paginate through the search index query https://www.loc.gov/search/index/location/?fa=partof:american+memory&fo=json&at=facets,pagination. This will respond with a facets object containing of a list of filters for the field you're interested in, in order of decreasing occurrences. Keep in mind that these facets queries are themselves very resource-intensive, so try not to paginate through too large a list of values.

Looking at the date ranges on the results page https://www.loc.gov/collections/vietnam-era-pow-mia-database/, you will see these values:

Date Range	Item Count
2020 to 2029	322
2010 to 2019	2,930
2000 to 2009	8,392
1990 to 1999	40,149
1980 to 1989	54,904
1970 to 1979	60,942
1960 to 1969	44,682
1950 to 1959	127
1940 to 1949	32
1930 to 1939	5
1920 to 1929	10
1910 to 1919	48

These facets are not necessarily mutually exclusive; if there are collection items that were created over a range of time, for example, from 2008 to 20021, those items would show up in the 2000/2009, 2010/2019, and 2020/2029 facets. So even with date faceted queries, you may need to deduplicate these results by id.

If you request the following query - https://www.loc.gov/collections/vietnam-era-pow-mia-database/?fo=json&at=facets - you'll get the list of the facet filters. In this case, the date filter is the fourth filter (0-indexed):

https://www.loc.gov/collections/vietnam-era-pow-mia-database/?fo=json&at=facets.3

```
  "facets.3": {
    "filters": [
      {
        "count": 322,
        "not": null,
        "off": null,
        "on": "https://www.loc.gov/collections/vietnam-era-pow-mia-database/?dates=2020/2029&fo=json",
        "term": "2020-01-01T00:00:00Z",
        "title": "2020 to 2029"
      },
      {
        "count": 2932,
        "not": null,
        "off": null,
        "on": "https://www.loc.gov/collections/vietnam-era-pow-mia-database/?dates=2010/2019&fo=json",
        "term": "2010-01-01T00:00:00Z",
        "title": "2010 to 2019"
      },
      [… deleted several other filters …]
      {
        "count": 49,
        "not": null,
        "off": null,
        "on": "https://www.loc.gov/collections/vietnam-era-pow-mia-database/?dates=1910/1919&fo=json",
        "term": "1910-01-01T00:00:00Z",
        "title": "1910 to 1919"
      }
    ],
    "title": "Date",
    "type": "dates"
  }
}
```

You can take each of those "on" (i.e., "filter is on") urls and append &at=results&c=1000&sp=1 to get the first 1000 results of that faceted query, and increment the sp parameter to get 1000 results per page until you get fewer than 1000 results, which will be the last page of the result set. (Requesting more than 1000 results per page will also impact performance; if you notice any problems, try scaling down to 500, then 100.)

The search results metadata isn't complete; to get the complete metadata for each time, you'll want to pull the url for each item in the result set to get the complete metadata; for that, you can just append &fo=json&at=item,resources to the url value in the item result. Following are clipped outputs of a faceted query for a search result, then a query for the item record for a single search result.

https://www.loc.gov/collections/vietnam-era-pow-mia-database/?dates=1960/1969&fo=json&at=results&c=1000&sp=2

```
{
  "results": [
    [...deleted multiple fields from result set item...]
    {
      "access_restricted": false,
      "aka": [
        "https://www.loc.gov/item/powmia/pwmaster_103790/",
        "https://www.loc.gov/resource/powmia/pwmaster_103790/"
      ],
      "contributor": [
        "commander us military assistance command vietnam"
      ],
      "date": "1967-07-19",
      "dates": [
        "1967-07-19T00:00:00Z",
        "1966-02-01T00:00:00Z"
      ],
      "image_url": [
        "https://tile.loc.gov/storage-services/service/frd/pwmia/49/103790.gif"
      ],
      "online_format": [
        "pdf"
      ],
      "timestamp": "2022-03-10T23:31:44.885Z",
      "title": "CAPTURED US AIRMAN",
      "url": "https://www.loc.gov/item/powmia/pwmaster_103790/"
    },

https://www.loc.gov/item/powmia/pwmaster_103790/?fo=json&at=item,resources


  [...deleted multiple fields from item...]
  "item": {
    "dates": [
      {
        "1966 to 1967": "https://www.loc.gov/search/?dates=1966/1967&fo=json"
      }
    ],
    "format": [
      {
        "manuscript/mixed material": "https://www.loc.gov/search/?fa=original_format:manuscript/mixed+material&fo=json"
      }
    ],
    "image_url": [
      "https://tile.loc.gov/storage-services/service/frd/pwmia/49/103790.gif"
    ],
    "resources": [
    {
      "files": [

      ],
      "pdf": "https://tile.loc.gov/storage-services/service//frd/pwmia/49/103790.pdf",
      "tif": null,
      "url": "https://www.loc.gov/item/powmia/pwmaster_103790/"
    }
  ]
}
```
