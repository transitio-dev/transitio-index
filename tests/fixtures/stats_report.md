# Build statistics

Snapshot `<snapshot>` (sample); schema 9, Overture None.

## Build

The snapshot the statistics describe and the catalogues it read.

| metric | value |
|---|---|
| snapshot_id | <snapshot> |
| stats_schema_version | 3 |
| schema_version | 9 |
| overture_release |  |
| classifier |  |
| sample | sample |

### sources

| key | archive_sha256 | commit | commit_verified | csv_label | csv_sha256 |
|---|---|---|---|---|---|
| atlas | <digest> | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | False |  |  |
| gbfs |  |  |  | 2026-08-28 | <digest> |
| mdb |  |  |  | 2026-08-28 | <digest> |

### catalogue dates

| key | value |
|---|---|
| mdb | 2026-08-28 |
| atlas | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa |
| gbfs | 2026-08-28 |

### catalogue rows

| key | value |
|---|---|
| mdb | 2 |
| atlas |  |
| gbfs | 1 |

## Declared places

What the catalogue rows say about where a feed is, counted over the MDB rows the build ingested; boxes are the declared bounding boxes.

| metric | value |
|---|---|
| mdb_rows | 2 |
| missing_country | 2 |
| missing_subdivision | 2 |
| missing_municipality | 2 |
| subdivision_without_municipality | 0 |
| municipality_repeats_subdivision | 0 |
| municipality_lists_several | 0 |
| missing_bbox | 2 |
| bbox_over_15_degrees | 0 |
| bbox_over_40_degrees | 0 |
| atlas_rows_without_location | 2 |
| gbfs_rows_with_free_text_location | 0 |

## Identity

Catalogue ids, deprecated rows and their redirects, and what the crosswalk made of the rows (feeds by source and match method).

| metric | value |
|---|---|
| deprecated_rows | 0 |
| deprecated_with_redirect | 0 |
| redirect_target_present | 0 |
| redirect_shares_target_url | 0 |
| mdb_rows_without_name | 2 |
| gbfs_duplicate_system_ids | 0 |
| rows_into_feeds | 4 |
| gbfs_systems_kept | 1 |
| feeds | 3 |

### rows by source

| key | value |
|---|---|
| mdb | 2 |
| atlas | 2 |
| gbfs | 1 |

### id namespaces

| key | value |
|---|---|
| mdb | 2 |

### rows dropped by reason

| key | value |
|---|---|
| not_transit | 1 |

### feeds by source

| key | value |
|---|---|
| both | 1 |
| atlas | 1 |
| mdb | 1 |

### feeds by crosswalk method

| key | value |
|---|---|
| url_exact | 1 |
| none | 2 |

## Availability

Crawl outcomes per feed, the failure classes from the fetcher's recorded reason, and outcomes by the row's catalogue status.

| metric | value |
|---|---|
| feeds | 3 |
| crawled | 0 |
| with_calendar | 0 |

### by outcome

| key | value |
|---|---|
| not_crawled | 3 |

## Licensing

Licence declarations and the redistribution judgement per feed.


### licence state

| key | value |
|---|---|
| none | 3 |

### redistribution allowed

| key | value |
|---|---|
| unknown | 3 |

## Realtime

The GTFS-RT companions shipped beside the GTFS feeds: linked to a static feed of the index or not, by source, endpoint entity type and link method, and the static feeds that have one.

| metric | value |
|---|---|
| feeds | 0 |
| linked | 0 |
| unlinked | 0 |
| static_feeds_with_realtime | 0 |

## Scale

Stops per crawled feed and places per feed (count, median, 95th percentile, maximum), and countries served per feed.


### stop count

| key | value |
|---|---|
| count | 0 |

### places served

| key | value |
|---|---|
| count | 3 |
| median | 0 |
| p95 | 0 |
| max | 0 |

### countries served

| key | value |
|---|---|
| 0 | 3 |

## Validity

Feed validity against the build date: dated feeds, the ones valid on it, expired or not started, the valid ones ending within 30 and 90 days (cumulative), span quantiles, and the places whose best window (most valid feeds) contains the build date.

| metric | value |
|---|---|
| build_date | 2026-09-12 |
| feeds_dated | 0 |
| valid | 0 |
| expired | 0 |
| not_started | 0 |
| ending_within_30_days | 0 |
| ending_within_90_days | 0 |
| places_with_dated_feeds | 0 |
| places_best_covers_build_date | 0 |

### service days

| key | value |
|---|---|
| count | 0 |

## Country agreement

The catalogues' declared country against the home country classify found from the stops; the share is disagree over agree plus disagree.


### by agreement

| key | value |
|---|---|
| unobserved | 3 |

### by catalogue

| key | disagreement_share | unobserved |
|---|---|---|
| mdb |  | 2 |
| atlas |  | 2 |
| gbfs |  |  |

## Declared municipality

Where a crawled feed's stops fall relative to the municipality the catalogue declared: in the place, in its region, elsewhere, or unplaceable when the declared place left the index.

| metric | value |
|---|---|
| placed | 0 |

## Duplicate coverage

Feed pairs whose served place sets overlap at the containment threshold (shared places over the smaller set): a catalogue duplicate when one row redirects to the other or they share a download URL.

| metric | value |
|---|---|
| pairs | 0 |
| containment | 0.9 |

## Distributions

Edges by tier and relevance category, and relevance quantiles, per country and per place kind.

