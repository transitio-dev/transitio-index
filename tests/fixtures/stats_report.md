# Build statistics

Snapshot `<snapshot>` (sample); schema 7, Overture None.

## Build

The snapshot the statistics describe and the catalogues it read.

| metric | value |
|---|---|
| snapshot_id | <snapshot> |
| stats_schema_version | 1 |
| schema_version | 7 |
| overture_release |  |
| sample | sample |

### sources

| key | archive_sha256 | commit | commit_verified | csv_label | csv_sha256 |
|---|---|---|---|---|---|
| atlas | <archive> | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | False |  |  |
| gbfs |  |  |  | 2026-08-28 | 5beb2ff25a7003704b9b2f35c6605e33f5657e5a76caf1e92f8d431730d002c8 |
| mdb |  |  |  | 2026-08-28 | 0413ed59f046d67263228aa6af7239c098fa5f012a8440ab3d506060b394298f |

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
| rows_into_feeds | 5 |
| feeds | 4 |

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

### feeds by source

| key | value |
|---|---|
| both | 1 |
| atlas | 1 |
| mdb | 1 |
| systems_csv | 1 |

### feeds by crosswalk method

| key | value |
|---|---|
| url_exact | 1 |
| none | 3 |

## Availability

Crawl outcomes per feed, the failure classes from the fetcher's recorded reason, and outcomes by the row's catalogue status.

| metric | value |
|---|---|
| feeds | 4 |
| crawled | 0 |
| with_calendar | 0 |

### by outcome

| key | value |
|---|---|
| not_crawled | 4 |

## Licensing

Licence declarations and the redistribution judgement per feed.


### licence state

| key | value |
|---|---|
| none | 4 |

### redistribution allowed

| key | value |
|---|---|
| unknown | 4 |

## Scale

Stops per crawled feed and places per feed (count, median, 95th percentile, maximum), and countries served per feed.


### stop count

| key | value |
|---|---|
| count | 0 |

### places served

| key | value |
|---|---|
| count | 4 |
| median | 0 |
| p95 | 0 |
| max | 0 |

### countries served

| key | value |
|---|---|
| 0 | 4 |

## Country agreement

The catalogues' declared country against the home country classify found from the stops; the share is disagree over agree plus disagree.


### by agreement

| key | value |
|---|---|
| unobserved | 4 |

### by catalogue

| key | disagreement_share | unobserved |
|---|---|---|
| mdb |  | 2 |
| atlas |  | 2 |
| gbfs |  | 1 |

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

