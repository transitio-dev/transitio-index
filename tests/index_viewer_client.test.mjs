// Unit tests of the viewer page's pure helpers, run by pytest through node.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  INITIAL_TABLE,
  INITIAL_VIEW,
  bboxParam,
  classColorExpression,
  crumbs,
  detailsHtml,
  edgeRowHtml,
  feedColumns,
  feedDetailsHtml,
  feedFilter,
  feedRowHtml,
  feedSliceParams,
  fillColorExpression,
  headerHtml,
  kindOptionsHtml,
  kindRank,
  legendHtml,
  buildLabel,
  collectionBounds,
  overflowText,
  overviewParams,
  paddedBounds,
  placesUrl,
  popupHtml,
  reachColorExpression,
  revealPath,
  rowHtml,
  sliceParams,
  sortRows,
  summaryLabel,
  tableColumns,
  tableState,
  tableUrl,
  treeNodeHtml,
  viewParams,
} from "../scripts/index_viewer.mjs";

const bounds = { west: 19.123456, south: 59, east: 32, north: 71 };

test("a slice URL encodes the build, drops null parameters and floors the zoom", () => {
  assert.equal(placesUrl("es full", {}), "/api/builds/es%20full/places");
  assert.equal(
    placesUrl("fi", { zoom: 6.8, bbox: null, kind: "city", parent_id: "uus" }),
    "/api/builds/fi/places?zoom=6&kind=city&parent_id=uus",
  );
});

test("the overview has no bbox; from zoom 7 the slice follows the padded viewport", () => {
  assert.deepEqual(sliceParams(5, bounds), { zoom: 5 });
  assert.deepEqual(sliceParams(7, bounds), { zoom: 7, bbox: "12.68518,53.00000,38.43827,77.00000" });
  assert.equal(bboxParam(bounds), "19.12346,59.00000,32.00000,71.00000");
  assert.deepEqual(
    paddedBounds({ west: -179, south: 80, east: 179, north: 89 }),
    { west: -180, south: 75.5, east: 180, north: 90 }, // clamped to the world
  );
  // Longitudes past the antimeridian (unwrapped map bounds) are cut at it on
  // both sides, so west never ends up past east.
  assert.deepEqual(
    paddedBounds({ west: 170, south: 0, east: 190, north: 10 }),
    { west: 160, south: -5, east: 180, north: 15 },
  );
  assert.deepEqual(
    paddedBounds({ west: 185, south: 0, east: 190, north: 10 }),
    { west: 180, south: -5, east: 180, north: 15 },
  );
  // A build change loads the overview: never a bbox, never finer than zoom 6.
  assert.deepEqual(overviewParams(12.4), { zoom: 6 });
  assert.deepEqual(overviewParams(3), { zoom: 3 });
});

test("labels name a build by id, date and count, and mark an incomplete one", () => {
  const row = { id: "es-full", complete: true, built_at: "2026-09-11T00:00:00+00:00", counts: { places: 11875 } };
  assert.equal(buildLabel(row), "es-full · 2026-09-11 · 11875 places");
  assert.equal(buildLabel({ id: "half", complete: false }), "half (incomplete)");
  assert.equal(
    summaryLabel({ counts: { places: 6, feeds: 1, edges: 1 }, served_places: 1 }),
    "6 places · 1 feeds · 1 edges · 1 served",
  );
  assert.equal(
    summaryLabel({ counts: { places: 6, feeds: 3, edges: 3 }, served_places: 1, feeds_by_spec: { gtfs: 2, gbfs: 1 } }),
    "6 places · 3 feeds (2 gtfs, 1 gbfs) · 3 edges · 1 served",
  );
});

test("a collection's extent spans every geometry and is null when empty", () => {
  const collection = {
    type: "FeatureCollection",
    features: [
      { geometry: { type: "Polygon", coordinates: [[[19, 59], [32, 59], [32, 71], [19, 59]]] } },
      { geometry: { type: "Point", coordinates: [-5, 80] } },
      { geometry: null },
    ],
  };
  assert.deepEqual(collectionBounds(collection), { west: -5, south: 59, east: 32, north: 80 });
  assert.equal(collectionBounds({ features: [] }), null);
});

test("the popup escapes its text and lists the service stats", () => {
  const html = popupHtml({
    name: "A <b>&</b> B",
    kind: "city",
    place_id: "x",
    served: true,
    feed_count: 2,
    service: '{"stops": 10}',
  });
  assert.match(html, /A &lt;b&gt;&amp;&lt;\/b&gt; B/);
  assert.match(html, /served by 2 feeds/);
  assert.match(html, /<li>stops: 10<\/li>/);
  assert.match(popupHtml({ name: "n", kind: "k", place_id: "p", served: false, service: "nope" }), /not served/);
});

test("the overflow hint names the count and, when known, the size", () => {
  assert.equal(overflowText({ matched: 4200, limit: 3000 }), "4200 places match; zoom in to load at most 3000.");
  assert.equal(
    overflowText({ matched: 800, limit: 3000, bytes: 9 * 1048576 }),
    "800 places match (9.0 MB); zoom in to load at most 3000.",
  );
});

test("from zoom 9 the viewport slice asks for every kind", () => {
  assert.equal(sliceParams(8, bounds).kind, undefined);
  assert.equal(sliceParams(9, bounds).kind, "all");
});

test("a level and a spec reach every request; the defaults add nothing", () => {
  assert.deepEqual(viewParams(INITIAL_VIEW), {});
  const view = { level: "city", spec: "gbfs" };
  assert.deepEqual(viewParams(view), { level: "city", spec: "gbfs" });
  // The level names the kinds, so no kind=all; the city level is bounded at any zoom.
  const slice = sliceParams(3, bounds, view);
  assert.deepEqual([slice.level, slice.spec, slice.kind, typeof slice.bbox], ["city", "gbfs", undefined, "string"]);
  assert.equal(sliceParams(3, bounds, { level: "national", spec: "all" }).bbox, undefined);
  assert.deepEqual(overviewParams(12, view), { zoom: 6, spec: "gbfs" }); // the fit spans the build
  assert.match(tableUrl("b", INITIAL_TABLE, view), /&level=city&spec=gbfs$/);
  assert.equal(feedSliceParams("f1", 9, bounds, view).level, "city");
});

test("the table state resets its page when narrowed and flips a re-sorted column", () => {
  let state = tableState(INITIAL_TABLE, { type: "page", delta: 2 });
  assert.equal(state.offset, 100);
  state = tableState(state, { type: "search", query: "mad" });
  assert.deepEqual([state.query, state.offset], ["mad", 0]);
  state = tableState(tableState(state, { type: "page", delta: 1 }), { type: "sort", sort: "stops" });
  assert.deepEqual([state.sort, state.order, state.offset], ["stops", "asc", 0]);
  state = tableState(state, { type: "sort", sort: "stops" });
  assert.equal(state.order, "desc");
  assert.equal(tableState(state, { type: "page", delta: -3 }).offset, 0);
  state = tableState(state, { type: "filter", field: "kind", value: "city" });
  assert.equal(
    tableUrl("es full", state),
    "/api/builds/es%20full/places/table?sort=stops&order=desc&offset=0&limit=50&q=mad&kind=city",
  );
});

test("table rows and headers render stats, dashes and the sort marker", () => {
  const html = rowHtml({
    place_id: "x<y",
    name: "A & B",
    kind: "city",
    parent_name: null,
    country_code: "FI",
    served: true,
    feed_count: 2,
    stops: 10,
    routes: null,
    departures_per_day: 12.34,
  });
  assert.match(html, /^<tr data-id="x&lt;y">/);
  assert.match(html, /<td>A &amp; B<\/td><td>city<\/td><td>—<\/td><td>FI<\/td><td>yes<\/td><td>2<\/td><td>10<\/td><td>—<\/td><td>12.3<\/td>/);
  assert.match(headerHtml({ sort: "stops", order: "desc" }), /<th data-sort="stops">Stops ▼<\/th>/);
  assert.match(headerHtml({ sort: "stops", order: "desc" }), /<th data-sort="name">Name<\/th>/);
  // The class column follows the build's field, after the feed count.
  assert.deepEqual(tableColumns("category")[6], ["category", "Class"]);
  assert.match(rowHtml({ place_id: "p", feed_count: 1, category: "primary" }, tableColumns("category")), /<td>1<\/td><td>primary<\/td>/);
});

test("the details render the chain, feeds and external ids, escaped", () => {
  const record = {
    properties: {
      place_id: "hel",
      name: "Hel<b>sinki",
      kind: "city",
      served: true,
      feed_count: 1,
      service: { stops: 649, departures_per_day: 41936.01 },
      ancestors: [
        { place_id: "fi", name: "Finland", kind: "country" },
        { place_id: "uus", name: "Uusimaa", kind: "region" },
      ],
      children: { count: 0, by_kind: {}, served: 0 },
      edges: [{ feed_id: "f1", feed_name: "HSL", tier: "local", tier_confidence: 0.85, method: "crawl" }],
      feeds_by_spec: { gtfs: 1 },
      wikidata_id: "Q1757",
      osm_relation_id: null,
    },
  };
  assert.deepEqual(crumbs(record).map((c) => c.place_id), ["fi", "uus", "hel"]);
  const html = detailsHtml(record);
  assert.match(html, /<button type="button" class="crumb" data-id="fi">Finland<\/button> › <button[^>]*data-id="uus">Uusimaa<\/button> › <strong>Hel&lt;b&gt;sinki<\/strong>/);
  assert.match(html, /served by 1 feed \(1 gtfs\)</);
  assert.match(html, /<li>stops: 649<\/li><li>departures_per_day: 41936.0<\/li>/);
  assert.match(html, /<td>HSL<\/td><td>local<\/td><td>—<\/td><td>0.85<\/td><td>crawl<\/td>/);
  // On schema 7 the class is the relevance category, with relevance and the border flag.
  const ranked = { properties: { ...record.properties, reached_from: { international: 1 }, edges: [{ ...record.properties.edges[0], relevance_category: "international", relevance: 0.3, cross_border: true }] } };
  assert.match(detailsHtml(ranked, "category"), /<td>HSL<\/td><td>international<\/td><td>0.30<\/td><td>0.85<\/td><td>crawl · cross-border<\/td>/);
  assert.match(detailsHtml(ranked, "category"), /served by 1 feed \(1 gtfs\) · reached from 1 international</);
  assert.match(html, /href="https:\/\/www.wikidata.org\/wiki\/Q1757"/);
  assert.doesNotMatch(html, /OSM relation/);
  assert.doesNotMatch(html, /children/);
  // Every value of a hostile record is escaped, the counts included.
  const hostile = {
    properties: {
      ...record.properties,
      children: { count: "<img>", by_kind: { "<b>": "<i>" }, served: "<u>" },
    },
  };
  const escaped = detailsHtml(hostile);
  assert.match(escaped, /&lt;img&gt; children \(&lt;i&gt; &lt;b&gt;\), &lt;u&gt; served/);
  assert.doesNotMatch(escaped, /<img>|<b>|<i>|<u>/);
});

test("tree nodes render a caret only with children, nest arrived children and mark the selection", () => {
  const leaf = { place_id: "hel", name: "Hel<sinki", kind: "city", served: true, feed_count: 1, child_count: 0 };
  const html = treeNodeHtml(leaf, "hel");
  assert.match(html, /^<li data-id="hel" aria-selected="true"><span class="caret leaf"><\/span>/);
  assert.match(html, /class="node" data-id="hel">Hel&lt;sinki<\/button><small class="kind">city<\/small> <small class="counts">1 feed<\/small><ul hidden><\/ul><\/li>$/);
  const region = { place_id: "uus", name: "Uusimaa", kind: "region", served: false, feed_count: 0, child_count: 2 };
  assert.match(treeNodeHtml(region, null), /<button type="button" class="caret" data-id="uus" aria-expanded="false">▸<\/button>/);
  assert.match(treeNodeHtml(region, null), /class="node unserved"/);
  assert.match(treeNodeHtml(region, null), /<small class="counts">2 children<\/small>/);
  assert.match(treeNodeHtml({ ...region, feed_count: 3 }, null), /<small class="counts">2 children · 3 feeds<\/small>/);
  assert.doesNotMatch(treeNodeHtml({ ...leaf, feed_count: 0 }, null), /counts/); // nothing to count
  const nested = treeNodeHtml({ ...region, children: [leaf] }, "x");
  assert.match(nested, /aria-expanded="true">▾<\/button>/);
  assert.match(nested, /<ul><li data-id="hel">/);
  assert.deepEqual([kindRank("country"), kindRank("region"), kindRank("city"), kindRank("odd")], [0, 1, 2, 3]);
});

test("the reveal path runs from the root to the place itself", () => {
  const record = {
    properties: {
      place_id: "hel",
      ancestors: [
        { place_id: "fi", name: "Finland", kind: "country" },
        { place_id: "uus", name: "Uusimaa", kind: "region" },
      ],
    },
  };
  assert.deepEqual(revealPath(record), ["fi", "uus", "hel"]);
  assert.deepEqual(revealPath({ properties: { place_id: "fi", ancestors: [] } }), ["fi"]);
});

test("feed rows sort by number or text with nulls last, and filter by name or id", () => {
  const rows = [
    { feed_id: "b", name: "Beta", stop_count: 10 },
    { feed_id: "a", name: null, stop_count: null },
    { feed_id: "c", name: "alpha", stop_count: 200 },
  ];
  assert.deepEqual(sortRows(rows, "stop_count", "desc").map((r) => r.feed_id), ["c", "b", "a"]);
  assert.deepEqual(sortRows(rows, "stop_count", "asc").map((r) => r.feed_id), ["b", "c", "a"]);
  assert.deepEqual(sortRows(rows, "name", "asc").map((r) => r.feed_id), ["c", "b", "a"]);
  assert.deepEqual(sortRows(rows, "name", "desc").map((r) => r.feed_id), ["b", "c", "a"]); // nulls last
  // One fixed collation on every client: accents and case do not split the order.
  const accented = [{ name: "Zaragoza" }, { name: "Évora" }, { name: "alpha" }, { name: "eger" }];
  assert.deepEqual(sortRows(accented, "name").map((r) => r.name), ["alpha", "eger", "Évora", "Zaragoza"]);
  assert.deepEqual(feedFilter(rows, " ALPHA").map((r) => r.feed_id), ["c"]);
  assert.deepEqual(feedFilter(rows, "a").map((r) => r.feed_id), ["b", "a", "c"]);
  assert.equal(feedFilter(rows, ""), rows);
});

test("feed and edge rows render names, dashes and the review flag", () => {
  const feed = { feed_id: "f<1", name: null, spec: "gtfs", source: null, home_country: "FI", scope: "domestic", stop_count: 12, crawl_status: "ok", places_served: 3, tier_local: 2, tier_regional: 0, tier_national: 1, tier_international: 0, tier_unknown: 0 };
  const html = feedRowHtml(feed, feedColumns("tier"));
  assert.match(html, /^<tr data-id="f&lt;1"><td>f&lt;1<\/td><td>gtfs<\/td><td>—<\/td><td>FI<\/td><td>domestic<\/td><td>—<\/td><td>12<\/td><td>ok<\/td><td>3<\/td><td>2<\/td><td>0<\/td><td>1<\/td>/);
  // The count columns follow the field: categories on schema 7, tiers before.
  assert.deepEqual(feedColumns("category").slice(-5).map(([column]) => column), ["category_primary", "category_secondary", "category_tertiary", "category_international", "category_unknown"]);
  assert.match(feedRowHtml({ ...feed, category_primary: 4 }, feedColumns("category")), /<td>3<\/td><td>4<\/td><td>—<\/td>/);
  // Hostile text in every textual field is escaped, none of it survives raw.
  const hostileFeed = { feed_id: "<f>", name: "<n>", spec: "<s>", source: "<o>", stop_count: 1, crawl_status: "<c>", places_served: 0, tier_local: 0, tier_regional: 0, tier_national: 0, tier_international: 0, tier_unknown: 0 };
  const hostileHtml = feedRowHtml(hostileFeed);
  assert.doesNotMatch(hostileHtml, /<f>|<n>|<s>|<o>|<c>/);
  assert.match(hostileHtml, /^<tr data-id="&lt;f&gt;"><td>&lt;n&gt;<\/td><td>&lt;s&gt;<\/td><td>&lt;o&gt;<\/td><td>—<\/td><td>—<\/td><td>—<\/td><td>1<\/td><td>&lt;c&gt;<\/td>/);
  const edge = { place_id: "hel", place_name: "Helsinki", kind: "city", feed_id: "f1", feed_name: "HSL", tier: "local", tier_confidence: 0.85, method: "crawl", needs_review: true };
  assert.equal(edgeRowHtml(edge, "place"), "<tr><td>HSL</td><td>local</td><td>—</td><td>0.85</td><td>crawl</td><td>review</td></tr>");
  assert.match(edgeRowHtml({ ...edge, needs_review: false, tier_confidence: null }, "feed"), /^<tr><td>Helsinki \(city\)<\/td><td>local<\/td><td>—<\/td><td>—<\/td><td>crawl<\/td><td><\/td>/);
  const link = { ...edge, spec: "gbfs", relevance_category: "international", relevance: 0.3, cross_border: true, feed_partition: "<SE>" };
  assert.match(edgeRowHtml(link, "place", "category"), /<td>HSL \(gbfs\)<\/td><td>international<\/td><td>0.30<\/td><td>0.85<\/td><td>crawl<\/td><td>cross-border from &lt;SE&gt; · review<\/td>/);
  const hostileEdge = { place_id: "p", place_name: "<b>P", kind: "<i>", feed_id: "f", feed_name: "<u>F", tier: "<t>", tier_confidence: null, method: "<m>", needs_review: false };
  for (const side of ["place", "feed"]) {
    const html = edgeRowHtml(hostileEdge, side);
    assert.doesNotMatch(html, /<b>|<i>|<u>|<t>|<m>/);
    assert.match(html, side === "place" ? /&lt;u&gt;F/ : /&lt;b&gt;P \(&lt;i&gt;\)/);
    assert.match(html, /<td>&lt;t&gt;<\/td><td>—<\/td><td>—<\/td><td>&lt;m&gt;<\/td>/);
  }
});

test("a feed's details, served-places slice and legend follow its tiers", () => {
  const record = { geometry: null, properties: { feed_id: "f1", name: "HSL", spec: "gtfs", source: "atlas", places_served: 1, tiers: { local: 1, regional: 0 }, stop_count: 649, crawl_status: "ok", last_crawled: "2026-09-10T12:00:00+00:00" } };
  const html = feedDetailsHtml(record);
  assert.match(html, /<strong>HSL<\/strong>/);
  assert.match(html, /1 place served \(1 local\) · 649 stops · no coverage hull/);
  assert.match(html, /crawl: ok \(2026-09-10\)/);
  assert.deepEqual(feedSliceParams("f1", 9.7, bounds), {
    zoom: 9.7,
    kind: "all",
    feed_id: "f1",
    bbox: "12.68518,53.00000,38.43827,77.00000",
  });
  assert.equal(classColorExpression()[0], "match");
  assert.match(legendHtml(), /style="background:#16a34a"><\/span>local/);
  // One palette for both fields: the strongest class of each shares a colour;
  // a served place takes its class colour, an unserved one its kind colour.
  assert.match(legendHtml("category"), /style="background:#16a34a"><\/span>primary/);
  assert.deepEqual(classColorExpression("category").slice(0, 4), ["match", ["get", "category"], "primary", "#16a34a"]);
  const fill = fillColorExpression("category");
  assert.deepEqual([fill[0], fill[2][1], fill[3][1]], ["case", ["get", "category"], ["get", "kind"]]);
  // The international level grades served countries by the feeds reaching them.
  assert.deepEqual(fillColorExpression("category", "international")[2], reachColorExpression());
  assert.deepEqual(reachColorExpression().slice(0, 4), ["interpolate", ["linear"], ["get", "feed_count"], 1]);
  assert.match(legendHtml("category", "international"), /1 feeds from elsewhere .*20\+ feeds from elsewhere$/);
  // Categories replace tiers in a ranked feed's details, with its home and scope.
  const rankedFeed = { geometry: null, properties: { ...record.properties, home_country: "FI", scope: "domestic", categories: { primary: 1, secondary: 0 } } };
  assert.match(feedDetailsHtml(rankedFeed), /1 place served \(1 primary\)/);
  assert.match(feedDetailsHtml(rankedFeed), /home FI · domestic<\/small>/);
});

test("the kind filter lists the build's own kinds in tree order with counts", () => {
  assert.equal(
    kindOptionsHtml({ city: 4689, metro: 3, country: 13, region: 1007 }),
    '<option value="">any kind</option><option value="country">country (13)</option>' +
      '<option value="region">region (1007)</option><option value="city">city (4689)</option>' +
      '<option value="metro">metro (3)</option>',
  );
  assert.equal(kindOptionsHtml(undefined), '<option value="">any kind</option>');
});
