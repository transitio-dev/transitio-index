// Unit tests of the viewer page's pure helpers, run by pytest through node.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  INITIAL_TABLE,
  bboxParam,
  crumbs,
  detailsHtml,
  headerHtml,
  kindRank,
  buildLabel,
  collectionBounds,
  overflowText,
  overviewParams,
  paddedBounds,
  placesUrl,
  popupHtml,
  revealPath,
  rowHtml,
  sliceParams,
  summaryLabel,
  tableState,
  tableUrl,
  treeNodeHtml,
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
      wikidata_id: "Q1757",
      osm_relation_id: null,
    },
  };
  assert.deepEqual(crumbs(record).map((c) => c.place_id), ["fi", "uus", "hel"]);
  const html = detailsHtml(record);
  assert.match(html, /<button type="button" class="crumb" data-id="fi">Finland<\/button> › <button[^>]*data-id="uus">Uusimaa<\/button> › <strong>Hel&lt;b&gt;sinki<\/strong>/);
  assert.match(html, /served by 1 feed</);
  assert.match(html, /<li>stops: 649<\/li><li>departures_per_day: 41936.0<\/li>/);
  assert.match(html, /<td>HSL<\/td><td>local<\/td><td>0.85<\/td><td>crawl<\/td>/);
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
