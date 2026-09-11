// Unit tests of the viewer page's pure helpers, run by pytest through node.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  bboxParam,
  buildLabel,
  collectionBounds,
  overflowText,
  overviewParams,
  paddedBounds,
  placesUrl,
  popupHtml,
  sliceParams,
  summaryLabel,
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
