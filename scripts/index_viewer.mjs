// The index viewer's page logic. The pure helpers are exported for Node's
// unit tests; the DOM and map wiring at the bottom runs only in a browser.

const VIEWPORT_ZOOM = 7; // from here the slice follows the viewport
export const CITY_ZOOM = 9; // from here the slice includes cities
const KIND_COLORS = { country: "#6b7280", region: "#2563eb", city: "#f59e0b", metro: "#db2777" };
const KIND_MATCH = ["match", ["get", "kind"], ...Object.entries(KIND_COLORS).flat(), "#999"];

// --- level and spec: the top bar's two selectors, applied to every request
// A level names its place kinds and the edge classes that count on the
// server; the page only passes it on. At the defaults nothing is added.
export const INITIAL_VIEW = { level: "", spec: "all" };

export function viewParams(view) {
  const params = {};
  if (view.level) params.level = view.level;
  if (view.spec && view.spec !== "all") params.spec = view.spec;
  return params;
}

export function placesUrl(build, params = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) continue;
    query.set(key, key === "zoom" ? String(Math.floor(value)) : String(value));
  }
  const suffix = query.size ? `?${query}` : "";
  return `/api/builds/${encodeURIComponent(build)}/places${suffix}`;
}

export function bboxParam(bounds) {
  return [bounds.west, bounds.south, bounds.east, bounds.north]
    .map((value) => value.toFixed(5))
    .join(",");
}

// The viewport grown by half its size on every side, kept inside the world
// on both sides of every axis, so a bbox slice — which the server clips to
// the box — stays continuous while panning until the next refresh. A view
// that reaches past the antimeridian is cut at it (the builds are per
// country; none straddles the date line).
export function paddedBounds(bounds, factor = 0.5) {
  const dx = (bounds.east - bounds.west) * factor;
  const dy = (bounds.north - bounds.south) * factor;
  const lon = (value) => Math.min(180, Math.max(-180, value));
  const lat = (value) => Math.min(90, Math.max(-90, value));
  return {
    west: lon(bounds.west - dx),
    south: lat(bounds.south - dy),
    east: lon(bounds.east + dx),
    north: lat(bounds.north + dy),
  };
}

export function sliceParams(zoom, bounds, view = INITIAL_VIEW) {
  const params = { zoom, ...viewParams(view) };
  // The city level shows cities at any zoom, and a slice with cities is bounded.
  if (zoom >= VIEWPORT_ZOOM || view.level === "city") params.bbox = bboxParam(paddedBounds(bounds));
  if (zoom >= CITY_ZOOM && !view.level) params.kind = "all";
  return params;
}

// The whole-build overview a build change loads before fitting the map to
// it: no bbox, and never finer than the coarsest zoom band, so a large build
// stays under the byte cap whatever the current zoom. No level: the fit spans
// the build, whatever the selectors say; the spec still colours it.
export function overviewParams(zoom, view = INITIAL_VIEW) {
  return { zoom: Math.min(zoom, VIEWPORT_ZOOM - 1), ...viewParams({ ...view, level: "" }) };
}

export function buildLabel(row) {
  if (!row.complete) return `${row.id} (incomplete)`;
  const date = (row.built_at || "").slice(0, 10);
  const places = row.counts && row.counts.places;
  return [row.id, date, places === undefined ? null : `${places} places`]
    .filter(Boolean)
    .join(" · ");
}

export function summaryLabel(summary) {
  const counts = summary.counts || {};
  const specs = Object.entries(summary.feeds_by_spec || {})
    .map(([spec, n]) => `${n} ${spec}`)
    .join(", ");
  return `${counts.places ?? "?"} places · ${counts.feeds ?? "?"} feeds${specs ? ` (${specs})` : ""} · ${
    counts.edges ?? "?"
  } edges · ${summary.served_places ?? "?"} served`;
}

export function collectionBounds(collection) {
  let west = Infinity, south = Infinity, east = -Infinity, north = -Infinity;
  const visit = (coords) => {
    if (typeof coords[0] === "number") {
      west = Math.min(west, coords[0]);
      east = Math.max(east, coords[0]);
      south = Math.min(south, coords[1]);
      north = Math.max(north, coords[1]);
    } else {
      coords.forEach(visit);
    }
  };
  for (const feature of collection.features) {
    if (feature.geometry) visit(feature.geometry.coordinates);
  }
  return west === Infinity ? null : { west, south, east, north };
}

export function escapeHtml(text) {
  return String(text).replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}

export function popupHtml(properties) {
  const served = properties.served
    ? `served by ${properties.feed_count} feed${properties.feed_count === 1 ? "" : "s"}`
    : "not served";
  let service = "";
  try {
    const stats = JSON.parse(properties.service || "null");
    if (stats && typeof stats === "object") {
      service = Object.entries(stats)
        .map(([key, value]) => `<li>${escapeHtml(key)}: ${escapeHtml(value)}</li>`)
        .join("");
    }
  } catch {
    service = "";
  }
  return (
    `<strong>${escapeHtml(properties.name)}</strong> ` +
    `<small>${escapeHtml(properties.kind)} · ${escapeHtml(properties.place_id)}</small>` +
    `<div>${escapeHtml(served)}</div>` +
    (service ? `<ul>${service}</ul>` : "")
  );
}

export function overflowText(record) {
  const size = record.bytes ? ` (${(record.bytes / 1048576).toFixed(1)} MB)` : "";
  return `${record.matched} places match${size}; zoom in to load at most ${record.limit}.`;
}

// --- the places explorer: the table's state and the details renderer ---

export const TABLE_PAGE = 50;
export const INITIAL_TABLE = { query: "", kind: "", served: "", sort: "name", order: "asc", offset: 0 };

// One reducer for the Places tab: every change that narrows or reorders the
// table goes back to the first page; sorting the sorted column flips it.
export function tableState(state, action) {
  switch (action.type) {
    case "search":
      return { ...state, query: action.query, offset: 0 };
    case "filter":
      return { ...state, [action.field]: action.value, offset: 0 };
    case "sort":
      if (state.sort === action.sort) {
        return { ...state, order: state.order === "asc" ? "desc" : "asc", offset: 0 };
      }
      return { ...state, sort: action.sort, order: "asc", offset: 0 };
    case "page":
      return { ...state, offset: Math.max(0, state.offset + action.delta * TABLE_PAGE) };
    default:
      return state;
  }
}

export function tableUrl(build, state, view = INITIAL_VIEW) {
  const params = new URLSearchParams({
    sort: state.sort,
    order: state.order,
    offset: String(state.offset),
    limit: String(TABLE_PAGE),
    ...viewParams(view),
  });
  if (state.query) params.set("q", state.query);
  if (state.kind) params.set("kind", state.kind);
  if (state.served) params.set("served", state.served);
  return `/api/builds/${encodeURIComponent(build)}/places/table?${params}`;
}

const DASH = "—";

export function formatStat(value) {
  if (value === null || value === undefined) return DASH;
  return Number.isInteger(value) ? String(value) : value.toFixed(1);
}

export const TABLE_COLUMNS = [
  ["name", "Name"],
  ["kind", "Kind"],
  ["parent_name", "Parent"],
  ["country_code", "Country"],
  ["served", "Served"],
  ["feed_count", "Feeds"],
  ["stops", "Stops"],
  ["routes", "Routes"],
  ["departures_per_day", "Departures/day"],
];

// The class column follows the build: relevance ``category`` on schema 7,
// ``tier`` before it. The server names the field in its summary.
export function tableColumns(field = "tier") {
  const columns = [...TABLE_COLUMNS];
  columns.splice(6, 0, [field, "Class"]);
  return columns;
}

export function rowHtml(row, columns = TABLE_COLUMNS) {
  const cells = columns.map(([column]) => {
    const value = row[column];
    if (column === "served") return value ? "yes" : "no";
    return typeof value === "number" ? formatStat(value) : escapeHtml(value ?? DASH);
  });
  return `<tr data-id="${escapeHtml(row.place_id)}">${cells.map((c) => `<td>${c}</td>`).join("")}</tr>`;
}

export function headerHtml(state, columns = TABLE_COLUMNS) {
  return columns.map(([column, label]) => {
    const marker = state.sort === column ? (state.order === "asc" ? " ▲" : " ▼") : "";
    return `<th data-sort="${column}">${label}${marker}</th>`;
  }).join("");
}

// The place itself after its ancestors: the chain the details panel shows.
export function crumbs(record) {
  const p = record.properties;
  return [...p.ancestors, { place_id: p.place_id, name: p.name, kind: p.kind }];
}

const EXTERNAL_IDS = [
  ["wikidata_id", "Wikidata", (id) => `https://www.wikidata.org/wiki/${encodeURIComponent(id)}`],
  ["osm_relation_id", "OSM relation", (id) => `https://www.openstreetmap.org/relation/${encodeURIComponent(id)}`],
  ["geonames_id", "GeoNames", (id) => `https://www.geonames.org/${encodeURIComponent(id)}`],
  ["overture_id", "Overture", null],
];

// An edge's class: the relevance category on schema 7, else its tier.
const EDGE_CLASS = { category: "relevance_category", tier: "tier" };
const edgeClass = (edge, field) => edge[EDGE_CLASS[field] ?? "tier"] ?? DASH;
const specLine = (bySpec) =>
  Object.entries(bySpec || {})
    .map(([spec, n]) => `${escapeHtml(n)} ${escapeHtml(spec)}`)
    .join(", ");

export function detailsHtml(record, field = "tier") {
  const p = record.properties;
  const chain = crumbs(record)
    .map((c, i, all) =>
      i === all.length - 1
        ? `<strong>${escapeHtml(c.name)}</strong>`
        : `<button type="button" class="crumb" data-id="${escapeHtml(c.place_id)}">${escapeHtml(c.name)}</button>`,
    )
    .join(" › ");
  const specs = specLine(p.feeds_by_spec);
  const served = p.served
    ? `served by ${p.feed_count} feed${p.feed_count === 1 ? "" : "s"}${specs ? ` (${specs})` : ""}`
    : "not served";
  const stats = Object.entries(p.service || {})
    .map(([key, value]) => `<li>${escapeHtml(key)}: ${escapeHtml(formatStat(value))}</li>`)
    .join("");
  const children = p.children && p.children.count
    ? `<p>${escapeHtml(p.children.count)} children (${Object.entries(p.children.by_kind || {})
        .map(([kind, n]) => `${escapeHtml(n)} ${escapeHtml(kind)}`)
        .join(", ")}), ${escapeHtml(p.children.served)} served</p>`
    : "";
  const feeds = (p.edges || [])
    .map(
      (e) =>
        `<tr><td>${escapeHtml(e.feed_name ?? e.feed_id)}</td><td>${escapeHtml(edgeClass(e, field))}</td>` +
        `<td>${e.relevance == null ? DASH : e.relevance.toFixed(2)}</td>` +
        `<td>${e.tier_confidence == null ? DASH : e.tier_confidence.toFixed(2)}</td>` +
        `<td>${escapeHtml(e.method ?? DASH)}${e.cross_border ? " · cross-border" : ""}</td></tr>`,
    )
    .join("");
  const ids = EXTERNAL_IDS.filter(([key]) => p[key])
    .map(([key, label, link]) =>
      link
        ? `<li>${label}: <a href="${link(p[key])}" target="_blank" rel="noopener">${escapeHtml(p[key])}</a></li>`
        : `<li>${label}: ${escapeHtml(p[key])}</li>`,
    )
    .join("");
  return (
    `<p class="chain">${chain}</p>` +
    `<p><small>${escapeHtml(p.kind)} · ${escapeHtml(p.place_id)}</small> · ${served}</p>` +
    (stats ? `<ul class="stats">${stats}</ul>` : "") +
    children +
    (feeds
      ? `<table class="feeds"><thead><tr><th>Feed</th><th>Class</th><th>Rel.</th><th>Conf.</th><th>Method</th></tr></thead><tbody>${feeds}</tbody></table>`
      : "") +
    (ids ? `<ul class="ids">${ids}</ul>` : "")
  );
}

// --- the hierarchy tree: one node per place, expanded lazily

export const KIND_RANK = { country: 0, region: 1, city: 2 };

export function kindRank(kind) {
  return KIND_RANK[kind] ?? Object.keys(KIND_RANK).length;
}

// A node's list item: a caret only where there are children (its <ul> is
// filled when expanded, or already when the node arrived with children), the
// name as the button that selects the place, and the counts that are not
// zero — children below it, feeds serving it — as a hint.
export function treeNodeHtml(node, selectedId) {
  const id = escapeHtml(node.place_id);
  const expanded = Array.isArray(node.children);
  const caret = node.child_count
    ? `<button type="button" class="caret" data-id="${id}" aria-expanded="${expanded}">${expanded ? "▾" : "▸"}</button>`
    : '<span class="caret leaf"></span>';
  const counts = [
    node.child_count ? `${escapeHtml(node.child_count)} children` : null,
    node.feed_count ? `${escapeHtml(node.feed_count)} feed${node.feed_count === 1 ? "" : "s"}` : null,
  ]
    .filter(Boolean)
    .join(" · ");
  const count = counts ? ` <small class="counts">${counts}</small>` : "";
  const selected = node.place_id === selectedId ? ' aria-selected="true"' : "";
  const children = expanded ? node.children.map((c) => treeNodeHtml(c, selectedId)).join("") : "";
  return (
    `<li data-id="${id}"${selected}>${caret}` +
    `<button type="button" class="node${node.served ? "" : " unserved"}" data-id="${id}">` +
    `${escapeHtml(node.name)}</button><small class="kind">${escapeHtml(node.kind)}</small>${count}` +
    `<ul${expanded ? "" : " hidden"}>${children}</ul></li>`
  );
}

// The ids to expand, root first, ending with the place itself.
export function revealPath(record) {
  return [...record.properties.ancestors.map((a) => a.place_id), record.properties.place_id];
}

// --- feeds and coverage: the feeds table, a feed's served places by tier, edges

// The classes of each field in rank order, and one palette for both: the
// first class of either field is the strongest.
export const CLASSES = {
  category: ["primary", "secondary", "tertiary", "international", "unknown"],
  tier: ["local", "regional", "national", "international", "unknown"],
};
const CLASS_COLORS = ["#16a34a", "#2563eb", "#9333ea", "#dc2626", "#6b7280"];
const CLASS_SHORT = { category: ["P", "S", "T", "I", "?"], tier: ["L", "R", "N", "I", "?"] };

export function classColors(field = "tier") {
  return Object.fromEntries(CLASSES[field].map((name, i) => [name, CLASS_COLORS[i]]));
}

// The colour expression for a class the slice carries: the place's own class
// under the level and spec, or the tier of the selected feed's outline.
export function classColorExpression(field = "tier") {
  return ["match", ["get", field], ...Object.entries(classColors(field)).flat(), CLASS_COLORS[4]];
}

// A served place in its class colour, an unserved one in its kind colour.
export function fillColorExpression(field = "tier") {
  return ["case", ["to-boolean", ["get", "served"]], classColorExpression(field), KIND_MATCH];
}

export function legendHtml(field = "tier") {
  return Object.entries(classColors(field))
    .map(([name, color]) => `<span class="swatch" style="background:${color}"></span>${name}`)
    .join(" ");
}

// Client-side sorting for the small feeds table: numbers numerically,
// strings by one fixed collation (case- and accent-insensitive, the same on
// every client), nulls last whatever the order.
const COLLATOR = new Intl.Collator("en", { sensitivity: "base" });
export function sortRows(rows, column, order = "asc") {
  const direction = order === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const x = a[column];
    const y = b[column];
    if (x == null && y == null) return 0;
    if (x == null) return 1;
    if (y == null) return -1;
    if (typeof x === "number" && typeof y === "number") return (x - y) * direction;
    return COLLATOR.compare(String(x), String(y)) * direction;
  });
}

export function feedFilter(rows, query) {
  const needle = query.trim().toLowerCase();
  if (!needle) return rows;
  return rows.filter((row) =>
    [row.name, row.feed_id].some((value) => value && String(value).toLowerCase().includes(needle)),
  );
}

export const FEED_COLUMNS = [
  ["name", "Feed"],
  ["spec", "Spec"],
  ["source", "Source"],
  ["home_country", "Home"],
  ["scope", "Scope"],
  ["stop_count", "Stops"],
  ["crawl_status", "Crawl"],
  ["places_served", "Places"],
];

// The feed table's columns end with the edge counts per class of ``field``.
export function feedColumns(field = "tier") {
  return [...FEED_COLUMNS, ...CLASSES[field].map((name, i) => [`${field}_${name}`, CLASS_SHORT[field][i]])];
}

export function feedRowHtml(row, columns = FEED_COLUMNS) {
  const cells = columns.map(([column]) => {
    const value = column === "name" ? (row.name ?? row.feed_id) : row[column];
    return typeof value === "number" ? formatStat(value) : escapeHtml(value ?? DASH);
  });
  return `<tr data-id="${escapeHtml(row.feed_id)}">${cells.map((c) => `<td>${c}</td>`).join("")}</tr>`;
}

// One edge row seen from a place (the feed side) or from a feed (the place side).
export function edgeRowHtml(row, side, field = "tier") {
  const other = side === "place" ? (row.feed_name ?? row.feed_id) : `${row.place_name ?? row.place_id} (${row.kind ?? DASH})`;
  const confidence = row.tier_confidence == null ? DASH : row.tier_confidence.toFixed(2);
  const flags = [row.cross_border ? "cross-border" : null, row.needs_review ? "review" : null].filter(Boolean);
  return (
    `<tr><td>${escapeHtml(other)}</td><td>${escapeHtml(edgeClass(row, field))}</td>` +
    `<td>${row.relevance == null ? DASH : row.relevance.toFixed(2)}</td>` +
    `<td>${confidence}</td><td>${escapeHtml(row.method ?? DASH)}</td>` +
    `<td>${flags.join(" · ")}</td></tr>`
  );
}
export const EDGE_HEADERS = "<th>Class</th><th>Rel.</th><th>Conf.</th><th>Method</th><th></th>";

export function feedDetailsHtml(record) {
  const p = record.properties;
  const tiers = Object.entries(p.categories || p.tiers || {})
    .filter(([, n]) => n)
    .map(([name, n]) => `${escapeHtml(n)} ${escapeHtml(name)}`)
    .join(", ");
  return (
    `<p class="chain"><strong>${escapeHtml(p.name ?? p.feed_id)}</strong></p>` +
    `<p><small>${escapeHtml(p.spec ?? DASH)} · ${escapeHtml(p.source ?? DASH)} · ${escapeHtml(p.feed_id)}` +
    `${p.home_country ? ` · home ${escapeHtml(p.home_country)}` : ""}${p.scope ? ` · ${escapeHtml(p.scope)}` : ""}</small></p>` +
    `<p>${escapeHtml(p.places_served)} place${p.places_served === 1 ? "" : "s"} served${tiers ? ` (${tiers})` : ""}` +
    `${p.stop_count == null ? "" : ` · ${formatStat(p.stop_count)} stops`}` +
    `${record.geometry ? "" : " · no coverage hull"}</p>` +
    `${p.crawl_status ? `<p>crawl: ${escapeHtml(p.crawl_status)}${p.last_crawled ? ` (${escapeHtml(String(p.last_crawled).slice(0, 10))})` : ""}</p>` : ""}`
  );
}

// The kind filter's options from the build's own kinds (a build may carry
// kinds beyond country/region/city, such as metro), in tree order with counts.
export function kindOptionsHtml(byKind) {
  const kinds = Object.entries(byKind || {}).sort(
    ([a], [b]) => kindRank(a) - kindRank(b) || a.localeCompare(b),
  );
  return (
    '<option value="">any kind</option>' +
    kinds
      .map(([kind, n]) => `<option value="${escapeHtml(kind)}">${escapeHtml(kind)} (${escapeHtml(n)})</option>`)
      .join("")
  );
}

// The slice that outlines a feed's served places within the padded viewport.
export function feedSliceParams(feedId, zoom, bounds, view = INITIAL_VIEW) {
  return { zoom, kind: "all", feed_id: feedId, bbox: bboxParam(paddedBounds(bounds)), ...viewParams(view) };
}

const OSM_STYLE = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "© OpenStreetMap contributors",
    },
  },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};
const EMPTY = { type: "FeatureCollection", features: [] };
const FILL_PAINT = {
  "fill-color": fillColorExpression(),
  "fill-opacity": ["case", ["get", "served"], 0.45, 0.12],
};
const LINE_PAINT = { "line-color": KIND_MATCH, "line-width": 1 };

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  return response.json();
}

// A slice, with the snapshot digest the server stamped on it.
async function fetchSlice(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  return { data: await response.json(), snapshot: response.headers.get("x-snapshot") };
}

async function main() {
  const select = document.getElementById("build");
  const stats = document.getElementById("stats");
  const hint = document.getElementById("hint");
  const panel = document.getElementById("panel");
  const details = document.getElementById("details");
  const tableHead = document.querySelector("#places-table thead tr");
  const tableBody = document.querySelector("#places-table tbody");
  const tableSearch = document.getElementById("places-search");
  const tableKind = document.getElementById("places-kind");
  const tableServed = document.getElementById("places-served");
  const tableCount = document.getElementById("places-count");
  const tablePrev = document.getElementById("places-prev");
  const tableNext = document.getElementById("places-next");
  const PLACEHOLDER = details.innerHTML;
  const treeRoot = document.getElementById("tree-root");
  const legend = document.getElementById("legend");
  const feedsHead = document.querySelector("#feeds-table thead tr");
  const feedsBody = document.querySelector("#feeds-table tbody");
  const feedsSearch = document.getElementById("feeds-search");
  const edgesTitle = document.getElementById("edges-title");
  const edgesHead = document.querySelector("#edges-table thead tr");
  const edgesBody = document.querySelector("#edges-table tbody");
  const treeSection = document.getElementById("tree");
  const levelSelect = document.getElementById("level");
  const specSelect = document.getElementById("spec");
  let view = { ...INITIAL_VIEW }; // the level and spec every request carries
  let classField = "tier"; // the build's class field, from its summary
  let selectedId = null; // the selected place, marked in the tree
  let selectedRecord = null; // `{ record, mine }`: revealed once the tree has loaded
  let pendingScroll = null; // the tree item to scroll to when the Tree tab is next shown
  const builds = await fetchJson("/api/builds");
  for (const row of builds) {
    const option = document.createElement("option");
    option.value = row.id;
    option.textContent = buildLabel(row);
    option.disabled = !row.complete;
    select.append(option);
  }
  const map = new maplibregl.Map({
    container: "map",
    style: OSM_STYLE,
    center: [10, 30],
    zoom: 1.5,
    renderWorldCopies: false, // one world: bounds stay within ±180
  });
  map.addControl(new maplibregl.NavigationControl());
  let current = null; // the selected build id
  let snapshot = null; // the digest of the summary's snapshot
  let generation = 0; // bumped on every build change
  let sequence = 0; // bumped on every slice request
  let fitting = false; // a build change is fitting the map: moveend waits for its own fit
  let loadSelection = 0; // the selection sequence when the current build load began

  const boundsOf = () => {
    const b = map.getBounds();
    return { west: b.getWest(), south: b.getSouth(), east: b.getEast(), north: b.getNorth() };
  };
  // A slice that overflows or fails clears only the places layer; the
  // selection outlines go with a build change or another selection.
  const clearPlaces = () => map.getSource("places").setData(EMPTY);
  const clear = () => {
    for (const name of ["places", "selected", "ancestors", "hull", "served"]) {
      map.getSource(name).setData(EMPTY);
    }
  };
  const showHint = (text) => {
    hint.textContent = text;
    hint.hidden = false;
  };
  // A failure of a request that is still current clears the screen and shows
  // the error where the overflow hint goes; a superseded one says nothing.
  const fail = (error) => {
    clearPlaces();
    stats.textContent = "";
    showHint(String(error));
  };

  // ``fit``: a build change loads the whole overview (no bbox, coarse) and
  // fits the map to it; the fit's moveend then requests the viewport slice.
  async function refresh(build, gen, fit) {
    const mine = ++sequence;
    const params = fit ? overviewParams(map.getZoom(), view) : sliceParams(map.getZoom(), boundsOf(), view);
    let slice;
    try {
      slice = await fetchSlice(placesUrl(build, params));
    } catch (error) {
      if (mine === sequence && gen === generation) {
        fitting = false;
        fail(error);
      }
      return;
    }
    if (mine !== sequence || gen !== generation) return; // superseded
    if (slice.snapshot !== snapshot) {
      // ``latest`` was republished between the summary and this slice:
      // start over so counts and map describe one snapshot.
      startLoad(build);
      return;
    }
    const data = slice.data;
    if (data.overflow) {
      clearPlaces();
      showHint(overflowText(data));
      fitting = false;
      return;
    }
    hint.hidden = true;
    map.getSource("places").setData(data);
    if (fit && selectionSequence !== loadSelection) {
      // A selection made while the overview loaded has moved the map already:
      // its zoom wins, and the viewport slice follows right away.
      fitting = false;
      refresh(build, gen, false).catch(console.error);
      return;
    }
    const extent = fit && collectionBounds(data);
    if (extent) {
      // The fit's own moveend requests the first viewport slice; until then
      // moveend is ignored, so an older animation cannot supersede this fit.
      map.once("moveend", () => {
        if (gen !== generation) return;
        fitting = false;
        refresh(build, gen, false).catch(console.error);
      });
      map.fitBounds([extent.west, extent.south, extent.east, extent.north], { padding: 24 });
    } else if (fit) {
      fitting = false;
    }
  }

  async function loadBuild(id, gen) {
    sequence++; // any slice still in flight belongs to the old build
    fitting = true;
    map.stop(); // end the previous build's camera animation now, not later
    current = id;
    clear();
    stats.textContent = "";
    hint.hidden = true;
    // The panel forgets the old build at once: nothing of it stays clickable
    // while the new summary loads, and selections or pages still in flight
    // are dropped when they land.
    loadSelection = ++selectionSequence;
    tableSequence++;
    recordCache.clear();
    dropPopup();
    details.innerHTML = PLACEHOLDER;
    table = { ...INITIAL_TABLE };
    tableSearch.value = tableKind.value = tableServed.value = "";
    tableHead.innerHTML = tableBody.innerHTML = "";
    tableCount.textContent = "";
    tablePrev.disabled = tableNext.disabled = true;
    selectedId = null;
    treeRoot.innerHTML = "";
    selectedFeed = null;
    feedCache.clear();
    feedRows = [];
    feedsSearch.value = feedQuery = "";
    edgesTitle.textContent = "Nothing selected.";
    edgesHead.innerHTML = edgesBody.innerHTML = "";
    feedsHead.innerHTML = feedsBody.innerHTML = "";
    selectedRecord = null;
    pendingScroll = null;
    let summary;
    try {
      summary = await fetchJson(`/api/builds/${encodeURIComponent(id)}/summary`);
    } catch (error) {
      if (gen === generation) {
        fitting = false;
        fail(error);
      }
      return;
    }
    if (gen !== generation) return;
    snapshot = summary.snapshot_id;
    stats.textContent = summaryLabel(summary);
    classField = summary.category_field ?? "tier";
    legend.innerHTML = legendHtml(classField);
    map.setPaintProperty("places-fill", "fill-color", fillColorExpression(classField));
    tableKind.innerHTML = kindOptionsHtml((summary.counts || {}).places_by_kind);
    loadTable().catch(console.error);
    loadTree().catch(console.error);
    loadFeeds().catch(console.error);
    await refresh(id, gen, true);
  }

  function startLoad(id) {
    loadBuild(id, ++generation).catch(console.error);
  }

  // --- selection: one record per click, outlined, with its ancestors dashed
  // Records are cached per build *and* snapshot, as `{ data, snapshot }`, so
  // a republished `latest` can never serve a record from the old snapshot.
  const recordCache = new Map();
  let selectionSequence = 0; // bumped on every selection and build change
  let popup = null; // the map popup of the last click, if any
  const fetchRecord = (build, id) => {
    const key = `${build}/${snapshot}/${id}`;
    if (!recordCache.has(key)) {
      const url = `/api/builds/${encodeURIComponent(build)}/places/${encodeURIComponent(id)}`;
      const pending = fetchSlice(url).catch((error) => {
        if (recordCache.get(key) === pending) recordCache.delete(key); // retry later
        throw error;
      });
      recordCache.set(key, pending);
    }
    return recordCache.get(key);
  };
  const dropPopup = () => {
    if (popup) popup.remove();
    popup = null;
  };
  const showTab = (name) => {
    for (const tab of panel.querySelectorAll(".tab")) tab.hidden = tab.id !== name;
    for (const button of panel.querySelectorAll("nav button")) {
      button.classList.toggle("active", button.dataset.tab === name);
    }
    if (name === "tree" && pendingScroll) {
      // A hidden section has no layout to scroll: the reveal left this for now.
      pendingScroll.scrollIntoView({ block: "nearest" });
      pendingScroll = null;
    }
  };

  // One selection at a time: a later click supersedes an earlier one still
  // loading, and a record from another snapshot restarts the build load. A
  // popup belongs to the map click that opened it: it is filled only while
  // that selection is current, and any other selection removes it.
  async function selectPlace(id, { zoom, fromPopup = null }) {
    const build = current;
    const gen = generation;
    const mine = ++selectionSequence;
    const wanted = () => gen === generation && mine === selectionSequence;
    if (fromPopup) popup = fromPopup;
    else dropPopup();
    resetEdges("Loading…");
    clearFeedSelection(); // a place click supersedes a feed at once
    renderFeeds();
    details.innerHTML = '<p class="muted">Loading…</p>'; // no superseded details linger
    let reply;
    try {
      reply = await fetchRecord(build, id);
    } catch (error) {
      if (wanted()) showHint(String(error));
      return;
    }
    if (!wanted()) return;
    if (reply.snapshot !== snapshot) {
      startLoad(build);
      return;
    }
    const record = reply.data;
    selectedId = record.properties.place_id;
    loadEdges({ place_id: id }, record.properties.name, mine).catch(console.error);
    selectedRecord = { record, mine }; // with the sequence it was accepted under
    details.innerHTML = detailsHtml(record, classField);
    revealInTree(record, mine).catch(console.error);
    if (fromPopup && popup === fromPopup && popup.isOpen()) popup.setHTML(detailsHtml(record, classField));
    showTab("details");
    map.getSource("selected").setData(record);
    map.getSource("ancestors").setData(EMPTY); // the old chain goes with the old selection
    const box = record.properties.bbox;
    if (zoom && Array.isArray(box) && box.length === 4 && box.every(Number.isFinite)) {
      map.fitBounds(box, { padding: 40, maxZoom: 12 });
    }
    const replies = await Promise.all(
      record.properties.ancestors.map((a) => fetchRecord(build, a.place_id).catch(() => null)),
    );
    if (!wanted()) return;
    if (replies.some((r) => r && r.snapshot !== snapshot)) {
      startLoad(build); // an ancestor from another snapshot: start over
      return;
    }
    const features = replies.filter(Boolean).map((r) => r.data);
    map.getSource("ancestors").setData({ type: "FeatureCollection", features });
  }

  // --- the Places tab: the whole build, paged and sorted by the server
  let table = { ...INITIAL_TABLE };
  let tableSequence = 0;

  async function loadTable() {
    if (!current) return;
    const build = current;
    const gen = generation;
    const mine = ++tableSequence;
    const columns = tableColumns(classField);
    tableHead.innerHTML = headerHtml(table, columns);
    let page;
    try {
      page = await fetchSlice(tableUrl(build, table, view));
    } catch (error) {
      if (gen === generation && mine === tableSequence) showHint(String(error));
      return;
    }
    if (gen !== generation || mine !== tableSequence) return;
    if (page.snapshot !== snapshot) {
      startLoad(build); // ``latest`` was republished: one snapshot for all
      return;
    }
    page = page.data;
    tableBody.innerHTML = page.rows.map((row) => rowHtml(row, columns)).join("");
    const last = page.offset + page.rows.length;
    tableCount.textContent = `${page.total ? page.offset + 1 : 0}–${last} of ${page.total}`;
    tablePrev.disabled = page.offset === 0;
    tableNext.disabled = last >= page.total;
  }
  const dispatch = (action) => {
    table = tableState(table, action);
    loadTable().catch(console.error);
  };
  let searchTimer = null;
  tableSearch.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => dispatch({ type: "search", query: tableSearch.value.trim() }), 250);
  });
  tableKind.addEventListener("change", () => dispatch({ type: "filter", field: "kind", value: tableKind.value }));
  tableServed.addEventListener("change", () =>
    dispatch({ type: "filter", field: "served", value: tableServed.value }),
  );
  tableHead.addEventListener("click", (event) => {
    const header = event.target.closest("th[data-sort]");
    if (header) dispatch({ type: "sort", sort: header.dataset.sort });
  });
  tablePrev.addEventListener("click", () => dispatch({ type: "page", delta: -1 }));
  tableNext.addEventListener("click", () => dispatch({ type: "page", delta: 1 }));
  tableBody.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-id]");
    if (row) selectPlace(row.dataset.id, { zoom: true }).catch(console.error);
  });
  details.addEventListener("click", (event) => {
    const crumb = event.target.closest(".crumb");
    if (crumb) selectPlace(crumb.dataset.id, { zoom: true }).catch(console.error);
  });
  for (const button of panel.querySelectorAll("nav button")) {
    button.addEventListener("click", () => showTab(button.dataset.tab));
  }

  // --- the Tree tab: roots with their first level, deeper levels on demand
  const treeUrl = (build, root) =>
    `/api/builds/${encodeURIComponent(build)}/tree?` +
    new URLSearchParams(root ? { root } : { depth: "2" });

  async function loadTree() {
    const build = current;
    const gen = generation;
    let reply;
    try {
      reply = await fetchSlice(treeUrl(build, null));
    } catch (error) {
      if (gen === generation) showHint(String(error));
      return;
    }
    if (gen !== generation) return;
    if (reply.snapshot !== snapshot) {
      startLoad(build); // ``latest`` was republished: one snapshot for all
      return;
    }
    treeRoot.innerHTML = reply.data.nodes.map((n) => treeNodeHtml(n, selectedId)).join("");
    // A place selected before the roots arrived is revealed now — only if
    // that selection is still the current one.
    if (selectedRecord && selectedRecord.mine === selectionSequence) {
      revealInTree(selectedRecord.record, selectedRecord.mine).catch(console.error);
    }
  }

  // Fills a node's <ul> once and shows it. Concurrent callers share one
  // fetch (keyed by generation, snapshot and node, so a build change never
  // reuses an old request); every caller checks ``wanted`` after the await
  // and before filling or opening, so a superseded reveal fills and opens
  // nothing; a list filled meanwhile is never re-rendered, so a marked or
  // pending node inside it stays attached.
  const expanding = new Map(); // `${generation}/${snapshot}/${id}` -> the pending fetch
  async function expandNode(item, wanted = () => true) {
    const list = item.querySelector(":scope > ul");
    const caret = item.querySelector(":scope > .caret");
    if (!caret) return list;
    if (!list.children.length) {
      const build = current;
      const gen = generation;
      const key = `${gen}/${snapshot}/${item.dataset.id}`;
      if (!expanding.has(key)) {
        const fetching = fetchSlice(treeUrl(build, item.dataset.id)).finally(() => {
          if (expanding.get(key) === fetching) expanding.delete(key);
        });
        expanding.set(key, fetching);
      }
      const reply = await expanding.get(key);
      if (gen !== generation || !wanted()) return list;
      if (reply.snapshot !== snapshot) {
        startLoad(build);
        return list;
      }
      if (!list.children.length) {
        list.innerHTML = reply.data.nodes.map((n) => treeNodeHtml(n, selectedId)).join("");
      }
    } else if (!wanted()) {
      return list;
    }
    caret.setAttribute("aria-expanded", "true");
    caret.textContent = "▾";
    list.hidden = false;
    return list;
  }

  function collapseNode(item) {
    item.querySelector(":scope > ul").hidden = true;
    const caret = item.querySelector(":scope > .caret");
    caret.setAttribute("aria-expanded", "false");
    caret.textContent = "▸";
  }

  // Expands the selection's ancestors in order, marks its node and scrolls to
  // it — at once when the Tree tab is visible, else when it is next shown.
  // Guarded by the selection sequence, so a slower reveal of an earlier
  // selection never touches a newer one; a place selected before the roots
  // loaded is revealed by loadTree afterwards.
  async function revealInTree(record, mine) {
    const gen = generation;
    const wanted = () => gen === generation && mine === selectionSequence;
    let item = null;
    for (const id of revealPath(record)) {
      const next = treeRoot.querySelector(`li[data-id="${CSS.escape(id)}"]`);
      if (!next) break;
      item = next;
      if (id !== record.properties.place_id) await expandNode(item, wanted);
      if (!wanted()) return;
    }
    if (!item || item.dataset.id !== record.properties.place_id) return; // not loaded yet
    for (const marked of treeRoot.querySelectorAll("[aria-selected]")) marked.removeAttribute("aria-selected");
    item.setAttribute("aria-selected", "true");
    if (treeSection.hidden) pendingScroll = item;
    else item.scrollIntoView({ block: "nearest" });
  }

  treeRoot.addEventListener("click", (event) => {
    const caret = event.target.closest("button.caret");
    if (caret) {
      const item = caret.closest("li");
      if (caret.getAttribute("aria-expanded") === "true") collapseNode(item);
      else expandNode(item).catch((error) => showHint(String(error)));
      return;
    }
    const node = event.target.closest("button.node");
    if (node) selectPlace(node.dataset.id, { zoom: true }).catch(console.error);
  });

  // --- the Feeds tab: the whole table, sorted and filtered here; a feed's
  // hull and, within the viewport, the places it serves in tier colours
  let feedRows = [];
  let feedSequence = 0; // bumped on every feeds request: only the newest lands
  let feedSort = { sort: "name", order: "asc" };
  let feedQuery = "";
  let selectedFeed = null;
  let selectedFeedToken = 0; // the selection sequence the feed was accepted under
  let feedFitting = false; // the hull fit is moving the map: its own moveend refreshes
  let servedSequence = 0;
  // Feed records are cached per build and snapshot as `{ data, snapshot }`;
  // a failed request is dropped again so it can be retried.
  const feedCache = new Map();
  const fetchFeed = (build, id) => {
    const key = `${build}/${snapshot}/${id}`;
    if (!feedCache.has(key)) {
      const url = `/api/builds/${encodeURIComponent(build)}/feeds/${encodeURIComponent(id)}`;
      const pending = fetchSlice(url).catch((error) => {
        if (feedCache.get(key) === pending) feedCache.delete(key);
        throw error;
      });
      feedCache.set(key, pending);
    }
    return feedCache.get(key);
  };
  const resetEdges = (title) => {
    edgesTitle.textContent = title;
    edgesHead.innerHTML = edgesBody.innerHTML = "";
  };

  function renderFeeds() {
    const columns = feedColumns(classField);
    feedsHead.innerHTML = headerHtml(feedSort, columns);
    const rows = sortRows(feedFilter(feedRows, feedQuery), feedSort.sort, feedSort.order);
    feedsBody.innerHTML = rows.map((row) => feedRowHtml(row, columns)).join("");
    for (const row of feedsBody.querySelectorAll("tr")) {
      row.toggleAttribute("aria-selected", row.dataset.id === selectedFeed);
    }
  }

  async function loadFeeds() {
    const build = current;
    const gen = generation;
    const mine = ++feedSequence;
    let reply;
    try {
      const params = new URLSearchParams(viewParams(view));
      reply = await fetchSlice(`/api/builds/${encodeURIComponent(build)}/feeds?${params}`);
    } catch (error) {
      if (gen === generation && mine === feedSequence) showHint(String(error));
      return;
    }
    if (gen !== generation || mine !== feedSequence) return;
    if (reply.snapshot !== snapshot) {
      startLoad(build); // ``latest`` was republished: one snapshot for all
      return;
    }
    feedRows = reply.data.rows;
    renderFeeds();
  }

  // The selected feed's served places within the padded viewport; a slice
  // that fails or overflows while current empties the layer, so nothing
  // from another feed or viewport stays drawn.
  async function refreshServed() {
    if (!selectedFeed) return;
    const build = current;
    const gen = generation;
    const feed = selectedFeed;
    const token = selectedFeedToken;
    const mine = ++servedSequence;
    // Current only while this feed, accepted under this selection, is still
    // the selection: a later feed or place click drops the reply.
    const wanted = () =>
      gen === generation && mine === servedSequence && selectedFeed === feed && selectionSequence === token;
    let slice;
    try {
      slice = await fetchSlice(placesUrl(build, feedSliceParams(feed, map.getZoom(), boundsOf(), view)));
    } catch (error) {
      if (wanted()) {
        map.getSource("served").setData(EMPTY);
        showHint(String(error));
      }
      return;
    }
    if (!wanted()) return;
    if (slice.snapshot !== snapshot) {
      startLoad(build);
      return;
    }
    if (slice.data.overflow) {
      map.getSource("served").setData(EMPTY);
      showHint(overflowText(slice.data));
      return;
    }
    map.getSource("served").setData(slice.data);
  }

  // The Edges tab follows the selection: ``mine`` is the selection sequence
  // the request was made for, and only that selection's reply is shown.
  async function loadEdges(scope, title, mine) {
    const build = current;
    const gen = generation;
    const wanted = () => gen === generation && mine === selectionSequence;
    const params = new URLSearchParams({ ...scope, ...viewParams(view) });
    let reply;
    try {
      reply = await fetchSlice(`/api/builds/${encodeURIComponent(build)}/edges?${params}`);
    } catch (error) {
      if (wanted()) resetEdges(`${title}: ${error}`);
      return;
    }
    if (!wanted()) return;
    if (reply.snapshot !== snapshot) {
      startLoad(build);
      return;
    }
    const rows = reply.data;
    const side = "place_id" in scope ? "place" : "feed";
    const shown = rows.truncated ? ` (first ${rows.rows.length} shown)` : "";
    edgesTitle.textContent = `${title}: ${rows.total} edge${rows.total === 1 ? "" : "s"}${shown}`;
    edgesHead.innerHTML = `<th>${side === "place" ? "Feed" : "Place"}</th>${EDGE_HEADERS}`;
    edgesBody.innerHTML = rows.rows.map((row) => edgeRowHtml(row, side, classField)).join("");
  }

  const clearFeedSelection = () => {
    selectedFeed = null;
    feedFitting = false;
    servedSequence++;
    for (const name of ["hull", "served"]) map.getSource(name).setData(EMPTY);
  };

  // A feed selection shares the selection sequence with places: the later of
  // the two wins, and each clears what the other drew.
  async function selectFeed(id) {
    const build = current;
    const gen = generation;
    const mine = ++selectionSequence;
    const wanted = () => gen === generation && mine === selectionSequence;
    dropPopup();
    resetEdges("Loading…");
    // The previous feed and the place selection go now, not when the record
    // arrives: a feed and a place are never selected together.
    clearFeedSelection();
    selectedId = null;
    selectedRecord = null;
    pendingScroll = null;
    for (const name of ["selected", "ancestors"]) map.getSource(name).setData(EMPTY);
    for (const marked of treeRoot.querySelectorAll("[aria-selected]")) marked.removeAttribute("aria-selected");
    details.innerHTML = '<p class="muted">Loading…</p>';
    renderFeeds();
    let reply;
    try {
      reply = await fetchFeed(build, id);
    } catch (error) {
      if (wanted()) showHint(String(error));
      return;
    }
    if (!wanted()) return;
    if (reply.snapshot !== snapshot) {
      startLoad(build);
      return;
    }
    const record = reply.data;
    // End any camera transition while no feed is selected yet, so its moveend
    // requests no served slice; this selection also wins a build fit still
    // pending.
    map.stop();
    fitting = false;
    selectedFeed = id;
    selectedFeedToken = mine;
    renderFeeds();
    details.innerHTML = feedDetailsHtml(record);
    showTab("details");
    const hull = record.geometry ? { type: "FeatureCollection", features: [record] } : EMPTY;
    map.getSource("hull").setData(hull);
    const extent = collectionBounds(hull);
    if (extent) {
      // Only the hull fit's own completion requests the served slice, for
      // the fitted view; the general moveend handler stands back until then.
      feedFitting = true;
      map.once("moveend", () => {
        feedFitting = false;
        if (wanted()) refreshServed().catch(console.error);
      });
      map.fitBounds([extent.west, extent.south, extent.east, extent.north], { padding: 24 });
    } else {
      refreshServed().catch(console.error);
    }
    loadEdges({ feed_id: id }, record.properties.name ?? id, mine).catch(console.error);
  }

  feedsSearch.addEventListener("input", () => {
    feedQuery = feedsSearch.value;
    renderFeeds();
  });
  feedsHead.addEventListener("click", (event) => {
    const header = event.target.closest("th[data-sort]");
    if (!header) return;
    const column = header.dataset.sort;
    feedSort =
      feedSort.sort === column
        ? { sort: column, order: feedSort.order === "asc" ? "desc" : "asc" }
        : { sort: column, order: "asc" };
    renderFeeds();
  });
  feedsBody.addEventListener("click", (event) => {
    const row = event.target.closest("tr[data-id]");
    if (row) selectFeed(row.dataset.id).catch(console.error);
  });
  legend.innerHTML = legendHtml();

  // A new level or spec: every view of the build follows — the map slice and
  // the served outline, the tables, and the edges of what is selected.
  const changeView = () => {
    view = { level: levelSelect.value, spec: specSelect.value };
    if (!current) return;
    // While a build fit is in flight its own moveend requests the viewport
    // slice, and reads the view then.
    if (!fitting) refresh(current, generation, false).catch(console.error);
    table = { ...table, offset: 0 }; // another result set: back to its first page
    loadTable().catch(console.error);
    loadFeeds().catch(console.error);
    refreshServed().catch(console.error);
    if (selectedFeed) {
      loadEdges({ feed_id: selectedFeed }, selectedFeed, selectionSequence).catch(console.error);
    } else if (selectedRecord && selectedRecord.mine === selectionSequence) {
      const { record } = selectedRecord;
      loadEdges({ place_id: record.properties.place_id }, record.properties.name, selectionSequence).catch(console.error);
    }
  };
  levelSelect.addEventListener("change", changeView);
  specSelect.addEventListener("change", changeView);

  map.on("load", () => {
    map.addSource("places", { type: "geojson", data: EMPTY });
    map.addLayer({ id: "places-fill", type: "fill", source: "places", paint: FILL_PAINT });
    map.addLayer({ id: "places-line", type: "line", source: "places", paint: LINE_PAINT });
    map.addSource("ancestors", { type: "geojson", data: EMPTY });
    map.addLayer({
      id: "ancestors-line",
      type: "line",
      source: "ancestors",
      paint: { "line-color": "#111", "line-width": 1.5, "line-dasharray": [2, 2] },
    });
    map.addSource("hull", { type: "geojson", data: EMPTY });
    map.addLayer({ id: "hull-fill", type: "fill", source: "hull", paint: { "fill-color": "#0d9488", "fill-opacity": 0.12 } });
    map.addLayer({ id: "hull-line", type: "line", source: "hull", paint: { "line-color": "#0d9488", "line-width": 2 } });
    map.addSource("served", { type: "geojson", data: EMPTY });
    map.addLayer({ id: "served-line", type: "line", source: "served", paint: { "line-color": classColorExpression("tier"), "line-width": 2 } });
    map.addSource("selected", { type: "geojson", data: EMPTY });
    map.addLayer({ id: "selected-line", type: "line", source: "selected", paint: { "line-color": "#111", "line-width": 2.5 } });
    map.on("mousemove", "places-fill", (event) => {
      map.getCanvas().title = event.features[0].properties.name;
    });
    map.on("mouseleave", "places-fill", () => {
      map.getCanvas().title = "";
    });
    map.on("click", "places-fill", (event) => {
      // The slice's summary at once; the full details replace it when the
      // record arrives (the same request the panel uses).
      dropPopup();
      const opened = new maplibregl.Popup()
        .setLngLat(event.lngLat)
        .setHTML(popupHtml(event.features[0].properties))
        .addTo(map);
      selectPlace(event.features[0].id, { zoom: false, fromPopup: opened }).catch(console.error);
    });
    map.on("moveend", () => {
      if (current && !fitting) {
        refresh(current, generation, false).catch(console.error);
        if (!feedFitting) refreshServed().catch(console.error);
      }
    });
    select.addEventListener("change", () => startLoad(select.value));
    const first = builds.find((row) => row.complete);
    if (first) {
      select.value = first.id;
      startLoad(first.id);
    }
  });
}

if (typeof document !== "undefined") {
  main().catch((error) => {
    document.getElementById("hint").textContent = String(error);
    document.getElementById("hint").hidden = false;
  });
}
