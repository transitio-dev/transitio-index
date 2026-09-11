// The index viewer's page logic. The pure helpers are exported for Node's
// unit tests; the DOM and map wiring at the bottom runs only in a browser.

const VIEWPORT_ZOOM = 7; // from here the slice follows the viewport
export const CITY_ZOOM = 9; // from here the slice includes cities
const KIND_COLORS = { country: "#6b7280", region: "#2563eb", city: "#f59e0b" };

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

export function sliceParams(zoom, bounds) {
  const params = { zoom };
  if (zoom >= VIEWPORT_ZOOM) params.bbox = bboxParam(paddedBounds(bounds));
  if (zoom >= CITY_ZOOM) params.kind = "all";
  return params;
}

// The whole-build overview a build change loads before fitting the map to
// it: no bbox, and never finer than the coarsest zoom band, so a large build
// stays under the byte cap whatever the current zoom.
export function overviewParams(zoom) {
  return { zoom: Math.min(zoom, VIEWPORT_ZOOM - 1) };
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
  return `${counts.places ?? "?"} places · ${counts.feeds ?? "?"} feeds · ${
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

export function tableUrl(build, state) {
  const params = new URLSearchParams({
    sort: state.sort,
    order: state.order,
    offset: String(state.offset),
    limit: String(TABLE_PAGE),
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

export function rowHtml(row) {
  const cells = TABLE_COLUMNS.map(([column]) => {
    const value = row[column];
    if (column === "served") return value ? "yes" : "no";
    return typeof value === "number" ? formatStat(value) : escapeHtml(value ?? DASH);
  });
  return `<tr data-id="${escapeHtml(row.place_id)}">${cells.map((c) => `<td>${c}</td>`).join("")}</tr>`;
}

export function headerHtml(state) {
  return TABLE_COLUMNS.map(([column, label]) => {
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

export function detailsHtml(record) {
  const p = record.properties;
  const chain = crumbs(record)
    .map((c, i, all) =>
      i === all.length - 1
        ? `<strong>${escapeHtml(c.name)}</strong>`
        : `<button type="button" class="crumb" data-id="${escapeHtml(c.place_id)}">${escapeHtml(c.name)}</button>`,
    )
    .join(" › ");
  const served = p.served ? `served by ${p.feed_count} feed${p.feed_count === 1 ? "" : "s"}` : "not served";
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
        `<tr><td>${escapeHtml(e.feed_name ?? e.feed_id)}</td><td>${escapeHtml(e.tier ?? DASH)}</td>` +
        `<td>${e.tier_confidence == null ? DASH : e.tier_confidence.toFixed(2)}</td>` +
        `<td>${escapeHtml(e.method ?? DASH)}</td></tr>`,
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
    `<p><small>${escapeHtml(p.kind)} · ${escapeHtml(p.place_id)}</small> · ${escapeHtml(served)}</p>` +
    (stats ? `<ul class="stats">${stats}</ul>` : "") +
    children +
    (feeds
      ? `<table class="feeds"><thead><tr><th>Feed</th><th>Tier</th><th>Conf.</th><th>Method</th></tr></thead><tbody>${feeds}</tbody></table>`
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
  "fill-color": ["match", ["get", "kind"], ...Object.entries(KIND_COLORS).flat(), "#999"],
  "fill-opacity": ["case", ["get", "served"], 0.45, 0.12],
};
const LINE_PAINT = {
  "line-color": ["match", ["get", "kind"], ...Object.entries(KIND_COLORS).flat(), "#999"],
  "line-width": 1,
};

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
  const treeSection = document.getElementById("tree");
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
    for (const name of ["places", "selected", "ancestors"]) map.getSource(name).setData(EMPTY);
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
    const params = fit ? overviewParams(map.getZoom()) : sliceParams(map.getZoom(), boundsOf());
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
    loadTable().catch(console.error);
    loadTree().catch(console.error);
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
    selectedRecord = { record, mine }; // with the sequence it was accepted under
    details.innerHTML = detailsHtml(record);
    revealInTree(record, mine).catch(console.error);
    if (fromPopup && popup === fromPopup && popup.isOpen()) popup.setHTML(detailsHtml(record));
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
    tableHead.innerHTML = headerHtml(table);
    let page;
    try {
      page = await fetchSlice(tableUrl(build, table));
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
    tableBody.innerHTML = page.rows.map(rowHtml).join("");
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
      if (current && !fitting) refresh(current, generation, false).catch(console.error);
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
