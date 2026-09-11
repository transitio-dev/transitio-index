// The index viewer's page logic. The pure helpers are exported for Node's
// unit tests; the DOM and map wiring at the bottom runs only in a browser.

const VIEWPORT_ZOOM = 7; // from here the slice follows the viewport
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

  const boundsOf = () => {
    const b = map.getBounds();
    return { west: b.getWest(), south: b.getSouth(), east: b.getEast(), north: b.getNorth() };
  };
  const clear = () => map.getSource("places").setData(EMPTY);
  const showHint = (text) => {
    hint.textContent = text;
    hint.hidden = false;
  };
  // A failure of a request that is still current clears the screen and shows
  // the error where the overflow hint goes; a superseded one says nothing.
  const fail = (error) => {
    clear();
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
      clear();
      showHint(overflowText(data));
      fitting = false;
      return;
    }
    hint.hidden = true;
    map.getSource("places").setData(data);
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
    await refresh(id, gen, true);
  }

  function startLoad(id) {
    loadBuild(id, ++generation).catch(console.error);
  }

  map.on("load", () => {
    map.addSource("places", { type: "geojson", data: EMPTY });
    map.addLayer({ id: "places-fill", type: "fill", source: "places", paint: FILL_PAINT });
    map.addLayer({ id: "places-line", type: "line", source: "places", paint: LINE_PAINT });
    map.on("mousemove", "places-fill", (event) => {
      map.getCanvas().title = event.features[0].properties.name;
    });
    map.on("mouseleave", "places-fill", () => {
      map.getCanvas().title = "";
    });
    map.on("click", "places-fill", (event) => {
      new maplibregl.Popup()
        .setLngLat(event.lngLat)
        .setHTML(popupHtml(event.features[0].properties))
        .addTo(map);
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
