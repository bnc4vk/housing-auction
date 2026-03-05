const DATA_CANDIDATES = [
  "../output/all_properties_enriched.csv",
  "../output/all_properties_scored_geocoded.csv",
  "../output/all_properties_scored.csv",
];
const BOUNDARY_DATA_CANDIDATES = ["../output/parcel_boundaries.geojson"];

const PARCEL_SERVICES = {
  "San Diego": {
    queryUrl:
      "https://gis-public.sandiegocounty.gov/arcgis/rest/services/cosd_warehouse/parcels_all_for_public_use/MapServer/0/query",
    primaryField: "APN",
    altField: "APN_8",
    outFields: "APN,APN_8",
  },
  Riverside: {
    queryUrl:
      "https://gis.countyofriverside.us/arcgis_mapping/rest/services/OpenData/Assessor/MapServer/40/query",
    primaryField: "APN",
    outFields: "APN",
  },
};

const MAX_VISIBLE_BOUNDARIES = 120;
const BOUNDARY_BATCH_SIZE = 40;

const map = L.map("map", { zoomControl: true }).setView([33.2, -116.8], 8);

L.tileLayer(
  "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
  {
    attribution:
      "Tiles &copy; Esri, Maxar, Earthstar Geographics, and the GIS User Community",
    maxZoom: 20,
  }
).addTo(map);

L.tileLayer(
  "https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}",
  {
    attribution: "Labels &copy; Esri",
    maxZoom: 20,
    opacity: 0.9,
  }
).addTo(map);

const state = {
  allRows: [],
  filteredRows: [],
  rowByKey: new Map(),
  markers: new Map(),
  markerLayer: L.layerGroup().addTo(map),
  visibleBoundaryLayer: L.layerGroup().addTo(map),
  activeBoundaryLayer: L.layerGroup().addTo(map),
  boundaryCache: new Map(),
  boundaryPromises: new Map(),
  boundaryBatchToken: 0,
  activeBoundaryToken: 0,
  activeKey: null,
  dataSource: "",
  boundarySource: "",
  visibleBoundaryCount: 0,
};

const els = {
  searchInput: document.getElementById("searchInput"),
  countySelect: document.getElementById("countySelect"),
  minScoreInput: document.getElementById("minScoreInput"),
  recommendedOnly: document.getElementById("recommendedOnly"),
  showBoundaries: document.getElementById("showBoundaries"),
  limitInput: document.getElementById("limitInput"),
  resetBtn: document.getElementById("resetBtn"),
  stats: document.getElementById("stats"),
  propertyList: document.getElementById("propertyList"),
};

function toNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toBool(v) {
  const s = String(v ?? "").toLowerCase().trim();
  return s === "true" || s === "1" || s === "yes";
}

function toMaybeBool(v) {
  const s = String(v ?? "").toLowerCase().trim();
  if (!s || s === "nan") return null;
  if (s === "true" || s === "1" || s === "yes") return true;
  if (s === "false" || s === "0" || s === "no") return false;
  return null;
}

function firstNumber(...values) {
  for (const value of values) {
    if (Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function cleanText(v) {
  const s = String(v ?? "").trim();
  if (!s || s.toLowerCase() === "nan") return "";
  return s;
}

function compareRowsByFinalScore(a, b) {
  return (
    (b.unified_score ?? -Infinity) - (a.unified_score ?? -Infinity) ||
    (b.unified_roi_pct ?? -Infinity) - (a.unified_roi_pct ?? -Infinity) ||
    (b.unified_upside ?? -Infinity) - (a.unified_upside ?? -Infinity) ||
    (a.opening_bid ?? Infinity) - (b.opening_bid ?? Infinity)
  );
}

function keyFor(row) {
  return `${row.county}::${row.parcel_id}::${row.item_id}`;
}

function normalizeParcelQuery(row) {
  const service = PARCEL_SERVICES[row.county];
  if (!service) return null;

  const digits = String(row.parcel_id || "").replace(/\D/g, "");
  if (!digits) return null;

  if (row.county === "San Diego") {
    if (digits.length === 8 && service.altField) {
      return {
        county: row.county,
        field: service.altField,
        value: digits,
        cacheKey: `${row.county}::${service.altField}::${digits}`,
      };
    }
    if (digits.length >= 10) {
      const apn = digits.slice(0, 10);
      return {
        county: row.county,
        field: service.primaryField,
        value: apn,
        cacheKey: `${row.county}::${service.primaryField}::${apn}`,
      };
    }
  }

  if (row.county === "Riverside" && digits.length >= 9) {
    const apn = digits.slice(0, 9);
    return {
      county: row.county,
      field: service.primaryField,
      value: apn,
      cacheKey: `${row.county}::${service.primaryField}::${apn}`,
    };
  }

  return {
    county: row.county,
    field: service.primaryField,
    value: digits,
    cacheKey: `${row.county}::${service.primaryField}::${digits}`,
  };
}

function formatMoney(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(n);
}

function formatPct(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return "-";
  return `${n.toFixed(1)}%`;
}

function formatFloat(n, decimals = 1) {
  if (n === null || n === undefined || Number.isNaN(n)) return "-";
  return Number(n).toFixed(decimals);
}

function scoreColor(score) {
  if (score >= 85) return "#2fbf71";
  if (score >= 70) return "#ffc857";
  if (score >= 55) return "#ff9f1c";
  return "#ff6b6b";
}

function ringsToLatLngs(rings) {
  if (!Array.isArray(rings)) return [];
  return rings
    .filter((ring) => Array.isArray(ring) && ring.length >= 4)
    .map((ring) => ring.map((xy) => [xy[1], xy[0]]));
}

async function fetchCsv(path) {
  const resp = await fetch(path, { cache: "no-store" });
  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status}`);
  }
  return await resp.text();
}

async function fetchJson(path) {
  const resp = await fetch(path, { cache: "no-store" });
  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status}`);
  }
  return await resp.json();
}

async function loadRows() {
  let csvText = "";
  for (const path of DATA_CANDIDATES) {
    try {
      csvText = await fetchCsv(path);
      state.dataSource = path;
      break;
    } catch (err) {
      // Try next candidate.
    }
  }
  if (!csvText) {
    throw new Error("Could not load scored CSV output.");
  }

  const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
  if (parsed.errors?.length) {
    console.warn("CSV parse warnings:", parsed.errors);
  }

  const rows = parsed.data.map((raw) => {
    const hiddenGemScore = toNumber(raw.hidden_gem_score);
    const proScore = toNumber(raw.pro_score);
    const estimatedRoi = toNumber(raw.estimated_roi_pct);
    const proRoi = toNumber(raw.roi_pro_pct);
    const grossUpside = toNumber(raw.gross_upside);
    const netUpsidePro = toNumber(raw.net_upside_pro);
    const estimatedCost = toNumber(raw.estimated_total_cost);
    const expectedCostPro = toNumber(raw.expected_total_cost_pro);
    const recommendedProperty = toBool(raw.recommended_property);
    const recommendedBidPro = toMaybeBool(raw.recommended_bid_pro);

    const unifiedScore = firstNumber(proScore, hiddenGemScore, 0);
    const unifiedRoi = firstNumber(proRoi, estimatedRoi);
    const unifiedUpside = firstNumber(netUpsidePro, grossUpside);
    const unifiedTotalCost = firstNumber(expectedCostPro, estimatedCost);

    return {
      ...raw,
      rank: toNumber(raw.rank),
      opening_bid: toNumber(raw.opening_bid),
      recommended_max_bid: toNumber(raw.recommended_max_bid),
      estimated_market_value: toNumber(raw.estimated_market_value),
      estimated_total_cost: estimatedCost,
      expected_total_cost_pro: expectedCostPro,
      gross_upside: grossUpside,
      net_upside_pro: netUpsidePro,
      estimated_roi_pct: estimatedRoi,
      roi_pro_pct: proRoi,
      roi_bear_pct: toNumber(raw.roi_bear_pct),
      roi_bull_pct: toNumber(raw.roi_bull_pct),
      confidence_score: toNumber(raw.confidence_score),
      hidden_gem_score: hiddenGemScore,
      pro_score: proScore,
      latitude: toNumber(raw.latitude),
      longitude: toNumber(raw.longitude),
      parcel_acres: toNumber(raw.parcel_acres),
      carry_cost_est: toNumber(raw.carry_cost_est),
      possession_months_est: toNumber(raw.possession_months_est),
      title_lien_risk_score: toNumber(raw.title_lien_risk_score),
      recommended_property: recommendedProperty,
      recommended_bid_pro: recommendedBidPro,
      recommended_final: recommendedBidPro ?? recommendedProperty,
      buildability_gate: cleanText(raw.buildability_gate),
      buildability_reasons: cleanText(raw.buildability_reasons),
      title_lien_tier: cleanText(raw.title_lien_tier),
      occupancy_risk: cleanText(raw.occupancy_risk),
      flood_risk: cleanText(raw.flood_risk),
      fire_risk: cleanText(raw.fire_risk),
      zoning_landuse: cleanText(raw.zoning_landuse),
      overlay_notes: cleanText(raw.overlay_notes),
      requires_attorney_review: toBool(raw.requires_attorney_review),
      risk_flags: cleanText(raw.risk_flags),
      unified_score: unifiedScore,
      unified_roi_pct: unifiedRoi,
      unified_upside: unifiedUpside,
      unified_total_cost: unifiedTotalCost,
      model_tier: Number.isFinite(proScore) ? "Enriched" : "Base",
      display_rank: null,
    };
  });

  const ranked = [...rows].sort(compareRowsByFinalScore);
  ranked.forEach((row, idx) => {
    row.display_rank = idx + 1;
  });

  return rows;
}

function geometryToRings(geometry) {
  if (!geometry || !geometry.type || !geometry.coordinates) return [];
  if (geometry.type === "Polygon") {
    return geometry.coordinates;
  }
  if (geometry.type === "MultiPolygon") {
    return geometry.coordinates.flat();
  }
  return [];
}

async function preloadBoundaryCache() {
  for (const path of BOUNDARY_DATA_CANDIDATES) {
    try {
      const fc = await fetchJson(path);
      const features = Array.isArray(fc?.features) ? fc.features : [];
      let count = 0;
      for (const feature of features) {
        const key = feature?.properties?.boundary_key;
        if (!key) continue;
        const rings = geometryToRings(feature.geometry);
        if (!rings.length) continue;
        state.boundaryCache.set(key, {
          rings,
          source: path,
        });
        count += 1;
      }
      if (count > 0) {
        state.boundarySource = path;
        return count;
      }
    } catch (err) {
      // Try next candidate path.
    }
  }
  return 0;
}

function applyFilters() {
  const search = els.searchInput.value.toLowerCase().trim();
  const county = els.countySelect.value;
  const minScore = Number(els.minScoreInput.value || 0);
  const recommendedOnly = els.recommendedOnly.checked;
  const limit = Math.max(10, Math.min(1500, Number(els.limitInput.value || 300)));

  let rows = state.allRows.filter((row) => {
    if (county !== "all" && row.county !== county) return false;
    if ((row.unified_score ?? 0) < minScore) return false;
    if (recommendedOnly && !row.recommended_final) return false;
    if (search) {
      const hay = `${row.parcel_id} ${row.city} ${row.address}`.toLowerCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  rows = rows.sort(compareRowsByFinalScore).slice(0, limit);
  state.filteredRows = rows;
}

function buildPopup(row) {
  const title = `<h3 class="popup-title">${row.parcel_id} (${row.county})</h3>`;
  const riskSummary = [row.risk_flags, row.overlay_notes].filter(Boolean).join("; ");
  const lines = `
    <div class="popup-lines">
      <div><strong>${row.property_type}</strong> | ${row.city}</div>
      <div>${row.address || ""}</div>
      <div>Open: ${formatMoney(row.opening_bid)} | Max Bid: ${formatMoney(row.recommended_max_bid)}</div>
      <div>Total Cost: ${formatMoney(row.unified_total_cost)} | Net Upside: ${formatMoney(row.unified_upside)}</div>
      <div>ROI: ${formatPct(row.unified_roi_pct)} | Score: ${formatFloat(row.unified_score, 1)}</div>
      <div>ROI Range (Bear/Bull): ${formatPct(row.roi_bear_pct)} / ${formatPct(row.roi_bull_pct)}</div>
      <div>Confidence: ${formatFloat(row.confidence_score, 0)} | Model: ${row.model_tier}</div>
      <div>Acres: ${formatFloat(row.parcel_acres, 2)} | Possession: ${formatFloat(row.possession_months_est, 1)} months</div>
      <div>Flood: ${row.flood_risk || "-"} | Fire: ${row.fire_risk || "-"} | Occupancy: ${row.occupancy_risk || "-"}</div>
      <div>Gate: ${row.buildability_gate || "-"} | Title: ${row.title_lien_tier || "-"}</div>
      <div>Carry Cost Est: ${formatMoney(row.carry_cost_est)} | Zoning: ${row.zoning_landuse || "-"}</div>
      <div>Attorney Review: ${row.requires_attorney_review ? "Yes" : "No"}</div>
      <div>Flags: ${riskSummary || "None"}</div>
    </div>
  `;
  return `${title}${lines}`;
}

function renderMarkers() {
  state.markerLayer.clearLayers();
  state.markers.clear();

  let mappedCount = 0;
  for (const row of state.filteredRows) {
    if (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)) {
      continue;
    }
    const key = keyFor(row);
    const marker = L.circleMarker([row.latitude, row.longitude], {
      radius: row.recommended_final ? 7 : 5,
      color: "#0b0d12",
      weight: 1.2,
      fillColor: scoreColor(row.unified_score || 0),
      fillOpacity: 0.9,
    }).bindPopup(buildPopup(row));

    marker.on("click", () => {
      selectRow(row, { openPopup: false, panToMarker: false });
    });

    marker.addTo(state.markerLayer);
    state.markers.set(key, marker);
    mappedCount += 1;
  }

  if (mappedCount > 0) {
    const group = L.featureGroup(Array.from(state.markers.values()));
    map.fitBounds(group.getBounds().pad(0.15), { maxZoom: 14 });
  }
}

function makeCard(row) {
  const key = keyFor(row);
  const active = key === state.activeKey ? "active" : "";
  const recommendedClass = row.recommended_final ? "ok" : "warn";
  const recommendedText = row.recommended_final ? "Bid Candidate" : "Needs Review";
  const mappedText =
    Number.isFinite(row.latitude) && Number.isFinite(row.longitude)
      ? "Mapped"
      : "No coordinates";
  const gateState = String(row.buildability_gate || "").toUpperCase();
  const titleTier = String(row.title_lien_tier || "").toLowerCase();
  const gateClass = gateState === "PASS" ? "ok" : "warn";
  const titleClass = titleTier === "low" ? "ok" : "warn";
  const riskSummary = [row.risk_flags, row.overlay_notes].filter(Boolean).join("; ");

  const card = document.createElement("div");
  card.className = `card ${active}`;
  card.innerHTML = `
    <div class="top">
      <div class="apn">${row.parcel_id}</div>
      <div class="rank">#${row.display_rank ?? "-"}</div>
    </div>
    <div class="meta">${row.county} | ${row.city} | ${row.property_type} | ${row.model_tier}</div>
    <div class="meta">${row.address || ""}</div>
    <div class="metrics">
      <div>Open: <strong>${formatMoney(row.opening_bid)}</strong></div>
      <div>Max Bid: <strong>${formatMoney(row.recommended_max_bid)}</strong></div>
      <div>Total Cost: <strong>${formatMoney(row.unified_total_cost)}</strong></div>
      <div>Upside: <strong>${formatMoney(row.unified_upside)}</strong></div>
      <div>ROI: <strong>${formatPct(row.unified_roi_pct)}</strong></div>
      <div>Score: <strong>${formatFloat(row.unified_score, 1)}</strong></div>
    </div>
    <div class="chips">
      <div class="chip ${recommendedClass}">${recommendedText}</div>
      <div class="chip ${gateClass}">${row.buildability_gate || "Gate Unknown"}</div>
      <div class="chip ${titleClass}">Title ${row.title_lien_tier || "unknown"}</div>
      <div class="chip">${mappedText}</div>
      ${row.requires_attorney_review ? '<div class="chip warn">Attorney Review</div>' : ""}
      ${riskSummary ? '<div class="chip warn">Risk Flags</div>' : ""}
    </div>
  `;

  card.addEventListener("click", () => {
    selectRow(row, { openPopup: true, panToMarker: true });
  });
  return card;
}

function renderList() {
  els.propertyList.innerHTML = "";
  for (const row of state.filteredRows) {
    els.propertyList.appendChild(makeCard(row));
  }
}

function renderStats() {
  const total = state.allRows.length;
  const shown = state.filteredRows.length;
  const mapped = state.filteredRows.filter(
    (r) => Number.isFinite(r.latitude) && Number.isFinite(r.longitude)
  ).length;
  const recommended = state.filteredRows.filter((r) => r.recommended_final).length;
  const enriched = state.filteredRows.filter((r) => r.model_tier === "Enriched").length;

  const statsParts = [
    `Showing ${shown}/${total}`,
    `${mapped} mapped`,
    `${recommended} bid candidates`,
    `${enriched} enriched`,
    `${state.visibleBoundaryCount} boundaries`,
    `source: ${state.dataSource.replace("../", "")}`,
    state.boundarySource ? `parcel file: ${state.boundarySource.replace("../", "")}` : "",
  ];
  els.stats.textContent = statsParts.filter(Boolean).join(" • ");
}

function buildWhereIn(field, values) {
  const escaped = values.map((v) => `'${String(v).replace(/'/g, "''")}'`).join(",");
  return `${field} IN (${escaped})`;
}

async function queryArcGisFeatures(service, whereClause, outFields) {
  const params = new URLSearchParams({
    where: whereClause,
    outFields,
    returnGeometry: "true",
    outSR: "4326",
    f: "json",
  });
  const url = `${service.queryUrl}?${params.toString()}`;
  const resp = await fetch(url);
  if (!resp.ok) {
    throw new Error(`Boundary query failed (${resp.status})`);
  }
  const payload = await resp.json();
  return Array.isArray(payload.features) ? payload.features : [];
}

function cacheBoundary(norm, rings, source) {
  state.boundaryCache.set(norm.cacheKey, {
    rings,
    source,
  });
}

function cacheNoBoundary(norm) {
  state.boundaryCache.set(norm.cacheKey, null);
}

async function fetchBoundaryForRow(row) {
  const norm = normalizeParcelQuery(row);
  if (!norm) return null;
  if (state.boundaryCache.has(norm.cacheKey)) {
    return state.boundaryCache.get(norm.cacheKey);
  }
  if (state.boundaryPromises.has(norm.cacheKey)) {
    return state.boundaryPromises.get(norm.cacheKey);
  }

  const service = PARCEL_SERVICES[row.county];
  if (!service) return null;

  const promise = (async () => {
    try {
      const where = `${norm.field}='${norm.value}'`;
      const features = await queryArcGisFeatures(service, where, service.outFields);
      if (!features.length) {
        cacheNoBoundary(norm);
        return null;
      }
      const rings = features[0]?.geometry?.rings;
      if (!Array.isArray(rings) || !rings.length) {
        cacheNoBoundary(norm);
        return null;
      }
      const record = { rings, source: service.queryUrl };
      cacheBoundary(norm, rings, service.queryUrl);
      return record;
    } catch (err) {
      console.warn("Boundary fetch failed", err);
      cacheNoBoundary(norm);
      return null;
    } finally {
      state.boundaryPromises.delete(norm.cacheKey);
    }
  })();

  state.boundaryPromises.set(norm.cacheKey, promise);
  return promise;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

async function prefetchVisibleBoundaries(rows, token) {
  const byCountyField = new Map();

  for (const row of rows) {
    const norm = normalizeParcelQuery(row);
    if (!norm) continue;
    if (state.boundaryCache.has(norm.cacheKey)) continue;

    const key = `${norm.county}::${norm.field}`;
    if (!byCountyField.has(key)) {
      byCountyField.set(key, { norm, values: new Set() });
    }
    byCountyField.get(key).values.add(norm.value);
  }

  for (const group of byCountyField.values()) {
    if (token !== state.boundaryBatchToken) return;
    const service = PARCEL_SERVICES[group.norm.county];
    const values = Array.from(group.values);
    const chunks = chunkArray(values, BOUNDARY_BATCH_SIZE);

    for (const chunk of chunks) {
      if (token !== state.boundaryBatchToken) return;
      const where = buildWhereIn(group.norm.field, chunk);
      try {
        const features = await queryArcGisFeatures(service, where, service.outFields);
        const found = new Set();

        for (const feature of features) {
          const attrs = feature.attributes || {};
          const value = attrs[group.norm.field] || attrs[service.primaryField] || attrs.APN;
          if (!value) continue;
          const normalized = String(value).replace(/\D/g, "");
          if (!normalized) continue;
          const cacheKey = `${group.norm.county}::${group.norm.field}::${normalized}`;
          const rings = feature.geometry?.rings;
          if (Array.isArray(rings) && rings.length) {
            state.boundaryCache.set(cacheKey, { rings, source: service.queryUrl });
          } else {
            state.boundaryCache.set(cacheKey, null);
          }
          found.add(normalized);
        }

        for (const asked of chunk) {
          const key = `${group.norm.county}::${group.norm.field}::${asked}`;
          if (!state.boundaryCache.has(key)) {
            state.boundaryCache.set(key, null);
          }
        }
      } catch (err) {
        console.warn("Boundary batch fetch failed", err);
      }
    }
  }
}

function renderVisibleBoundaries() {
  state.visibleBoundaryLayer.clearLayers();
  state.visibleBoundaryCount = 0;

  if (!els.showBoundaries.checked) return;

  const rows = state.filteredRows.slice(0, MAX_VISIBLE_BOUNDARIES);
  for (const row of rows) {
    const norm = normalizeParcelQuery(row);
    if (!norm) continue;
    const cached = state.boundaryCache.get(norm.cacheKey);
    if (!cached || !cached.rings) continue;

    const latlngs = ringsToLatLngs(cached.rings);
    if (!latlngs.length) continue;

    const poly = L.polygon(latlngs, {
      color: "#89c2ff",
      weight: 1,
      fillColor: "#7fb8ff",
      fillOpacity: 0.08,
      interactive: false,
    });
    poly.addTo(state.visibleBoundaryLayer);
    state.visibleBoundaryCount += 1;
  }
}

async function refreshVisibleBoundaries() {
  state.visibleBoundaryLayer.clearLayers();
  state.visibleBoundaryCount = 0;
  if (!els.showBoundaries.checked) {
    renderStats();
    return;
  }

  const token = ++state.boundaryBatchToken;
  const rows = state.filteredRows.slice(0, MAX_VISIBLE_BOUNDARIES);
  await prefetchVisibleBoundaries(rows, token);
  if (token !== state.boundaryBatchToken) return;
  renderVisibleBoundaries();
  renderStats();
}

async function showActiveBoundary(row) {
  state.activeBoundaryLayer.clearLayers();
  if (!row) return;
  const token = ++state.activeBoundaryToken;
  const boundary = await fetchBoundaryForRow(row);
  if (token !== state.activeBoundaryToken) return;
  if (!boundary || !boundary.rings) return;

  const latlngs = ringsToLatLngs(boundary.rings);
  if (!latlngs.length) return;

  const poly = L.polygon(latlngs, {
    color: "#ffe066",
    weight: 3,
    fillColor: "#ffd43b",
    fillOpacity: 0.08,
  });
  poly.addTo(state.activeBoundaryLayer);
}

async function selectRow(row, options = {}) {
  const { openPopup = false, panToMarker = false } = options;
  state.activeKey = keyFor(row);
  renderList();

  const marker = state.markers.get(state.activeKey);
  if (marker && panToMarker) {
    map.setView(marker.getLatLng(), 16);
  }
  if (marker && openPopup) {
    marker.openPopup();
  }

  await showActiveBoundary(row);
}

function refresh() {
  applyFilters();
  renderMarkers();
  renderList();
  renderStats();
  refreshVisibleBoundaries();

  if (state.activeKey) {
    const activeRow = state.rowByKey.get(state.activeKey);
    if (activeRow) {
      showActiveBoundary(activeRow);
    } else {
      state.activeBoundaryLayer.clearLayers();
    }
  } else {
    state.activeBoundaryLayer.clearLayers();
  }
}

function attachEvents() {
  for (const el of [
    els.searchInput,
    els.countySelect,
    els.minScoreInput,
    els.recommendedOnly,
    els.showBoundaries,
    els.limitInput,
  ]) {
    el.addEventListener("input", refresh);
    el.addEventListener("change", refresh);
  }

  els.resetBtn.addEventListener("click", () => {
    els.searchInput.value = "";
    els.countySelect.value = "all";
    els.minScoreInput.value = "0";
    els.recommendedOnly.checked = false;
    els.showBoundaries.checked = false;
    els.limitInput.value = "300";
    state.activeKey = null;
    refresh();
  });
}

async function main() {
  try {
    const rows = await loadRows();
    state.allRows = rows;
    state.rowByKey = new Map(rows.map((row) => [keyFor(row), row]));
    await preloadBoundaryCache();
    attachEvents();
    refresh();
  } catch (err) {
    console.error(err);
    els.stats.textContent =
      "Failed to load output CSV files. Run scoring + geocoding, then serve this repo with a local HTTP server.";
  }
}

main();
