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
const MOBILE_BREAKPOINT = 980;
const SOCAL_BOUNDS = {
  minLat: 32.0,
  maxLat: 35.5,
  minLon: -119.2,
  maxLon: -114.0,
};

const FLAG_LABELS = {
  no_geocode: "No geocoded address found",
  missing_situs: "Situs address missing",
  missing_situs_address: "Situs address missing",
  zoning_miss: "Zoning information missing",
  timeshare_liquidity_risk: "Timeshare resale liquidity risk",
  low_historical_sellthrough_bin: "Low historical sell-through in this pricing tier",
  value_estimated_no_assessor: "Market value estimated without assessor match",
  long_default_age: "Long time in tax default before sale",
  very_low_opening_bid: "Very low opening bid relative to market",
  fire_severity: "Elevated fire-severity area",
  small_parcel: "Small parcel may limit usability",
  missing_parcel_attributes: "Parcel attributes missing from county data",
};

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
  boundaryLayer: L.layerGroup().addTo(map),
  boundaryCache: new Map(),
  boundaryPromises: new Map(),
  boundaryBatchToken: 0,
  selectionToken: 0,
  activeKey: null,
  dataSource: "",
  boundarySource: "",
  visibleBoundaryCount: 0,
  filtersOpen: false,
  lastMobile: null,
};

const els = {
  panel: document.querySelector(".panel"),
  controls: document.getElementById("controls"),
  toggleFiltersBtn: document.getElementById("toggleFiltersBtn"),
  searchInput: document.getElementById("searchInput"),
  countySelect: document.getElementById("countySelect"),
  buildableOnly: document.getElementById("buildableOnly"),
  showUnmapped: document.getElementById("showUnmapped"),
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
  const key = s.toLowerCase();
  if (!s || key === "nan" || key === "none" || key === "n/a" || key === "na" || key === "-") {
    return "";
  }
  return s;
}

function splitTokens(value) {
  const clean = cleanText(value);
  if (!clean) return [];
  return clean
    .split(/[;,|]/)
    .map((part) => part.trim())
    .filter(Boolean);
}

function uniqueCaseInsensitive(values) {
  const out = [];
  const seen = new Set();
  for (const value of values) {
    const key = String(value || "").toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }
  return out;
}

function normalizeFlagKey(value) {
  return String(value || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function toTitleCaseLoose(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/\b([a-z])/g, (m) => m.toUpperCase());
}

function normalizeDisplayText(value, fallback = "-") {
  const cleaned = cleanText(value).replace(/[_]+/g, " ").replace(/\s+/g, " ");
  if (!cleaned) return fallback;
  const letters = cleaned.replace(/[^A-Za-z]/g, "");
  const upperCount = letters.replace(/[^A-Z]/g, "").length;
  const mostlyUpper = letters.length > 0 && upperCount / letters.length > 0.75;
  let out = mostlyUpper ? toTitleCaseLoose(cleaned) : cleaned;
  out = out
    .replace(/\bCa\b/g, "CA")
    .replace(/\bUsa\b/g, "USA")
    .replace(/\b([Nsew]{1,2})\b/g, (m) => m.toUpperCase());
  return out;
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function humanizeFlag(value) {
  const key = normalizeFlagKey(value);
  if (!key) return "";
  if (FLAG_LABELS[key]) return FLAG_LABELS[key];
  return normalizeDisplayText(key.replace(/_/g, " "));
}

function hoverHint(label, html, options = {}) {
  const align = ["left", "center", "right"].includes(options.align) ? options.align : "center";
  const placement = options.placement === "below" ? "below" : "above";
  const size = ["narrow", "wide"].includes(options.size) ? options.size : "normal";
  const variant = options.variant === "chip" ? "chip" : "text";
  const labelText = String(options.label || "Details");
  return `<span class="hover-hint hover-hint-${align} hover-hint-${placement} hover-hint-${size} hover-hint-${variant}" tabindex="0" aria-label="${escapeHtml(
    labelText
  )}">${label}<span class="hover-bubble">${html}</span></span>`;
}

function compareRowsByFinalScore(a, b) {
  return (
    (b.final_score ?? -Infinity) - (a.final_score ?? -Infinity) ||
    (b.final_roi_pct ?? -Infinity) - (a.final_roi_pct ?? -Infinity) ||
    (b.final_upside ?? -Infinity) - (a.final_upside ?? -Infinity) ||
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

function isMobile() {
  return window.innerWidth <= MOBILE_BREAKPOINT;
}

function isSoCalCoordinate(lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return false;
  return (
    lat >= SOCAL_BOUNDS.minLat &&
    lat <= SOCAL_BOUNDS.maxLat &&
    lon >= SOCAL_BOUNDS.minLon &&
    lon <= SOCAL_BOUNDS.maxLon
  );
}

function normalizeMapText(value) {
  return String(value || "").toLowerCase().replace(/\s+/g, "_");
}

function centroidForRing(ring) {
  if (!Array.isArray(ring) || ring.length < 3) return null;
  let twiceArea = 0;
  let cx = 0;
  let cy = 0;
  for (let i = 0; i < ring.length; i += 1) {
    const [x1, y1] = ring[i];
    const [x2, y2] = ring[(i + 1) % ring.length];
    const cross = x1 * y2 - x2 * y1;
    twiceArea += cross;
    cx += (x1 + x2) * cross;
    cy += (y1 + y2) * cross;
  }
  if (Math.abs(twiceArea) < 1e-9) {
    let sx = 0;
    let sy = 0;
    for (const [x, y] of ring) {
      sx += x;
      sy += y;
    }
    return {
      lon: sx / ring.length,
      lat: sy / ring.length,
      areaAbs: 0,
    };
  }
  return {
    lon: cx / (3 * twiceArea),
    lat: cy / (3 * twiceArea),
    areaAbs: Math.abs(twiceArea),
  };
}

function centroidFromRings(rings) {
  if (!Array.isArray(rings) || !rings.length) return null;
  let best = null;
  for (const ring of rings) {
    const centroid = centroidForRing(ring);
    if (!centroid) continue;
    if (!best || centroid.areaAbs > best.areaAbs) {
      best = centroid;
    }
  }
  if (!best) return null;
  if (!isSoCalCoordinate(best.lat, best.lon)) return null;
  return { lat: best.lat, lon: best.lon };
}

function costTooltipText(row) {
  const rows = [
    ["Predicted winning bid", row.estimated_winning_bid],
    ["Buyer premium", row.buyer_premium],
    ["Transfer tax", row.estimated_transfer_tax],
    ["Recording fee", row.estimated_recording_fee],
  ].filter(([, value]) => Number.isFinite(value));

  if (Number.isFinite(row.title_clearance_budget) && row.title_clearance_budget > 0) {
    rows.push(["Title clearance", row.title_clearance_budget]);
  }
  if (Number.isFinite(row.carry_cost_est) && row.carry_cost_est > 0) {
    rows.push(["Carry cost allowance", row.carry_cost_est]);
  }

  const lines = rows
    .map(
      ([label, value]) =>
        `<div class="tip-line"><span>${escapeHtml(label)}</span><strong>${escapeHtml(
          formatMoney(value)
        )}</strong></div>`
    )
    .join("");
  const total = `<div class="tip-line tip-total"><span>Total predicted cost</span><strong>${escapeHtml(
    formatMoney(row.final_total_cost)
  )}</strong></div>`;
  return `${lines}${total}`;
}

function boundaryMappedTooltip() {
  return "Location uses the parcel boundary centroid when no reliable geocoded address was available.";
}

function buildabilityTooltipText(row) {
  const gate = normalizeMapText(row.buildability_gate);
  if (gate === "pass") {
    return "Passed automated checks for parcel size, hazard overlays, and baseline buildability signals.";
  }
  const reasons = uniqueCaseInsensitive(splitTokens(row.buildability_reasons).map(humanizeFlag));
  if (reasons.length) {
    return `Review is required due to: ${reasons.join("; ")}.`;
  }
  return "Review is required because one or more buildability checks were not fully cleared.";
}

function titleTierTooltipText(row) {
  const tier = normalizeMapText(row.title_lien_tier);
  if (tier === "low") {
    return "Low modeled title and lien complexity. Verify with a full title and lien report before bidding.";
  }
  if (tier === "medium") {
    return "Moderate modeled title and lien complexity. A pre-bid title review is recommended.";
  }
  return "Title and lien complexity data is limited for this parcel.";
}

function riskTooltipText(row) {
  if (!Array.isArray(row.risk_items_human) || !row.risk_items_human.length) {
    return '<div class="tip-copy">No risk flags in the current model output.</div>';
  }
  return row.risk_items_human
    .map((flag) => `<div class="tip-line"><span>${escapeHtml(flag)}</span></div>`)
    .join("");
}

function popupOffsetForLatLng(latlng) {
  const defaultOffset = L.point(190, 16);
  if (!latlng || !map || !map.getSize) return defaultOffset;
  const size = map.getSize();
  const point = map.latLngToContainerPoint(latlng);
  const preferRight = point.x <= size.x * 0.56;
  return L.point(preferRight ? 190 : -190, 16);
}

function applyPopupPlacement(marker) {
  const popup = marker?.getPopup?.();
  if (!popup) return;
  popup.options.offset = popupOffsetForLatLng(marker.getLatLng());
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
    const baseScore = toNumber(raw.hidden_gem_score);
    const enhancedScore = toNumber(raw.pro_score);
    const baseRoi = toNumber(raw.estimated_roi_pct);
    const enhancedRoi = toNumber(raw.roi_pro_pct);
    const baseUpside = toNumber(raw.gross_upside);
    const enhancedUpside = toNumber(raw.net_upside_pro);
    const baseCost = toNumber(raw.estimated_total_cost);
    const enhancedCost = toNumber(raw.expected_total_cost_pro);
    const baseRecommendation = toBool(raw.recommended_property);
    const enhancedRecommendation = toMaybeBool(raw.recommended_bid_pro);

    const finalScore = firstNumber(enhancedScore, baseScore, 0);
    const finalRoi = firstNumber(enhancedRoi, baseRoi);
    const finalUpside = firstNumber(enhancedUpside, baseUpside);
    const finalCost = firstNumber(enhancedCost, baseCost);
    const rawLat = toNumber(raw.latitude);
    const rawLon = toNumber(raw.longitude);
    const lat = isSoCalCoordinate(rawLat, rawLon) ? rawLat : null;
    const lon = isSoCalCoordinate(rawLat, rawLon) ? rawLon : null;
    const riskFlagsText = cleanText(raw.risk_flags);
    const overlayText = cleanText(raw.overlay_notes);
    const buildabilityReasonsText = cleanText(raw.buildability_reasons);
    const riskItemsHuman = uniqueCaseInsensitive(
      [riskFlagsText, overlayText, buildabilityReasonsText]
        .flatMap((value) => splitTokens(value))
        .map(humanizeFlag)
        .filter(Boolean)
    );

    return {
      ...raw,
      rank: toNumber(raw.rank),
      opening_bid: toNumber(raw.opening_bid),
      max_suggested_bid: toNumber(raw.recommended_max_bid),
      estimated_winning_bid: toNumber(raw.estimated_winning_bid),
      buyer_premium: toNumber(raw.buyer_premium),
      estimated_transfer_tax: toNumber(raw.estimated_transfer_tax),
      estimated_recording_fee: toNumber(raw.estimated_recording_fee),
      title_clearance_budget: toNumber(raw.title_clearance_budget),
      estimated_market_value: toNumber(raw.estimated_market_value),
      final_total_cost: finalCost,
      final_upside: finalUpside,
      final_roi_pct: finalRoi,
      roi_bear_pct: toNumber(raw.roi_bear_pct),
      roi_bull_pct: toNumber(raw.roi_bull_pct),
      confidence_score: toNumber(raw.confidence_score),
      final_score: finalScore,
      latitude: lat,
      longitude: lon,
      map_source: Number.isFinite(lat) && Number.isFinite(lon) ? "geocode" : "none",
      parcel_acres: toNumber(raw.parcel_acres),
      carry_cost_est: toNumber(raw.carry_cost_est),
      possession_months_est: toNumber(raw.possession_months_est),
      title_lien_risk_score: toNumber(raw.title_lien_risk_score),
      is_candidate: enhancedRecommendation ?? baseRecommendation,
      is_buildability_pass: normalizeMapText(raw.buildability_gate) === "pass",
      buildability_gate: cleanText(raw.buildability_gate),
      buildability_reasons: buildabilityReasonsText,
      title_lien_tier: cleanText(raw.title_lien_tier),
      occupancy_risk: cleanText(raw.occupancy_risk),
      flood_risk: cleanText(raw.flood_risk),
      fire_risk: cleanText(raw.fire_risk),
      zoning_landuse: cleanText(raw.zoning_landuse),
      overlay_notes: overlayText,
      requires_attorney_review: toBool(raw.requires_attorney_review),
      risk_flags: riskFlagsText,
      risk_notes: riskItemsHuman.join("; "),
      risk_items_human: riskItemsHuman,
      risk_count: riskItemsHuman.length,
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

function applyBoundaryCoordinateFallback(rows) {
  let fallbackCount = 0;
  for (const row of rows) {
    if (Number.isFinite(row.latitude) && Number.isFinite(row.longitude)) {
      continue;
    }
    const norm = normalizeParcelQuery(row);
    if (!norm) continue;
    const cached = state.boundaryCache.get(norm.cacheKey);
    if (!cached?.rings) continue;
    const centroid = centroidFromRings(cached.rings);
    if (!centroid) continue;
    row.latitude = centroid.lat;
    row.longitude = centroid.lon;
    row.map_source = "boundary-centroid";
    fallbackCount += 1;
  }
  return fallbackCount;
}

function setFiltersOpen(nextOpen) {
  state.filtersOpen = !!nextOpen;
  els.panel.classList.toggle("filters-open", state.filtersOpen);
  if (isMobile()) {
    els.toggleFiltersBtn.textContent = state.filtersOpen ? "Hide" : "Filters";
  } else {
    els.toggleFiltersBtn.textContent = "Filters";
  }
}

function syncLayoutForViewport(force = false) {
  const mobile = isMobile();
  if (!force && mobile === state.lastMobile) return;

  state.lastMobile = mobile;
  if (mobile) {
    setFiltersOpen(false);
  } else {
    setFiltersOpen(true);
  }
  map.invalidateSize();
}

function applyFilters() {
  const search = els.searchInput.value.toLowerCase().trim();
  const county = els.countySelect.value;
  const buildableOnly = els.buildableOnly.checked;
  const showUnmapped = els.showUnmapped.checked;
  const limit = Math.max(10, Math.min(1500, Number(els.limitInput.value || 10)));

  let rows = state.allRows.filter((row) => {
    if (county !== "all" && row.county !== county) return false;
    if (buildableOnly && !row.is_buildability_pass) return false;
    if (!showUnmapped && (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude))) return false;
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
  const gateLabel = normalizeDisplayText(row.buildability_gate, "Review");
  const titleLabel = normalizeDisplayText(row.title_lien_tier, "Unknown");
  const riskSummary = Array.isArray(row.risk_items_human) && row.risk_items_human.length
    ? row.risk_items_human.join("; ")
    : "None";
  const buildabilityTag = hoverHint(
    `Buildability ${escapeHtml(gateLabel)}`,
    `<div class="tip-copy">${escapeHtml(buildabilityTooltipText(row))}</div>`,
    { align: "left", size: "wide", variant: "chip", label: "Buildability details" }
  );
  const titleTag = hoverHint(
    `Title ${escapeHtml(titleLabel)}`,
    `<div class="tip-copy">${escapeHtml(titleTierTooltipText(row))}</div>`,
    { align: "left", size: "wide", variant: "chip", label: "Title risk details" }
  );
  return `
    <div class="popup-card">
      <div class="popup-head">
        <div class="popup-id">${row.parcel_id}</div>
        <div class="popup-rank">#${row.display_rank ?? "-"}</div>
      </div>
      <div class="popup-sub">${normalizeDisplayText(row.property_type)} | ${normalizeDisplayText(
        row.city
      )}</div>
      <div class="popup-address">${normalizeDisplayText(row.address)}</div>
      <div class="popup-grid">
        <div><span>Opening bid</span><strong>${formatMoney(row.opening_bid)}</strong></div>
        <div><span>Max suggested bid</span><strong>${formatMoney(row.max_suggested_bid)}</strong></div>
        <div><span>Upside</span><strong>${formatMoney(row.final_upside)}</strong></div>
        <div><span>Total predicted cost</span><strong>${formatMoney(row.final_total_cost)}</strong></div>
        <div><span>Return</span><strong>${formatPct(row.final_roi_pct)}</strong></div>
        <div><span>Score</span><strong>${formatFloat(row.final_score, 1)}</strong></div>
      </div>
      <div class="popup-tags">
        <span class="popup-tag">${buildabilityTag}</span>
        <span class="popup-tag">${titleTag}</span>
      </div>
      <div class="popup-flags">Flags: ${escapeHtml(riskSummary)}</div>
    </div>
  `;
}

function renderMarkers(options = {}) {
  const { fitToMarkers = true } = options;
  state.markerLayer.clearLayers();
  state.markers.clear();

  let mappedCount = 0;
  for (const row of state.filteredRows) {
    if (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)) {
      continue;
    }
    const key = keyFor(row);
    const marker = L.circleMarker([row.latitude, row.longitude], {
      radius: row.is_candidate ? 7 : 5,
      color: "#0b0d12",
      weight: 1.2,
      fillColor: scoreColor(row.final_score || 0),
      fillOpacity: 0.9,
    }).bindPopup(buildPopup(row), {
      autoPan: true,
      keepInView: true,
      offset: popupOffsetForLatLng([row.latitude, row.longitude]),
    });

    marker.on("popupopen", () => {
      applyPopupPlacement(marker);
      marker.getPopup()?.update();
    });

    marker.on("click", () => {
      selectRow(row, { openPopup: false, panToMarker: false });
    });

    marker.addTo(state.markerLayer);
    state.markers.set(key, marker);
    mappedCount += 1;
  }

  if (fitToMarkers && mappedCount > 0) {
    const group = L.featureGroup(Array.from(state.markers.values()));
    map.fitBounds(group.getBounds().pad(0.15), { maxZoom: 14 });
  }
}

function makeCard(row) {
  const key = keyFor(row);
  const active = key === state.activeKey ? "active" : "";
  const gateState = normalizeMapText(row.buildability_gate);
  const titleTier = String(row.title_lien_tier || "").toLowerCase();
  const riskCount = Number(row.risk_count || 0);
  const costTipHtml = costTooltipText(row);
  const buildabilityChip =
    gateState && gateState !== "pass"
      ? `<div class="chip warn">Buildability ${normalizeDisplayText(gateState)}</div>`
      : "";
  const titleChip =
    titleTier && titleTier !== "low" ? `<div class="chip warn">Title ${normalizeDisplayText(titleTier)}</div>` : "";
  const riskChip =
    riskCount > 0
      ? `<div class="chip warn">${hoverHint(`${riskCount} flag${riskCount === 1 ? "" : "s"}`, riskTooltipText(row), {
          align: "left",
          size: "wide",
          variant: "chip",
          label: "Flag details",
        })}</div>`
      : "";
  const mapChip =
    row.map_source === "boundary-centroid"
      ? `<div class="chip">${hoverHint(
          "Boundary mapped",
          `<div class="tip-copy">${escapeHtml(boundaryMappedTooltip())}</div>`,
          {
            align: "left",
            size: "narrow",
            variant: "chip",
            label: "Boundary mapped details",
          }
        )}</div>`
      : "";
  const unmappedChip =
    !Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)
      ? '<div class="chip warn">Unmapped</div>'
      : "";

  const card = document.createElement("div");
  card.className = `card ${active}`;
  card.innerHTML = `
    <div class="top">
      <div class="apn">${row.parcel_id}</div>
      <div class="rank">#${row.display_rank ?? "-"}</div>
    </div>
    <div class="meta">${normalizeDisplayText(row.city)} | ${normalizeDisplayText(
      row.county
    )} | ${normalizeDisplayText(row.property_type)}</div>
    <div class="meta">${normalizeDisplayText(row.address)}</div>
    <div class="metrics">
      <div>Opening bid: <strong>${formatMoney(row.opening_bid)}</strong></div>
      <div>Max suggested bid: <strong>${formatMoney(row.max_suggested_bid)}</strong></div>
      <div>Upside: <strong>${formatMoney(row.final_upside)}</strong></div>
      <div>Return: <strong>${formatPct(row.final_roi_pct)}</strong></div>
      <div>Score: <strong>${formatFloat(row.final_score, 1)}</strong></div>
      <div>${hoverHint("Total predicted cost", costTipHtml, {
        align: "right",
        placement: "below",
        size: "wide",
        label: "Cost breakdown",
      })}: <strong>${formatMoney(row.final_total_cost)}</strong></div>
    </div>
    <div class="chips">
      ${buildabilityChip}
      ${titleChip}
      ${row.requires_attorney_review ? '<div class="chip warn">Attorney Review</div>' : ""}
      ${riskChip}
      ${mapChip}
      ${unmappedChip}
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
  const recommended = state.filteredRows.filter((r) => r.is_candidate).length;
  const statsParts = [
    `${shown}/${total}`,
    `${recommended} recommended`,
    `${mapped} mapped`,
    `${state.visibleBoundaryCount} boundaries`,
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

function renderBoundaries() {
  state.boundaryLayer.clearLayers();
  state.visibleBoundaryCount = 0;

  const rows = state.filteredRows.slice(0, MAX_VISIBLE_BOUNDARIES);
  for (const row of rows) {
    const norm = normalizeParcelQuery(row);
    if (!norm) continue;
    const cached = state.boundaryCache.get(norm.cacheKey);
    if (!cached || !cached.rings) continue;

    const latlngs = ringsToLatLngs(cached.rings);
    if (!latlngs.length) continue;

    const isActive = keyFor(row) === state.activeKey;
    const poly = L.polygon(latlngs, {
      color: isActive ? "#ffe066" : "#89c2ff",
      weight: isActive ? 3 : 1,
      fillColor: isActive ? "#ffd43b" : "#7fb8ff",
      fillOpacity: isActive ? 0.12 : 0.08,
      interactive: false,
    });
    poly.addTo(state.boundaryLayer);
    state.visibleBoundaryCount += 1;
  }
}

async function refreshBoundaries() {
  state.boundaryLayer.clearLayers();
  state.visibleBoundaryCount = 0;

  const token = ++state.boundaryBatchToken;
  const rows = state.filteredRows.slice(0, MAX_VISIBLE_BOUNDARIES);
  await prefetchVisibleBoundaries(rows, token);
  if (token !== state.boundaryBatchToken) return;
  renderBoundaries();
  renderStats();
}

async function selectRow(row, options = {}) {
  const { openPopup = false, panToMarker = false } = options;
  const token = ++state.selectionToken;
  state.activeKey = keyFor(row);
  renderList();

  let marker = state.markers.get(state.activeKey);
  if (marker && panToMarker) {
    map.setView(marker.getLatLng(), 16);
  }
  if (marker && openPopup) {
    applyPopupPlacement(marker);
    marker.openPopup();
  }

  const boundary = await fetchBoundaryForRow(row);
  if (token !== state.selectionToken) return;
  if (boundary?.rings) {
    const centroid = centroidFromRings(boundary.rings);
    if (
      centroid &&
      (!Number.isFinite(row.latitude) || !Number.isFinite(row.longitude))
    ) {
      row.latitude = centroid.lat;
      row.longitude = centroid.lon;
      row.map_source = "boundary-centroid";
      renderMarkers({ fitToMarkers: false });
      marker = state.markers.get(state.activeKey);
    }

    if ((!marker || !Number.isFinite(row.latitude) || !Number.isFinite(row.longitude)) && panToMarker) {
      const latlngs = ringsToLatLngs(boundary.rings);
      if (latlngs.length) {
        map.fitBounds(L.polygon(latlngs).getBounds().pad(0.15), { maxZoom: 16 });
      }
    }
  }

  renderBoundaries();
  renderStats();
}

function refresh() {
  applyFilters();
  renderMarkers();
  renderList();
  renderStats();
  refreshBoundaries();

  if (state.activeKey) {
    const activeRow = state.rowByKey.get(state.activeKey);
    if (activeRow) {
      selectRow(activeRow, { openPopup: false, panToMarker: false });
    } else {
      state.boundaryLayer.clearLayers();
    }
  } else {
    state.boundaryLayer.clearLayers();
  }
}

function attachEvents() {
  if (els.toggleFiltersBtn) {
    els.toggleFiltersBtn.addEventListener("click", () => {
      setFiltersOpen(!state.filtersOpen);
      map.invalidateSize();
    });
  }

  for (const el of [
    els.searchInput,
    els.countySelect,
    els.buildableOnly,
    els.showUnmapped,
    els.limitInput,
  ]) {
    el.addEventListener("input", refresh);
    el.addEventListener("change", refresh);
  }

  els.resetBtn.addEventListener("click", () => {
    els.searchInput.value = "";
    els.countySelect.value = "all";
    els.buildableOnly.checked = false;
    els.showUnmapped.checked = false;
    els.limitInput.value = "10";
    state.activeKey = null;
    refresh();
  });

  window.addEventListener("resize", () => {
    syncLayoutForViewport();
  });
}

async function main() {
  try {
    const rows = await loadRows();
    state.allRows = rows;
    state.rowByKey = new Map(rows.map((row) => [keyFor(row), row]));
    await preloadBoundaryCache();
    applyBoundaryCoordinateFallback(rows);
    attachEvents();
    syncLayoutForViewport(true);
    refresh();
  } catch (err) {
    console.error(err);
    els.stats.textContent =
      "Failed to load output CSV files. Run scoring + geocoding, then serve this repo with a local HTTP server.";
  }
}

main();
