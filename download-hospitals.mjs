/**
 * Copyright (c) 2024 GBO Systems
 *
 * Download hospital list from CMS, geocode results, store as GeoJSON files
 * 
 */


import * as fs from "fs"
import * as path from "path"
import { ensurePathExistsSync, getArgs, groupBy, readObjectFromFile, wait, writeObjectToFile } from "./common.mjs"


// SEE: https://data.cms.gov/provider-data/dataset/xubh-q36u
// SEE: https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0
// SEE: https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?limit=500&offset=500&count=true&results=true&schema=false&keys=true&format=json&rowIds=false
// SEE: https://www.here.com/docs/bundle/batch-api-developer-guide/page/topics/batch-request.html


const now = new Date();
const downloadBatchSize = 500;
const geocodeRecordMax = 100;

const getDownloadUrl = (offset, limit) => `https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?limit=${limit}&offset=${offset}&count=true&results=true&schema=false&keys=true&format=json&rowIds=false`;

const getLocationString = (record) => `${record.address}, ${record.citytown}, ${record.state}, ${record.zip_code}`;

const toGeoJsonFeature = (record) => {

    const id = record.facility_id;

    return {
        id: id,
        type: 'Feature',
        properties: record,
        geometry: null
    }
}

const loadExistingDataset = async (existing) => {

    const allPath = path.join(existing, "all.geojson");
    const locationOverridePath = path.join(existing, "location-override.json");

    if (!fs.existsSync(allPath)) {
        return { data: null, locations: {}, override: {} };
    }

    const all = await readObjectFromFile(allPath);
    const locations = {};

    for (let feature of all.features) {
        if (feature.geometry === null) {
            continue;
        }

        const key = getLocationString(feature.properties);

        locations[key] = feature.geometry;
    }

    const override = await readObjectFromFile(locationOverridePath);

    return { data: all, locations, override };
}

const applyExistingLocationsToIncomingData = (incoming, existing) => {

    const locations = existing.locations ?? {};
    const override = existing.override ?? {};

    for (let feature of incoming) {
        const key = getLocationString(feature.properties);
        const geometry = override[feature.id] ?? locations[key];

        if (geometry) {
            feature.geometry = geometry;
        }
    }

    return incoming;
}

const downloadHospitalList = async () => {

    let offset = 0;
    let total = -1; //
    const records = [];
    
    do {
        const url = getDownloadUrl(offset, downloadBatchSize);

        console.log(`Downloading ${url}`);

        const response = await fetch(url).then(r => r.json());

        console.log(`Download complete, records = ${response.results.length}; count = ${response.count}`);

        records.push(... (response.results ?? []));

        offset += response.results.length;
        total = response.count;
    } while (records.length < total)

    return records;
}

const geocodeSingle = async (query, apiKey) => {

    const url = "https://geocode.search.hereapi.com/v1/geocode" +
        "?in=countryCode:USA" +
        `&q=${encodeURIComponent(query)}` +
        `&apiKey=${apiKey}`;

    const response = await fetch(url, {
        method: 'GET',
        headers: {
            'Accept': 'application/json'
        }
    });

    const status = response.status;
    const json = await response.json();
    
    if (status === 429) {
        throw new Error(`${json.error}: ${json.error_description}`);
    }

    return json;
}

const geocodeIncomingData = async (data, apiKey) => {

    let count = 0;

    console.log("Starting geocode...");

    for (let feature of data) {

        if (feature.geometry !== null) {
            continue;
        }

        let geocode;
        const query = getLocationString(feature.properties);

        count++;

        console.log(`${count}) Geocoding (${feature.id}) "${query}"`);

        try {
            geocode = await geocodeSingle(query, apiKey);
        } catch (ex) {
            console.error(ex);
            break;
        }

        if (Array.isArray(geocode.items) && geocode.items.length > 0) {
            const position = geocode.items[0].position;

            feature.geometry = {
                type: 'Point',
                coordinates: [position.lng, position.lat]
            }
        } else {
            console.log(geocode);
        }

        if (count >= geocodeRecordMax) {
            break;
        }

        await wait(500);
    }

    console.log(`Geocode complete, geocoded ${count} features`);

    return data;
}

const execute = async () => {

    const args = getArgs();
    
    await ensurePathExistsSync(args.path);
    await ensurePathExistsSync(path.join(args.path, "state"));
    await ensurePathExistsSync(path.join(args.path, "emergency_services"));

    const existing = await loadExistingDataset(args.existing);
    const download = await downloadHospitalList();
    let incoming = download.map(toGeoJsonFeature).sort((a, b) => {
        const aValue = a.properties.facility_name ? a.properties.facility_name.toLowerCase() : "";
        const bValue = b.properties.facility_name ? b.properties.facility_name.toLowerCase() : "";

        return aValue <= bValue ? -1 : 1;
    });

    incoming = applyExistingLocationsToIncomingData(incoming, existing);

    if (typeof args.hereapikey === "string") {
        incoming = await geocodeIncomingData(incoming, args.hereapikey);
    }

    await writeObjectToFile(path.join(args.path, "all.geojson"), {
        type: "FeatureCollection",
        features: incoming
    });

    const byState = groupBy(
        incoming.filter(s => typeof s.properties.state === "string"),
        s => s.properties.state);
    
    for (let key in byState) {
        await writeObjectToFile(path.join(args.path, "state", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byState[key]
        });
    }

    const byEmergencyServices = groupBy(
        incoming.filter(s => typeof s.properties.emergency_services === "string"),
        s => s.properties.emergency_services);
    
    for (let key in byEmergencyServices) {
        await writeObjectToFile(path.join(args.path, "emergency_services", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byEmergencyServices[key]
        });
    }

    /* Write metadata.json */
    await writeObjectToFile(path.join(args.path, "metadata.json"), {
        source: "https://data.cms.gov/provider-data/dataset/xubh-q36u",
        credit: "Credit to the U.S Centers for Medicare & Medicaid Services and HERE Technologies.",
        updated: now.getTime(),
        total: incoming.length,
        endpoints: {
            all: {
                url: "https://github.com/gbosystems/data/raw/main/hospitals/all.geojson"
            },
            query: {
                url: "https://github.com/gbosystems/data/raw/main/hospitals/{property}/{value}.geojson",
                properties: {
                    "state": Object.keys(byState),
                    "emergency_services": Object.keys(byEmergencyServices)
                }
            }
        }
    });
}

try {
    await execute();
} catch (ex) {
    console.error(ex);
    process.exit(1);
}

console.log("Downloaded hospitals.");
process.exit(0);