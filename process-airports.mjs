/**
 * Copyright (c) 2024 GBO Systems
 *
 * Process airport list from OurAirports, store as GeoJSON files
 * 
 */


import * as fs from "fs"
import * as path from "path"
import { ensurePathExistsSync, getArgs, groupBy, readStringFromFile, writeObjectToFile } from "./common.mjs"
import papaparse from "papaparse"

const now = new Date();
const accuracy = 6;

const toGeoJsonFeature = (record) => {

    const id = record.id;
    const lat = parseFloat(parseFloat(record.latitude_deg).toFixed(accuracy));
    const lon = parseFloat(parseFloat(record.longitude_deg).toFixed(accuracy));

    delete record.id;
    delete record.latitude_deg;
    delete record.longitude_deg;

    const keys = Object.keys(record);

    for (let key of keys) {
        if (record[key] === null || record[key] === "") {
            delete record[key];
        }
    }

    return {
        id: id,
        type: 'Feature',
        properties: record,
        geometry: {
            type: 'Point',
            coordinates: [lon, lat]
        }
    }
}

const execute = async () => {

    const args = getArgs();
    const csvPath = path.join(args.source, "airports.csv")
    
    await ensurePathExistsSync(args.destination);
    await ensurePathExistsSync(path.join(args.destination, "type"));
    await ensurePathExistsSync(path.join(args.destination, "iso_country"));

    if (!fs.existsSync(csvPath)) {
        throw new Error(`${csvPath} does not exist!`);
    }

    const csv = await readStringFromFile(csvPath);
    const result = papaparse.parse(csv, { header: true, skipEmptyLines: true });
    const allFeatures = result.data.filter(s => s.type != "closed").map(toGeoJsonFeature).sort((a, b) => {
        const aValue = a.name ? a.name.toLowerCase() : "";
        const bValue = b.name ? b.name.toLowerCase() : "";

        return aValue <= bValue ? -1 : 1;
    });

    await writeObjectToFile(path.join(args.destination, "all.geojson"), {
        type: "FeatureCollection",
        features: allFeatures
    });  

    const byType = groupBy(
        allFeatures.filter(s => typeof s.properties.type === "string"),
        s => s.properties.type);

    for (let key in byType) {
        await writeObjectToFile(path.join(args.destination, "type", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byType[key]
        });
    }

    const byCountryCode = groupBy(
        allFeatures.filter(s => typeof s.properties.iso_country === "string"),
        s => s.properties.iso_country);

    for (let key in byCountryCode) {
        await writeObjectToFile(path.join(args.destination, "iso_country", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byCountryCode[key]
        });
    }

    /* Write metadata.json */
    await writeObjectToFile(path.join(args.destination, "metadata.json"), {
        source: "https://github.com/davidmegginson/ourairports-data",
        credit: "Credit to David Megginson and OurAirports.",
        updated: now.getTime(),
        total: allFeatures.length,
        endpoints: {
            all: {
                url: "https://github.com/gbosystems/synthetic-api/raw/main/airports/all.geojson"
            },
            query: {
                url: "https://github.com/gbosystems/synthetic-api/raw/main/airports/{property}/{value}.geojson",
                properties: {
                    "iso_country": Object.keys(byCountryCode),
                    "type": Object.keys(byType)
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

console.log("Processed airports.");
process.exit(0);