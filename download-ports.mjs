/**
 * Copyright (c) 2024 GBO Systems
 *
 * Download ports from the NGA and store as GeoJSON files
 * 
 * https://public.cyber.mil/pki-pke/tools-configuration-files/
 *  -> DoD Approved External PKI Certificate Trust Chains - Version 10.2
 *      -> _DoD\Trust_Anchors_Self-Signed\0-DoD_Root_CA_3.cer
 *      -> _DoD\Intermediate_and_Issuing_CA_Certs\1-DOD_SW_CA-66.cer
 * Combine certs into single .pem file
 * Powershell: $env:NODE_EXTRA_CA_CERTS=".\nga.mil-trust-chain.pem"
 *
 */


import { ensurePathExistsSync, getArgs, groupBy, writeObjectToFile } from "./common.mjs"
import * as path from "path"


const url = "https://msi.pub.kubic.nga.mil/api/publications/world-port-index?output=json";
const now = new Date();
const accuracy = 6;


const toGeoJsonFeature = (port) => {

    const id = port.globalId.substr(1, port.globalId.length - 2);
    const lat = parseFloat(port.ycoord.toFixed(accuracy));
    const lon = parseFloat(port.xcoord.toFixed(accuracy));

    return {
        id: id,
        type: 'Feature',
        properties: port,
        geometry: {
            type: 'Point',
            coordinates: [lon, lat]
        }
    }
}

const execute = async () => {

    const args = getArgs();
    
    await ensurePathExistsSync(args.path);
    await ensurePathExistsSync(path.join(args.path, "country"));
    await ensurePathExistsSync(path.join(args.path, "harbor-size"));
    await ensurePathExistsSync(path.join(args.path, "harbor-type"));

    const data = await fetch(url).then(r => r.json());
    const allFeatures = data.ports.map(toGeoJsonFeature).sort((a, b) => {
        const aValue = a.portName ? a.portName.toLowerCase() : "";
        const bValue = b.portName ? b.portName.toLowerCase() : "";

        return aValue <= bValue ? -1 : 1;
    });

    await writeObjectToFile(path.join(args.path, "all.geojson"), {
        type: "FeatureCollection",
        features: allFeatures
    });   

    const byHarborSize = groupBy(
        allFeatures.filter(s => typeof s.properties.harborSize === "string"),
        s => s.properties.harborSize);

    for (let key in byHarborSize) {
        await writeObjectToFile(path.join(args.path, "harbor-size", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byHarborSize[key]
        });
    }

    const byHarborType = groupBy(
        allFeatures.filter(s => typeof s.properties.harborType === "string"),
        s => s.properties.harborType);
    
    for (let key in byHarborType) {
        await writeObjectToFile(path.join(args.path, "harbor-type", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byHarborType[key]
        });
    }

    const byCountryCode = groupBy(
        allFeatures.filter(s => typeof s.properties.countryCode === "string"),
        s => s.properties.countryCode);
    
    for (let key in byCountryCode) {
        await writeObjectToFile(path.join(args.path, "country", `${key.toLowerCase()}.geojson`), {
            type: "FeatureCollection",
            features: byCountryCode[key]
        });
    }

    /* Write metadata.json */
    await writeObjectToFile(path.join(args.path, "metadata.json"), {
        source: url, 
        credit: "Credit to US National Geospatial-Intelligence Agency.",
        updated: now.getTime(),
        total: allFeatures.length,
        endpoints: {
            all: {
                url: "https://github.com/gbosystems/synthetic-api/raw/main/ports/all.geojson"
            },
            query: {
                url: "https://github.com/gbosystems/synthetic-api/raw/main/ports/{property}/{value}.geojson",
                values: {
                    "country": Object.keys(byCountryCode),
                    "harbor-size": Object.keys(byHarborSize),
                    "harbor-type": Object.keys(byHarborType),
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

console.log("Downloaded ports.");
process.exit(0);