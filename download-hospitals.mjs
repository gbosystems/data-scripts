/**
 * Copyright (c) 2024 GBO Systems
 *
 * Download hospital list from CMS, geocode results, store as GeoJSON files
 * 
 */

import * as path from "path"
import { ensurePathExistsSync, getArgs, groupBy, writeObjectToFile, writeStringToFile } from "./common.mjs"
import papaparse from "papaparse"


// SEE: https://data.cms.gov/provider-data/dataset/xubh-q36u
// SEE: https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0
// SEE: https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?limit=500&offset=500&count=true&results=true&schema=false&keys=true&format=json&rowIds=false
// SEE: https://www.here.com/docs/bundle/batch-api-developer-guide/page/topics/batch-request.html


const accuracy = 6;
const downloadBatchSize = 500;
const geocodeBatchSize = 1000;

const getDownloadUrl = (offset, limit) => `https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?limit=${limit}&offset=${offset}&count=true&results=true&schema=false&keys=true&format=json&rowIds=false`

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

const getRowForHospitalGeocode = (record) => {

    return [ record.facility_id, record.address, record.citytown, record.state, record.zip_code ];
}

const geocodeHospitalList = async (data) => {

    const result = {};
    let offset = 0;
    let total = data.length;

    const noMatch = [];

    do {
        const batch = data.slice(offset, geocodeBatchSize).map(getRowForHospitalGeocode);
        const batchAsCsv = papaparse.unparse(batch, { skipEmptyLines: true });

        console.log(`Geocoding batch ${offset} - ${offset + geocodeBatchSize}`);

        const url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
        const form = new FormData();
        form.set("addressFile", new File([batchAsCsv], "addresses.csv", {
            type: "text/csv",
        }), "addresses.csv");
        form.append('benchmark', 'Public_AR_Current'); //SEE: https://geocoding.geo.census.gov/geocoder/benchmarks

        const response = await fetch(url, {
            method: 'POST',
            body: form
        }).then(r => r.text());

        const parsedResponse = papaparse.parse(response, { header: false });
        const responseAsArray = parsedResponse.data;

        for (let record of responseAsArray) {
            if (record[2] === "Match") {
                const coords = record[5].split(',');
                const lon = parseFloat(parseFloat(coords[0]).toFixed(accuracy));
                const lat = parseFloat(parseFloat(coords[1]).toFixed(accuracy));

                result[record[0]] = {
                    "type": "Point",
                    "coordinates": [ lon, lat ]
                }
            } else {
                noMatch.push(record);
            }
        }

        offset += geocodeBatchSize;

    } while (offset < total)

    await writeObjectToFile(path.join("./", "nomatch.json"), noMatch);   


    return result;
}


const execute = async () => {

    const args = getArgs();
    
    await ensurePathExistsSync(args.path);
    //await ensurePathExistsSync(path.join(args.path, "country"));

    const data = await downloadHospitalList();
    const locations = await geocodeHospitalList(data, args.bingMapsKey);

    console.log(`LENGTH: ${Object.keys(locations).length}`)
    
    await writeObjectToFile(path.join(args.path, "all.json"), data);   

    await writeObjectToFile(path.join(args.path, "locations.json"), locations);   
    //await writeStringToFile(path.join(args.path, "trimmed.csv"), papaparse.unparse(trimmed, { header: false, skipEmptyLines: true }));   


}







try {
    await execute();
} catch (ex) {
    console.error(ex);
    process.exit(1);
}

console.log("Downloaded hospitals.");
process.exit(0);