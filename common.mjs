/**
 * Copyright (c) 2024 GBO Systems
 */


import * as fs from "fs"


export const ensurePathExistsSync = async (path) => {

    if (!fs.existsSync(path)) {
        fs.mkdirSync(path, { recursive: true });
    }
}

export const getArgs = () => {

    const args = process.argv.slice(2);
    const result = {};

    for (let i = 0; i < args.length; i++) {
        if (args[i].startsWith("--")) {
            let key = args[i].substr(2).toLowerCase();

            if (args[i + 1]) {
                if (args[i + 1].startsWith("--")) {
                    result[key] = true;
                } else {
                    result[key] = args[i + 1];
                    i++;
                }
            } else {
                result[key] = true;
            }
        }
    }

    return result;
}

export const groupBy = (array, selector) => {

    const result = {};

    if (Array.isArray(array)) {
        for (let i = 0; i < array.length; i++) {

            const item = array[i];
            const key = selector(item, i);
            const group = result[key] ? result[key] : (result[key] = []);

            group.push(item);
        }
    }

    return result;
}

export const readObjectFromFile = async (fileName) => {

    return new Promise((resolve, reject) => {
        fs.readFile(fileName, (err, data) => {
            if (err) {
                reject(err);
            }
            try {
                resolve(JSON.parse(data.toString()));
            } catch (ex) {
                reject(ex);
            }            
        });
    });
}


export const readStringFromFile = async (fileName) => {

    return new Promise((resolve, reject) => {
        fs.readFile(fileName, (err, data) => {
            if (err) {
                reject(err);
            }
            
            resolve(data.toString());
        });
    });
}

export const writeObjectToFile = async (fileName, data) => {

    return new Promise((resolve, reject) => {
        fs.writeFile(fileName, JSON.stringify(data), (err) => {
            if (err) {
                reject(err);
            }
            resolve();
        });
    });
}

export const writeStringToFile = async (fileName, data) => {

    return new Promise((resolve, reject) => {
        fs.writeFile(fileName, data, (err) => {
            if (err) {
                reject(err);
            }
            resolve();
        });
    });
}

export const wait = async (t) => {

    return new Promise((resolve) => {
        setTimeout(() => {
            resolve();
        }, t);
    });
}