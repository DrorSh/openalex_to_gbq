import chalk from "chalk";
import fileSystem from "fs";
import fs from "fs";
import path from "path";
import ndjson from "ndjson";
import zlib from "zlib";
import JSON from "JSON";
import util from "util";
import stream from "stream";
import _ from "lodash";
//import recursiveKeyReplace from "recursive-key-replace";



// fix stuff
// ----------

//https://github.com/pbojinov/recursive-key-replace/blob/master/index.js

function recursiveKeyReplace(obj) {
    _.forOwn(obj, (value, key) => {
        // if key matches `search` term, replace all occurences with `replaceValue`
        if (_.includes(key, '-')) {
            const cleanKey = _.replace(key, /\-/g, '_');
            obj[cleanKey] = value;
            delete obj[key];
        }
        // continue recursively looping through if we have an object or array
        if (_.isObject(value)) {
            return recursiveKeyReplace(value);
        }
    });
    return obj;
}


function fixHyphen(data) {

       
    recursiveKeyReplace(data);

	return(data)

}



// #################################################

function fixRecord(data) {

}

async function fixFile(inPath, outPath, file) {
	console.log(inPath+"/"+file)

	var pipeline = util.promisify(stream.pipeline);
    var inputStream = fileSystem.createReadStream( inPath+"/"+file );
    var outputStream = fileSystem.createWriteStream( outPath+"/"+file );

    var transformOutStream = ndjson.stringify();
    
    var gunzip = zlib.createGunzip();
    var gzip = zlib.createGzip();

    let count = 0;

    var transformInStream = ndjson.parse()
			.on(
				"data",

				function handleRecord( data ) {
                    ++count % 100000 || console.log(count);

					data = fixHyphen(data);
                    
                    delete data.international; //TODO

				}
			)

			.on(
				"end",
				function handleEnd() {

					console.log( chalk.green( "ndjson parsing complete!" ) );

				}
			)
		;

    await pipeline(
        inputStream,
        gunzip,
        transformInStream,
        transformOutStream,
        gzip,
        outputStream
    );
}


async function start(inPath, outPath, files) {
    for(let i=0; i< files.length; i++){
        await fixFile(inPath, outPath, files[i]);
      }
}


// RUN
// -----


const VERSION = process.env.DATA_VERSION;
if (!VERSION) { console.error("Error: DATA_VERSION env var not set"); process.exit(1); }

const BASE_PATH = `./data/raw/${VERSION}/concepts/`;
const CONVERTED_PATH = `./data/converted/${VERSION}/concepts/`;

const folders = fs.readdirSync(BASE_PATH, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name);

folders.forEach(folder => {
    const inFolderPath = path.join(BASE_PATH, folder);
    const outFolderPath = path.join(CONVERTED_PATH, folder);

    if (!fs.existsSync(outFolderPath)) {
        fs.mkdirSync(outFolderPath, { recursive: true });
    }

    const files = fs.readdirSync(inFolderPath);
    start(inFolderPath, outFolderPath, files);
});