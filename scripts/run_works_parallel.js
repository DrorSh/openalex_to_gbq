import chalk from "chalk";
import fs from "fs";
import path from "path";
import split2 from "split2";
import zlib from "zlib";
import JSON from "JSON";
import util from "util";
import stream, { Transform } from "stream";
import os from "os";

const pipelineAsync = util.promisify(stream.pipeline);

// Concurrency: default to number of CPUs, override with CONCURRENCY env var
const CONCURRENCY = parseInt(process.env.CONCURRENCY, 10) || os.cpus().length;

// Progress tracking
const progress = {
	startTime: Date.now(),
	totalFiles: 0,
	completedFiles: 0,
	skippedFiles: 0,
	totalRecords: 0,
	activeFiles: new Set(),
};

function elapsed() {
	const sec = (Date.now() - progress.startTime) / 1000;
	if (sec < 60) return `${sec.toFixed(0)}s`;
	if (sec < 3600) return `${Math.floor(sec / 60)}m ${Math.floor(sec % 60)}s`;
	return `${Math.floor(sec / 3600)}h ${Math.floor((sec % 3600) / 60)}m`;
}

function rateStr() {
	const sec = (Date.now() - progress.startTime) / 1000;
	if (sec < 1) return "—";
	const rate = progress.totalRecords / sec;
	if (rate >= 1000) return `${(rate / 1000).toFixed(1)}k rec/s`;
	return `${rate.toFixed(0)} rec/s`;
}

function printProgress(extra) {
	const parts = [
		`[${elapsed()}]`,
		`Files: ${progress.completedFiles}/${progress.totalFiles}` + (progress.skippedFiles ? ` (${progress.skippedFiles} skipped)` : ""),
		`Records: ${progress.totalRecords.toLocaleString()}`,
		rateStr(),
	];
	if (extra) parts.push(extra);
	console.log(chalk.cyan(parts.join(" | ")));
}

// fix stuff
// ----------

// empty objects

const empty_institution = {
	id: null,
	display_name: null,
	ror: null,
	country_code: null,
	type: null
}

const empty_counts_by_year = {
	year:  null,
	cited_by_count: null
}

const empty_mesh = {
	is_major_topic: null,
	descriptor_ui: null,
	descriptor_name: null,
	qualifier_ui: null,
	qualifier_name: null
}

const empty_alternate_host_venue = {
	id: null,
	display_name: null,
	type: null,
	url: null,
	is_oa: null,
	version: null,
	license: null
}

const empty_concept = {
	id: null,
	wikidata: null,
	display_name: null,
	level: null,
	score: null
}

const empty_host_venue = {
	license: null,
	issn: null,
	issn_l: null,
	publisher: null,
	is_oa: null,
	id: null,
	display_name: null,
	type: null,
	version: null,
	url: null
}

const empty_apc_list = {
	provenance: null,
	value_usd: null,
	currency: null,
	value: null
}

const empty_location = {
	license: null,
	pdf_url: null,
	is_oa: false,
	source: {
		publisher_id: null,
		host_organization_lineage: [],
		host_organization_lineage_names: [],
		is_in_doaj: false,
		publisher_lineage: [],
		issn_l: null,
		publisher_lineage_names: [],
		display_name: null,
		type: null,
		host_organization_name: null,
		issn: [],
		publisher: null,
		host_institution_lineage_names: [],
		is_oa: false,
		id: null,
		host_institution_lineage: [],
		host_organization: null
	},
	landing_page_url: null,
	version: null,
	doi: null,
	issn: []
};

// functions for fixing stuff

function fix_locations(locations) {
		if (!locations) {
			locations = [empty_location];
		} else {
			locations.forEach(location => {
				if (location.source && location.source.issn === null) {
					location.source.issn = [];
				}
			});
		}

	return(locations)
}

function fix_best_oa_location(best_oa_location) {
	if (!best_oa_location) {
        return empty_location;
    }

    if (best_oa_location.source && best_oa_location.source.issn === null) {
        best_oa_location.source.issn = [];
    }

    return best_oa_location;
}

function fix_primary_location(primary_location) {
	if (!primary_location) {
        return empty_location;
    }

    if (primary_location.source && primary_location.source.issn === null) {
        primary_location.source.issn = [];
    }

    return primary_location;
}

function fix_apc_list(apc_list) {
	if (!Array.isArray(apc_list)) {
		apc_list = apc_list ? [apc_list] : [];
	}

	if (apc_list.length === 0) {
		apc_list.push(empty_apc_list);
	}

	return apc_list;
}

function removeCountriesProperty(data) {
    if (data.authorships && Array.isArray(data.authorships)) {
        data.authorships.map(authorship => {
            delete authorship.countries;
            return authorship;
        });
    }
    return data;
}

function removeUnwantedProperties(data) {
    const newData = { ...data };
    delete newData.host_venue;
    delete newData.alternate_host_venues;
    return newData;
}


function fix_authorships(authorships) {
	if (!authorships) return [];
	return authorships.map(author => {
		if (!author.institutions || author.institutions.length === 0) {
			author.institutions = [empty_institution];
		}
		author.institutions.forEach(inst => {
			if (inst.lineage == null) inst.lineage = [];
		});
		if (author.affiliations) {
			author.affiliations.forEach(aff => {
				if (aff.institution_ids == null) aff.institution_ids = [];
			});
		}
		if (author.countries == null) author.countries = [];
		if (author.raw_affiliation_strings == null) author.raw_affiliation_strings = [];
		return author;
	});
}

function fix_top_level_arrays(data) {
	if (data.corresponding_author_ids == null) data.corresponding_author_ids = [];
	if (data.corresponding_institution_ids == null) data.corresponding_institution_ids = [];
	if (data.indexed_in == null) data.indexed_in = [];
	if (data.referenced_works == null) data.referenced_works = [];
	if (data.related_works == null) data.related_works = [];
	return data;
}

function fix_host_venue(host_venue) {
	if (!host_venue || host_venue.length === 0) {
		host_venue = [empty_host_venue];
	}
	return(host_venue);
}

function fix_counts_by_year(counts_by_year) {
	if (!counts_by_year || counts_by_year.length === 0) {
		counts_by_year = [empty_counts_by_year];
	}

	return(counts_by_year)
}

function fix_mesh(mesh){
	if (!mesh || mesh.length===0){
		mesh = [empty_mesh];
	}
	return(mesh)
}

function fix_alternate_host_venues(alternate_host_venues) {

	if (!alternate_host_venues || alternate_host_venues.length===0) {
		alternate_host_venues = [empty_alternate_host_venue];
	}

	return(alternate_host_venues)

}

function fix_concepts(concepts) {
	if (!concepts || concepts.length===0) {
		concepts = [empty_concept];
	}

	return(concepts)

}

function reconstructAbstract(abstractData) {
    if (!abstractData ||
        Object.keys(abstractData.InvertedIndex).length === 0) {
        return null;
    }

    const { IndexLength, InvertedIndex } = abstractData;

    const wordsArray = new Array(IndexLength).fill(null);

    for (const word in InvertedIndex) {
        for (const position of InvertedIndex[word]) {
            wordsArray[position] = word;
        }
    }

    return wordsArray.join(' ');
}



// #################################################

function fixRecord(data) {

}

async function fixFile(inPath, outPath, file) {
	const label = `${path.basename(inPath)}/${file}`;

	// Skip if output file already exists
	if (fs.existsSync(outPath + "/" + file)) {
		progress.skippedFiles++;
		return;
	}

	progress.activeFiles.add(label);

	const inputStream = fs.createReadStream(inPath + "/" + file);
	const outputStream = fs.createWriteStream(outPath + "/" + file);
	const gunzip = zlib.createGunzip();
	const gzip = zlib.createGzip();

	let count = 0;

	const fixTransform = new Transform({
		writableObjectMode: true,
		readableObjectMode: false,
		transform(line, encoding, cb) {
			const str = typeof line === "string" ? line : line.toString();
			if (!str) return cb();

			let data;
			try {
				data = JSON.parse(str);
			} catch (e) {
				return cb();
			}

			count++;
			progress.totalRecords++;

			if (count % 100000 === 0) {
				printProgress(`${label}: ${count.toLocaleString()}`);
			}

			// Fix schema issues (nulls, types)
			data.apc_list = fix_apc_list(data.apc_list);
			data.locations = fix_locations(data.locations);
			data.best_oa_location = fix_best_oa_location(data.best_oa_location);
			data.primary_location = fix_primary_location(data.primary_location);
			data.authorships = fix_authorships(data.authorships);
			fix_top_level_arrays(data);

			// Convert inverted index to JSON string (can't be a BQ nested type)
			if (data.abstract_inverted_index) {
				data.abstract_inverted_index = JSON.stringify(data.abstract_inverted_index);
			}

			cb(null, JSON.stringify(data) + "\n");
		},
		flush(cb) {
			progress.completedFiles++;
			progress.activeFiles.delete(label);
			console.log(chalk.green(`  ✓ ${label}: ${count.toLocaleString()} records`));
			cb();
		}
	});

	await pipelineAsync(
		inputStream,
		gunzip,
		split2(),
		fixTransform,
		gzip,
		outputStream
	);
}


// Run tasks with limited concurrency
async function runWithConcurrency(tasks, limit) {
    const results = [];
    const executing = new Set();

    for (const task of tasks) {
        const p = task().then(r => {
            executing.delete(p);
            return r;
        });
        executing.add(p);
        results.push(p);

        if (executing.size >= limit) {
            await Promise.race(executing);
        }
    }

    return Promise.all(results);
}


// RUN
// -----

const VERSION = process.env.DATA_VERSION;
if (!VERSION) { console.error("Error: DATA_VERSION env var not set"); process.exit(1); }

const BASE_PATH = `./data/raw/${VERSION}/works/`;
const CONVERTED_PATH = `./data/converted/${VERSION}/works/`;

// Get all folders under 'works', sorted alphabetically
let folders = fs.readdirSync(BASE_PATH, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name)
    .sort();

// Apply batch range if specified (e.g. BATCH_RANGE="1-50")
const batchRange = process.env.BATCH_RANGE;
if (batchRange) {
    const [startStr, endStr] = batchRange.split('-');
    const start_idx = parseInt(startStr, 10) - 1; // convert to 0-based
    const end_idx = parseInt(endStr, 10);
    console.log(`Batch mode: processing folders ${startStr}-${endStr} of ${folders.length}`);
    folders = folders.slice(start_idx, end_idx);
}

// Flatten all files into one task queue for file-level concurrency
const fileTasks = [];
for (const folder of folders) {
    const inFolderPath = path.join(BASE_PATH, folder);
    const outFolderPath = path.join(CONVERTED_PATH, folder);

    // Create output directory upfront
    if (!fs.existsSync(outFolderPath)) {
        fs.mkdirSync(outFolderPath, { recursive: true });
    }

    const files = fs.readdirSync(inFolderPath);
    for (const file of files) {
        fileTasks.push({ inPath: inFolderPath, outPath: outFolderPath, file });
    }
}

progress.totalFiles = fileTasks.length;
console.log(`Processing ${fileTasks.length} files across ${folders.length} folders with concurrency ${CONCURRENCY} ...`);
console.log("");

const tasks = fileTasks.map(({ inPath, outPath, file }) => () => fixFile(inPath, outPath, file));

runWithConcurrency(tasks, CONCURRENCY)
    .then(() => {
        console.log("");
        console.log(chalk.green.bold("═".repeat(60)));
        console.log(chalk.green.bold(`  All done in ${elapsed()}`));
        console.log(chalk.green.bold(`  ${progress.totalRecords.toLocaleString()} records across ${progress.completedFiles} files (${progress.skippedFiles} skipped)`));
        console.log(chalk.green.bold("═".repeat(60)));
    })
    .catch(err => {
        console.error("Fatal error:", err);
        process.exit(1);
    });
