import chalk from "chalk";
import fileSystem from "fs";
import fs from "fs";
import path from "path";
import ndjson from "ndjson";
import zlib from "zlib";
import JSON from "JSON";
import util from "util";
import stream from "stream";



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
			// If locations key exists, fix any null issn fields
			locations.forEach(location => {
				if (location.source && location.source.issn === null) {
					location.source.issn = [];
				}
				//if (location.source && location.source.issn_l === null) {
				//	location.source.issn_l = [];
				//}
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

	var authorships = authorships.map(author => {	
		if (author.institutions.length === 0) { 
			author.institutions.push(empty_institution)
		 }
		return(author)
	})

	return(authorships)
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
    // Check if abstract_inverted_index is not provided or if it's empty
    if (!abstractData || 
        Object.keys(abstractData.InvertedIndex).length === 0) {
        return null;
    }

    const { IndexLength, InvertedIndex } = abstractData;
    
    // Create an empty array with the length of IndexLength
    const wordsArray = new Array(IndexLength).fill(null);
    
    // For each word in the InvertedIndex, insert the word at its respective indices
    for (const word in InvertedIndex) {
        for (const position of InvertedIndex[word]) {
            wordsArray[position] = word;
        }
    }
    
    // Join the array to get the reconstructed abstract
    return wordsArray.join(' ');
}



// #################################################

function fixRecord(data) {

}

async function fixFile(inPath, outPath, file) {
	console.log(inPath+file)

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

					//data.authorships = fix_authorships(data.authorships);
					//data.host_venue = fix_host_venue(data.host_venue);
					//data.counts_by_year = fix_counts_by_year(data.counts_by_year);
					//data.mesh = fix_mesh(data.mesh);
					//data.alternate_host_venues = fix_alternate_host_venues(data.alternate_host_venues);
					//data.concepts = fix_concepts(data.concepts);
					data.apc_list = fix_apc_list(data.apc_list);
					data.locations = fix_locations(data.locations);
					data.best_oa_location = fix_best_oa_location(data.best_oa_location);
					data.primary_location = fix_primary_location(data.primary_location);

					//data = removeUnwantedProperties(data);
					delete data.host_venue;
					delete data.alternate_host_venues;
					delete data.apc_paid;
					delete data.apc_payment;
					
					data = removeCountriesProperty(data);

					//data.abstract_inverted_index = JSON.stringify(data.abstract_inverted_index);
					data.abstract_inverted_index = reconstructAbstract(data.abstract_inverted_index);
					
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


//const EXTENSION = '.gz';
//const FOLDER = 'works/updated_date=2023-07-07/'; //folder for conversion
//const inPath = './data/raw/' + FOLDER;
//const outPath = './data/converted/' + FOLDER;
//var files = fileSystem.readdirSync(inPath);
//start(inPath, outPath, files);

//const fs = require('fs');
//const path = require('path');

const BASE_PATH = './data/raw/works/';
const CONVERTED_PATH = './data/converted/works/';

// Get all folders under 'works'
const folders = fs.readdirSync(BASE_PATH, { withFileTypes: true })
    .filter(dirent => dirent.isDirectory())
    .map(dirent => dirent.name);

folders.forEach(folder => {
    const inFolderPath = path.join(BASE_PATH, folder);
    const outFolderPath = path.join(CONVERTED_PATH, folder);
    
    // Create corresponding folder in 'converted' if it doesn't exist
    if (!fs.existsSync(outFolderPath)) {
        fs.mkdirSync(outFolderPath, { recursive: true });
    }

    // Get all files in the current folder
    const files = fs.readdirSync(inFolderPath);

	//console.log(files);

    // Call your start function
    start(inFolderPath, outFolderPath, files);
});
