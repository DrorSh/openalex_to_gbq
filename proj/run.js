import chalk from "chalk";
import fileSystem from "fs";
import ndjson from "ndjson";
import zlib from "zlib";

const inPath = '/proj/data/raw/';
const outPath = '/proj/data/converted/';

// create the i/o streams

var inputStream = fileSystem.createReadStream( inPath + "0000_part_00.gz" ).pipe(zlib.createGunzip());
var transformInStream = inputStream.pipe( ndjson.parse() );

var transformOutStream = ndjson.stringify();
var outputStream = transformOutStream.pipe( fileSystem.createWriteStream( outPath + "/works.ndjson" ) );

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

// functions for fixing stuff

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
	if (!host_venue.issn) {
		host_venue.issn = [];		
	}
	return(host_venue)
};

function fix_counts_by_year(counts_by_year) {
	if (counts_by_year.length === 0) {
		counts_by_year.push(empty_counts_by_year)
	}
	
	return(counts_by_year)
}

function fix_mesh(mesh){
	if (mesh.length===0){
		mesh.push(empty_mesh)
	}
	return(mesh)
}

function fix_alternate_host_venues(alternate_host_venues) {

	if (alternate_host_venues.length===0) {
		alternate_host_venues.push(empty_alternate_host_venue)
	}

	return(alternate_host_venues)

}

function fix_concepts(concepts) {
	if (concepts.length===0) {
		concepts.push(empty_concept)
	}

	return(concepts)

}

// #################################################

// Transform the input stream
transformInStream
			// Each "data" event will emit one item from our original record-set.
			.on(
				"data",
				function handleRecord( data ) {

					data.authorships = fix_authorships(data.authorships);
					data.host_venue = fix_host_venue(data.host_venue);
					data.counts_by_year = fix_counts_by_year(data.counts_by_year);
					data.mesh = fix_mesh(data.mesh);
					data.alternate_host_venues = fix_alternate_host_venues(data.alternate_host_venues);
					data.concepts = fix_concepts(data.concepts);

					delete data.abstract_inverted_index;

					console.log( chalk.red( "Record:" ), data.id );
					transformOutStream.write(data);

				}
			)

			// Once ndjson has parsed all the input, let's indicate done.
			.on(
				"end",
				function handleEnd() {

					console.log( chalk.green( "ndjson parsing complete!" ) );
					transformOutStream.end();

				}
			)
		;


outputStream.on(
	"finish",
	function handleFinish() {

		console.log( chalk.green( "ndjson file saved!" ) );

	}
);
