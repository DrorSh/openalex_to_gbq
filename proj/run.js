// Require the core node modules.
//var chalk = require( 'chalk' );
//var fileSystem = require( 'fs' );
//var ndjson = require( 'ndjson' );

import chalk from "chalk";
import fileSystem from "fs";
import ndjson from "ndjson";


const inPath = '/proj/data/raw/';
const outPath = '/proj/data/converted/';

// create the i/o streams
var inputStream = fileSystem.createReadStream( inPath + "works.ndjson" );
var transformInStream = inputStream.pipe( ndjson.parse() );

var transformOutStream = ndjson.stringify();
var outputStream = transformOutStream.pipe( fileSystem.createWriteStream( outPath + "/works.ndjson" ) );


// Transform the input stream
transformInStream
			// Each "data" event will emit one item from our original record-set.
			.on(
				"data",
				function handleRecord( data ) {

					console.log( chalk.red( "Record (event):" ), data.id );
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
