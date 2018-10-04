const {expect} = require("chai");

const etcdjs = require("etcdjs");
const etcdjsPromise = require("etcdjs-promise");
const nope = require("junk-bucket");
const Future = require("junk-bucket/future");
const {parallel} = require("junk-bucket/future");

const assert = require("assert");
const EventEmitter = require("events");

function etcdWatchControlLoop( path, recursive, mapInit, mapChange, onNotify, etcdClientPromise, etcdClient ){
	assert(etcdClientPromise);
	assert(etcdClient);

	let continueWatch = true;
	let cancelWait = nope;
	let lastSeenIndex = -1;

	//Retrieve the current state of that resource
	etcdClientPromise.get( path, { recursive } ).then(function(initValueResponse){
		if( !continueWatch ) return;
		//
		const node = initValueResponse.node;
		if( node.dir ){
			lastSeenIndex = node.nodes.reduce((term, node) =>{
				return term < node.modifiedIndex ? node.modifiedIndex : term;
			}, node.modifiedIndex);
		} else {
			lastSeenIndex = node.modifiedIndex;
		}
		notifyOfValue( mapInit( node ) );
	});

	//
	function notifyOfValue( what ){
		if( !continueWatch ) return;

		Promise.resolve( what ).then( function( value ){
			if( !continueWatch ) return;
			Promise.resolve( onNotify( value ) ).then( function(){
				waitForNextChange();
			});
		});
	}

	function waitForNextChange(){
		if( !continueWatch ) return;

		const waitIndex = lastSeenIndex + 1;
		cancelWait = etcdClient.get(path, {wait: true, waitIndex, recursive, consistent:true}, onCallback);
	}

	function onCallback(error, result, next){
		//Error handling
		if( error ){
			//If you are proxying through Docker you will receive this instead of actual values.
			if( error.code == "ESOCKETTIMEDOUT"){ return next(onCallback); }
			//Chain real errors
			// doneFuture.reject(error);
		}

		//Translate the values
		const node = result.node;
		lastSeenIndex = node.modifiedIndex;
		notifyOfValue( mapChange( node ) );
	}

	return {
		// done: doneFuture.promised,
		end: () =>{
			continueWatch = false;
			cancelWait();
			// doneFuture.accept(null);
		}
	};
}

function EtcDConfig( programName, instanceName, clusterName = "default", etcd = "http://localhost:12379" ) {
	const etcdClient = new etcdjs(etcd);
	const etcdClientPromise = new etcdjsPromise(etcdClient);

	this.registry = {
		expose: async ( port ) => {
			assert(port);

			const path = ["fog", clusterName, "registry", programName, instanceName ].join("/");
			const value = {port: port};
			await etcdClientPromise.set( path, JSON.stringify(value) )
		}
	};

	this.discovery = {
		locate: async (targetProgram) => {
			const path = [ "fog", clusterName, "registry", targetProgram ].join("/");

			const result = await etcdClientPromise.get( path, {recursive:true} )
			if( !result.node ) { return []; }
			const values = result.node.nodes.map( n => n.value ).filter( v => v != '').map( v => JSON.parse(v));
			return values;
		}
	}

	this.config = {
		get: async (key) => {
			const path = ["fog", clusterName, "config", programName, key].join("/");
			const result = await etcdClientPromise.get( path );
			if( !result.node ) { return undefined; }
			const rawValue = result.node.value;
			return JSON.parse(rawValue);
		},
		set: async (key, value) => {
			const path = ["fog", clusterName, "config", programName, key].join("/") ;
			const result = await etcdClientPromise.set( path, JSON.stringify(value) );
		},
		watch: async ( key, action ) => {
			const path = ["fog", clusterName, "config", programName, key].join("/");

			function mapJSONValue( node ){
				const rawValue = node.value;
				const value = JSON.parse( rawValue );
				return value;
			}
			return etcdWatchControlLoop( path, false, mapJSONValue, mapJSONValue, action, etcdClientPromise, etcdClient );
		},
		setCollection: async (key, collection) => {
			const keys = Object.keys( collection );
			await parallel( keys.map( async k => {
				const path = [key, k].join("/");
				const value = collection[k];
				await this.config.set(path, value);
			} ) );
		},
		getCollection: async (key) => {
			const path = ["fog", clusterName, "config", programName, key].join("/") ;
			const rawCollection = await etcdClientPromise.get(path, {recursive:true});
			if( !rawCollection.node ) { return {}; }
			const nodes = rawCollection.node.nodes;

			const prefixLength = rawCollection.node.key.length + 1;
			const colllection = nodes.reduce( (r, n) => {
				const rawValue = n.value;
				const value = JSON.parse( rawValue );
				const key = n.key.substring( prefixLength );
				r[key] = value;
				return r;
 			}, {});
			return colllection;
		},
		watchCollection: async ( key, action ) => {
			//Resolve our target resources
			const path = ["fog", clusterName, "config", programName, key].join("/");

			let initialValue;
			function mapInitialValue( node ){
				const nodes = node.nodes;

				const prefixLength = node.key.length + 1;
				const colllection = nodes.reduce( (r, n) => {
					const rawValue = n.value;
					const value = JSON.parse( rawValue );
					const key = n.key.substring( prefixLength );
					r[key] = value;
					return r;
				}, {});
				initialValue = colllection;
				return colllection;
			}

			function mapChangeSet( changeSet ){
				return changeSet;
			}

			//Retrieve the current state of that resource
			return etcdWatchControlLoop( path, true, mapInitialValue, mapChangeSet, action, etcdClientPromise, etcdClient );
		},
	}
}

describe("Service registration & discovery", function(){
	it("can register the default port for an application", async function(){
		const programName = "test-registry";
		const port = 12345;
		const config = new EtcDConfig( programName, "instance-0");
		await config.registry.expose(port);
		const addresses = await config.discovery.locate( programName );
		expect( addresses ).to.deep.eq([{port}]);
	});

	it("can reigster a port with a specific intent", async function(){
		const port = 8000;
		const intent = "why";
		const program = "specific-intent";

		const config = new EtcDConfig( program, "penny-5");
		await config.registry.expose(port, intent);
		const addresses = await config.discovery.locate( program, intent );
		expect( addresses ).to.deep.eq([{port}]);
	});
});

describe("Program configuration", function(){
	describe("for simple values", function(){
		it("can store and retrieve simple values", async function(){
			const programName = "program-config-test";
			const cluster = "red bud";
			const key = "some-value";
			const exampleValue = "tin can";

			const system = new EtcDConfig( programName, programName, cluster);

			await system.config.set(key, exampleValue );
			const value = await system.config.get(key);
			expect(value).to.eq(exampleValue);
		});

		it("can store and retrieve complex values", async function(){
			const programName = "program-config-test";
			const cluster = "red bud";
			const key = "some-value";
			const exampleValue = { chi: 'do', river: "valley"};

			const system = new EtcDConfig( programName, programName, cluster);

			await system.config.set(key, exampleValue );
			const value = await system.config.get(key);
			expect(value).to.deep.eq(exampleValue);
		});

		describe("for watching", function(){
			it( "it seeds the values on start", async function(){
				const programName = "notify-test";
				const cluster = "frog";
				const key = "chirp";
				const exampleValue = 1;

				const system = new EtcDConfig( programName, programName, cluster);
				await system.config.set(key, exampleValue);
				const initialValueFuture = new Future();
				const controlLoop = await system.config.watch( key, (value) => {
					initialValueFuture.accept( value )
				} );
				const initValue = await initialValueFuture.promised;
				controlLoop.end();
				expect( initValue ).to.deep.eq( exampleValue );
			});

			//EtcD or this code has a sync issue causing the callback to never to called.
			xit( "it notifies on change", async function(){
				const programName = "notify-test";
				const cluster = "frog";
				const key = "chirp";
				const exampleValue = 1;
				const newValue = 42;

				const system = new EtcDConfig( programName, programName, cluster);
				await system.config.set(key, exampleValue);
				let seeded = false;
				const valueUpdate = new Future();
				const controlLoop = await system.config.watch( key, (value) => {
					if( seeded ){
						valueUpdate.accept( value );
						controlLoop.end();
					}else {
						seeded = true;
					}
				} );
				await system.config.set(key, newValue );
				const value = await valueUpdate.promised;
				expect( value ).to.deep.eq( newValue );
			})
		});
	});

	describe("for collections", function(){
		it( "may store and retrieve a collection of items", async function(){
			const programName = "collections";
			const cluster = "real-life";
			const key = "adversary";
			const example = { devision: 'hero', gotta:'find', tough: 'enough'};

			const system = new EtcDConfig( programName, programName, cluster);
			await system.config.setCollection(key, example);
			const values = await system.config.getCollection(key);
			expect( values ).to.deep.eq( example );
		});

		describe("for watching", function(){
			it( "it seeds the values on start", async function(){
				const programName = "airplane";
				const cluster = "turbulance";
				const key = "shaky";
				const example = { trailmix: 1, port: 42, strange: 'loop'};

				const system = new EtcDConfig( programName, programName, cluster);
				await system.config.setCollection(key, example);

				const initialValue = new Future();
				const controlLoop = await system.config.watchCollection(key, function(changeset) {
					initialValue.accept( changeset );
				});
				const value = await initialValue.promised;

				controlLoop.end();
				expect( value ).to.deep.eq(value);
			})

			it( "it notifies on a single change", async function(){
				const programName = "airplane";
				const cluster = "turbulance";
				const key = "shaky";
				const example = { trailmix: 1, port: 42, strange: 'loop'};

				const system = new EtcDConfig( programName, programName, cluster);
				await system.config.setCollection(key, example);

				let seenFirst = false;
				const syncFirst = new Future();
				const sync = new Future();
				const controlLoop = await system.config.watchCollection(key, function(changeset) {
					if( seenFirst ){
						sync.accept( changeset );
					} else {
						syncFirst.accept(true);
						seenFirst = true;
					}
				});
				await syncFirst.promised;
				await system.config.set(key + "/port" , 1024);
				const value = await sync.promised;

				controlLoop.end();
				expect( value ).to.deep.eq(value);
			});
		});
	});
});
