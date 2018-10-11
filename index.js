const etcdjs = require("etcdjs");
const etcdjsPromise = require("etcdjs-promise");
const nope = require("junk-bucket");
const Future = require("junk-bucket/future");
const {parallel} = require("junk-bucket/future");

const assert = require("assert");

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
			return ;
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

module.exports = {
	EtcDConfig
};
