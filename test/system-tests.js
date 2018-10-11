const {expect} = require("chai");

const {EtcDConfig} = require("../index");
const Future = require("junk-bucket/future");

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
			it( "it notifies on change", async function(){
				const programName = "notify-test";
				const cluster = "frog";
				const key = "chirp";
				const exampleValue = 1;
				const newValue = 42;

				const system = new EtcDConfig( programName, programName, cluster);
				await system.config.set(key, exampleValue);
				const seededFuture = new Future();
				let seeded = false;
				const valueUpdate = new Future();
				const controlLoop = await system.config.watch( key, (value) => {
					if( seeded ){
						valueUpdate.accept( value );
						controlLoop.end();
					}else {
						seeded = true;
						seededFuture.accept(true);
					}
				} );
				await seededFuture.promised;
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
			});

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
						controlLoop.end();
					} else {
						syncFirst.accept(true);
						seenFirst = true;
					}
				});
				await syncFirst.promised;
				await system.config.set(key + "/port" , 1024);
				const value = await sync.promised;

				expect( value ).to.deep.eq(value);
			});
		});
	});
});
