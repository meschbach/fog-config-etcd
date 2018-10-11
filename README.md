# fog-config-etcd

A module for interacting with configurations stored with an EtcD instance.  This library will utilize the HTTP interface
for the EtcD instance.

## Usage

```javascript
const {EtcDConfig} = require("fog-config-etcd");

const etcd = EtcDConfig("program", "i-" + process.pid, "default", "http://localhost:2379");
``` 

### Configuration

```javascript
const watch = await etcd.config.watch("test", (value) => {
	console.log(value);
});
etcd.config.set("test", {some: "value"});

//on shutdown
watch.end();
```


### Service Discovery & Registry

```javascript
await etcd.registry.expose("http://localhost:1234");

const feet = await etcd.discovery.locate( "happy-feet" );
feet.forEach( (address) => {
	console.log(address);
});
```

## Life Cycle

This library is fairly young and will change over time.