var helper = require("node-red-node-test-helper");
var sparkplugNode = require("../mqtt-sparkplug-wrapper.js");
var should = require("should");
var mqtt = require("mqtt");
var pako = require('pako');


var spPayload = require('sparkplug-payload').get("spBv1.0");

helper.init(require.resolve('node-red'));

let testBroker = 'mqtt://localhost';
var client = null;

beforeEach(function (done) {
	helper.startServer(done);
});

afterEach(function (done) {

	if (client) {
		client.end();
	}
	helper.unload();
	helper.stopServer(done);
});

describe('mqtt sparkplug device node', function () {

	var simpleFlow = [
		{
			"id": "n1",
			"type": "mqtt sparkplug device",
			"name": "TEST2",
			"metrics": {
				"test": {
					"dataType": "Int32"
				},
				"test2": {
					"dataType": "Int32"
				}
			},
			"broker": "b1"
		},
		{
			"id": "b1",
			"type": "mqtt-sparkplug-broker",
			"name": "Local Host",
			"deviceGroup": "My Devices",
			"eonName": "Node-Red",
			"broker": "localhost",
			"port": "1883",
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"enableStoreForward": false,
			"primaryScada": "MY SCADA"
		}
	];

	it('should be loaded', function (done) {
		var flow = [{ id: "n1", type: "mqtt sparkplug device", name: "device" }];
		helper.load(sparkplugNode, flow, function () {
			var n1 = helper.getNode("n1");
			n1.should.have.property('name', 'device');
			done();
		});
	});

	/**
	* Verify NBirth is send when starting up Node-Red with a Device loaded.
	*/
	it('should send NBirth message', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics.should.containDeep([
					{ name: 'Node Control/Rebirth', type: 'Boolean', value: false },
					{ name: 'bdSeq', type: 'UInt64' } // We don't check nmber
				 ]);
				 payload.metrics[1].value.low.should.eql(0);
				 payload.metrics[1].value.high.should.eql(0);
				 payload.metrics[1].value.unsigned.should.eql(true);
				payload.should.have.property("seq").which.is.eql(0);
				done();
				client.end();
			}
		});

	}); // it end 

	/**
	* Verify NBirth is send when starting up Node-Red with a Device loaded.
	*/
	it('only sends one NBirth message', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		let firstMessage = true;

		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
						// TIME
						setTimeout(() => {
							client.end();
							done()
						}, 500);						

					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red"){
			
				firstMessage.should.equal(true);
				firstMessage = false;
			}
		});

	}); // it end 


	it('should send DBirth message', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				//console.log(payload);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics.should.containDeep([{
					name: 'test',
					type: 'Int32',
					value: 11,
					//timestamp: 1630716767231
					},
					{
					name: 'test2',
					type: 'Int32',
					value: 11,
					//timestamp: 1630716767232
					}
				]);
				payload.should.have.property("seq").which.is.eql(1);
				done();
				//client.end();
			}
			
		});

	}); // it end 


	it('should not send DBirth if no metrics', function (done) {

		flow = JSON.parse(JSON.stringify(simpleFlow)); 
		flow[0].metrics = {}; // N

		Object.keys(flow[0].metrics).length.should.eql(0);

		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, flow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						console.log("Whats going on");
						done(e);
					}
				});
			  }
			})
		  });

		client.on('message', function (topic, message) {
			topic.should.not.eql("spBv1.0/My Devices/DBIRTH/Node-Red/TEST2")
			
			setTimeout(function() {
				done();
			  }, 500);
		});

	}); // it end 



	it('should not birth when metrics missing and  birthImmediately = false', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
						b1.birthImmediately = false;
						setTimeout(() => done(), 500);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			topic.should.not.equal("spBv1.0/My Devices/DBIRTH/Node-Red/TEST2")
			
		});

	}); // it

	it('should send DBirth message when birth immediately is set', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;

		flow = JSON.parse(JSON.stringify(simpleFlow));
		flow[0].birthImmediately = true;

		// Add Extra node to test
		extraDevice = JSON.parse(JSON.stringify(simpleFlow[0]));
		extraDevice.id = "n3";
		extraDevice.name = "TEST2"
	
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, flow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {

			// Should only BIRTH device with birthImmi attribute set.
			topic.should.not.eql("spBv1.0/My Devices/DBIRTH/Node-Red/TEST3");

			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				payload.should.have.property("timestamp").which.is.a.Number();
				payload.should.have.property("seq").which.is.eql(1);
				done();
				//client.end();
			}
			
		});

	}); // it

	it('should send Properties in DBIRTH message', function (done) {
		client = mqtt.connect(testBroker);
		let ts = Date.now();
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						n1.receive({
							"definition": {
								"TEST/TEST": {
									"dataType": "Int32",
									"properties": {
										"engUnits": {
											"type": "string",
											"value": "inHg"
										}
									} // properties end
								} // Metrics end
							},
							"payload" : {
								"metrics" : [
									{
										"name": "TEST/TEST",
										"value": 5,
										"timestamp" : ts
									  
									}]
							}
						});
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message with properties to the broker
			
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics.should.containDeep([{
					name: 'TEST/TEST',
					type: 'Int32',
					value: 5,
					"properties": {
						"engUnits": {
							"type": "String",
							"value": "inHg"
						}
					}, // properties end
					timestamp: ts
					}
				]);
				payload.should.have.property("seq").which.is.eql(1);
				helper.getNode("n1").receive({
					"payload" : {
						"metrics" : [
							{
								"name": "TEST/TEST",
								"value": 15,
								"timestamp" : ts
							  
							}]
					}
				});
				
			}
			else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				// Verify that DDATA is not send on first metrics (with value 5)
				// Verify that Properties are not send on DDATA
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics.should.containDeep([{
					name: 'TEST/TEST',
					type: 'Int32',
					value: 15,
					timestamp: ts
					}
				]);
				done();
			}
			
		});

	}); // it end 

	// Should end DDEATH message
	it('should send NDEATH messages', function (done) {

		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		var deathSend = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });
		  var expectedBd = 0;
		  client.on('message', function (topic, message) {
			
			if (topic === "spBv1.0/My Devices/NBIRTH/Node-Red") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				// Check that bdSeq in inceased every time we reconnect
				// this is a Long.js object. Not sure why this is a long? but its according to specs.
				payload.metrics[1].value.low.should.eql(expectedBd);
				payload.metrics[1].value.high.should.eql(0);

				// Force Disconnect
				b1.deregister(n1);
			} else if (topic === "spBv1.0/My Devices/NDEATH/Node-Red"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				// Check that bdSeq in inceased every time we reconnect
				payload.metrics[0].value.low.should.eql(expectedBd);
				payload.metrics[0].value.high.should.eql(0);

				if (expectedBd == 5) {
					done();
				}
				expectedBd++;


				// Force Reconnect
				b1.register(n1);
			}
		});

	}); // it end 


	it('should send REBIRTH messages', function (done) {
		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		var deathSend = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					// Verify that we reset the seq to 0
					payload.should.have.property("seq").which.is.eql(1);
				}
			} else if (topic === "spBv1.0/My Devices/NDEATH/Node-Red"){
				deathSend = true;
			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
					// Ready to issue rebirth
					if (initBirthDone === true) {
						var buffer = Buffer.from(message);
						var payload = spPayload.decodePayload(buffer);
						payload.should.have.property("seq").which.is.eql(1);
						deathSend.should.eql(true);
						done();
	
					} else {
						var rebirth = {
							metrics : [
							{
								"name" : "Node Control/Rebirth",
								"type" : "Boolean",
								"value": true
							},
						]
					}	
					var payload = spPayload.encodePayload(rebirth);
	
					client.publish("spBv1.0/My Devices/NCMD/Node-Red",payload);
					initBirthDone = true;
					}
				}
		});

	}); // it end 

	it('should send valid NData in input', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				//payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(3);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH
				done();
				//client.end();
			}
			
		});

	}); // it end 

	it('should convert NData timestamp to EPOC', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(4);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH
				done();
				client.end();
			}
			
		});

	}); // it end 

	it('should warn when passing unknown NData metric', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			const n1 = helper.getNode("n1");
			n1.on('input', () => {
				n1.warn.should.be.calledWithExactly("mqtt-sparkplug-wrapper.errors.device-unknown-metric");
				done();
			}); 
			  n1.receive({
				"payload" : {
					"metrics": [
						{
							"name": "does_not_exits",
							"value": 100
						},
					]}
				}
			);			

		}); // end helper
	}); // it end 

	it('should warn when passing NData metric without name', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			let n1 = helper.getNode("n1");
			n1.on('input', () => {
				n1.warn.should.be.calledWithExactly("mqtt-sparkplug-wrapper.errors.missing-attribute-name");
				done();
			  });
			n1.receive({
				"payload" : {
					"metrics": [
						{
							"value": 100
						},
					]}
				}
			);

		}); // end helper
	}); // it end 

	it('should error when passing NData metric that is not array', function (done) {
		helper.load(sparkplugNode, simpleFlow, function () {
		
			let n1 = helper.getNode("n1");
			n1.receive({
				"payload" : {
					"metrics": {"A": "B"} }
				}
			);

			n1.on('call:error', call => {
				// XXX
				call.firstArg.should.eql("mqtt-sparkplug-wrapper.errors.device-no-metrics")
				done();
			  });
		}); // end helper
	}); // it end 


	it('should add null_value on DData without value', function (done) {
			client = mqtt.connect(testBroker);
			let n1;
			let b1;
			client.on('connect', function () {
				client.subscribe('#', function (err) {
				if (!err) {
					helper.load(sparkplugNode, simpleFlow, function () {
						try {
							n1 = helper.getNode("n1");
							b1 = n1.brokerConn;

							// Send all metrics to trigger DBIRTH
							n1.receive({
								"payload" : {
									"metrics": [
										{
											"name": "test",
											"value": 11
										},
										{
											"name": "test2",
											"value": 11
										}
									]}
								}
							);
						}catch (e) {
							done(e);
						}
					});
				}
				})
			});

			client.on('message', function (topic, message) {
				// Verify that we sent a DBirth Message to the broker
				//console.log("TOPIC:", topic);
				if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
					n1.receive({
						"payload" : {
							"metrics": [
								{
									"name": "test",
									"value": null
									//"timestamp": new Date()
								},
							]}
						}
					);
				} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
					try {
						var buffer = Buffer.from(message);
						var payload = spPayload.decodePayload(buffer);
						payload.should.have.property("timestamp").which.is.a.Number();
						payload.metrics[0].should.have.property("name").which.is.eql("test");
						payload.metrics[0].should.have.property("value").which.is.eql(null);
						payload.metrics[0].should.have.property("type").which.is.eql("Int32");
						done();
					} catch (e) {
						done(e);
					}

				}
				
			});

	}); // it end */

	// STORE FORWARD TESTING
	it('should buffer when primary SCADA IS OFFLINE', function (done) {
		client = mqtt.connect(testBroker);

		// WARN! We'll enable buffering for all tests
		simpleFlow[1].enableStoreForward = true;

		// SET OFFLINE
		// Send Birth
		// SET SCADA ONLINE
		// VERIFY BIRTH is send when ONLINE

		var initBirthDone = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.publish("STATE/MY SCADA", "OFFLINE", true);
			// Set Online after 250ms 
			setTimeout(() => client.publish("STATE/MY SCADA", "ONLINE", true), 500);
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;
						n1.on('call:error', call => {
							console.log("ERROR", call.firstArg);
							call.firstArg.should.eql("mqtt-sparkplug-wrapper.errors.payload-type-object")
							done();
						});
						b1.on('call:error', call => {
							console.log("ERROR1", call.firstArg);
							call.firstArg.should.eql("mqtt-sparkplug-wrapper.errors.payload-type-object")
							done();
						});
			
						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
						});
						
					}catch (e) {
						done(e);
					}
				});
			  }
			})
			
			
		  });

		  client.on('message', function (topic, message) {
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("seq").which.is.eql(0);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");

			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("seq").which.is.eql(1);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
				simpleFlow[1].enableStoreForward = false;
				done();
			}
		});
	}); // it end 

	it('should send valid DEFLATE NData in input2', function (done) {
		client = mqtt.connect(testBroker);
		simpleFlow[1].compressAlgorithm = "DEFLATE";
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload = pako.inflate(payload.body);
				buffer = Buffer.from(payload);
				payload = spPayload.decodePayload(buffer);
	
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				//payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(3);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH

				simpleFlow[1].compressAlgorithm = undefined;
				done();
				//client.end();
			}
			
		});

	}); // it end 

	it('should send valid GZIP NData in input2', function (done) {
		client = mqtt.connect(testBroker);
		simpleFlow[1].compressAlgorithm = "GZIP";
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload = pako.inflate(payload.body);
				buffer = Buffer.from(payload);
				payload = spPayload.decodePayload(buffer);
	
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				//payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(3);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH

				simpleFlow[1].compressAlgorithm = undefined;
				done();
				//client.end();
			}
			
		});

	}); // it end 

	it('should warn and send uncompressed on unknown Compression Algorithm', function (done) {
		client = mqtt.connect(testBroker);
		simpleFlow[1].compressAlgorithm = "WINZUP";
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						n1.on('call:warn', call => {
							call.should.be.calledWithExactly('mqtt-sparkplug-wrapper.errors.unable-to-encode-message');
							done();
						  });

						n1.on('input', () => {
							
							// FIXME: warn should be called, but its not! (works in node-red)
							// need to fix test 
							//n1.warn.should.be.calledWithExactly('mqtt-sparkplug-wrapper.errors.unable-to-encode-message');
							
						});
						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {


						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				//payload.should.have.property("uuid");
				//payload.uuid.should.eql("SPBV1.0_COMPRESSED");

				//payload = pako.inflate(payload.body);
				//buffer = Buffer.from(payload);
				//payload = spPayload.decodePayload(buffer);
	
				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value").which.is.eql(100);
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				//payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(3);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH

				simpleFlow[1].compressAlgorithm = undefined;
				done();
				//client.end();
			}
			
		});

	}); // it end 

	// Dynamic definition verification:
	it('should output DBIRTH and Metric when definition is passed in message', function (done) {

		client = mqtt.connect(testBroker);
		
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						
						n1 = helper.getNode("n1");
						n1.on('call:error', call => {
							// XXX
							call.firstArg.should.eql("mqtt-sparkplug-wrapper.errors.payload-type-object")
							done();
						});
						n1.receive({
							"definition" : {
								"TEST/TEST" : {
									"dataType" : "Int32"
								}
							},
							"payload" : {
								"metrics" : [
								{
									"name" : "TEST/TEST",
									"value" : 5
								}]
						}}); 
					}catch (e) {
						done(e);
					}
				});
				
			  }
			})
		  });

		client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				payload.should.have.property("timestamp").which.is.a.Number();
				
				payload.metrics.should.containDeep([
					{ name: 'TEST/TEST', type: 'Int32', value: 5 },
				]);
				
				done();
				//client.end();
			}
			/*n1.on('input', () => {
				n1.error.should.be.calledWithExactly("Metrics should be an Array");
				done();
				});*/
		});


		
	}); // it end 
	
	it('should error when definition has invalid dataType', function (done) {

		client = mqtt.connect(testBroker);

		client.on('connect', function () {
			client.subscribe('#', function (err) {
				if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						n1.on('call:error', call => {
							//console.log(call);
							call.should.be.calledWithExactly('mqtt-sparkplug-wrapper.errors.invalid-metric-definition');
							done();
						  });


						n1.receive({
							"definition" : {
								"TEST/TEST" : {
									"dataType" : "fooBar"
								}
							}
							});
					}catch (e) {
						console.log(e);
						done(e);
					}
				});
				}
			})
			});
	

	}); // it end 

	it('should error when definition does not have a dataType', function (done) {

		client = mqtt.connect(testBroker);

		client.on('connect', function () {
			client.subscribe('#', function (err) {
				if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						n1.on('call:error', call => {
							call.should.be.calledWithExactly('mqtt-sparkplug-wrapper.errors.invalid-metric-definition');
							done();
						  });


						n1.receive({
							"definition" : {
								"TEST/TEST" : {
									"foo" : "bar"
								}
							}
							});
					}catch (e) {
						console.log(e);
						done(e);
					}
				});
				}
			})
			});
	

	}); // it end 

	it('should send REBIRTH messages on updated definition', function (done) {
		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		var deathDone = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			  
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					// Verify that we reset the seq to 0
					payload.should.have.property("seq").which.is.eql(1);
				}
			} else if (topic === "spBv1.0/My Devices/DDEATH/Node-Red/TEST2"){
				deathDone = true;
			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				// Ready to issue rebirth
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					deathDone.should.eql(true);
					payload.metrics.should.containDeep([
						{ name: 'TEST/TEST', type: 'Int32', value: 10 },
					]);
					done();

				} else {
					// Here we should force a rebirth
					n1.receive({
						"definition" : {
							"TEST/TEST" : {
								"dataType" : "Int32"
							}
						},
						"payload" : {
							"metrics" : [
							{
								"name" : "TEST/TEST",
								"value" : 10
							}]
					}});
					initBirthDone = true;
				}
			}
		});
	}); // it end 

	it('should send REBIRTH messages on updated definition w cache sync.', function (done) {
		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		var deathDone = false;
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			  
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					// Verify that we reset the seq to 0
					payload.should.have.property("seq").which.is.eql(1);
				}
			} else if (topic === "spBv1.0/My Devices/DDEATH/Node-Red/TEST2"){
				deathDone = true;
			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				// Ready to issue rebirth
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					deathDone.should.eql(true);
					payload.metrics.should.containDeep([
						{ name: 'TEST/TEST', type: 'Int32', value: 10 },
						{ name: 'test2', type: 'Int32', value: 11 },
					]);
					done();

				} else {
					// Here we should force a rebirth
					n1.receive({
						"definition" : {
							"TEST/TEST" : {
								"dataType" : "Int32"
							},
							"test2"  : {
								"dataType" : "Int32"
							},
						},
						"payload" : {
							"metrics" : [
							{
								"name" : "TEST/TEST",
								"value" : 10
							}]
					}});
					initBirthDone = true;
				}
			}
		});
	}); // it end 

	// Check Rebirth
	it('should send REBIRTH on REBIRTH Command', function (done) {
		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		var deathDone = false;
		let n1;
		let b1;

		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			  
			
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					// Verify that we reset the seq to 0
					payload.should.have.property("seq").which.is.eql(1);
				}
			} else if (topic === "spBv1.0/My Devices/DDEATH/Node-Red/TEST2"){
				deathDone = true;
			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				// Ready to issue rebirth
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					deathDone.should.eql(true);
					done();

				} else {
					// Here we should force a rebirth
					n1.receive({
						"command" : {
							"device" : {
								"rebirth" : true
							}
						}   
					});
					initBirthDone = true;
				}
			}
		});
	}); // it end 

	// Check DEATH command will send DDEATH 
	it('should send DDEATH on DEATH Command', function (done) {
		client = mqtt.connect(testBroker);
		var initBirthDone = false;
		var deathDone = false;
		let n1;
		let b1;

		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			  
			
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red") {
				if (initBirthDone === true) {
					var buffer = Buffer.from(message);
					var payload = spPayload.decodePayload(buffer);
					// Verify that we reset the seq to 0
					payload.should.have.property("seq").which.is.eql(1);
				}
			} else if (topic === "spBv1.0/My Devices/DDEATH/Node-Red/TEST2"){
				done();
			} else if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){
				// Here we should force a rebirth
				n1.receive({
					"command" : {
						"device" : {
							"death" : true
						}
					}   
				});
				initBirthDone = true;
				
			}
		});
	}); // it end 


	it('should alias metrics if enabled', function (done) {
		client = mqtt.connect(testBroker);
		let n1;
		let b1;
		client.on('connect', function () {
			client.subscribe('#', function (err) {
			  if (!err) {
				simpleFlow[1].aliasMetrics = true;
				helper.load(sparkplugNode, simpleFlow, function () {
					try {
						n1 = helper.getNode("n1");
						b1 = n1.brokerConn;

						// Send all metrics to trigger DBIRTH
						n1.receive({
							"payload" : {
								"metrics": [
									{
										"name": "test",
										"value": 11,
									},
									{
										"name": "test2",
										"value": 11
									}
								]}
							}
						);
					}catch (e) {
						done(e);
					}
				});
			  }
			})
		  });

		  client.on('message', function (topic, message) {
			// Verify that we sent a DBirth Message to the broker
			//console.log("TOPIC:", topic);
			if (topic === "spBv1.0/My Devices/DBIRTH/Node-Red/TEST2"){

				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql("test");
				payload.metrics[0].should.have.property("value");
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				//payload.metrics[0].should.have.property("alias").which.is.eql(1);
				alias = payload.metrics[0].alias.toNumber();
				alias.should.eql(3);
				n1.receive({
					"payload" : {
						"metrics": [
							{
								"name": "test",
								"value": 100,
								//"timestamp": new Date()
							},
						]}
					}
				);
			} else if (topic === "spBv1.0/My Devices/DDATA/Node-Red/TEST2") {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);

				payload.should.have.property("timestamp").which.is.a.Number();
				payload.metrics[0].should.have.property("name").which.is.eql(""); // name is decoded to "" if missing
				payload.metrics[0].should.have.property("value");
				payload.metrics[0].should.have.property("type").which.is.eql("Int32");
				payload.metrics[0].should.have.property("alias");

				alias = payload.metrics[0].alias.toNumber();
				alias.should.eql(3);
				
				//payload.metrics[0].should.have.property("timestamp").which.is.a.Number();
				payload.metrics.length.should.eql(1);
				Object.keys(payload.metrics[0]).length.should.eql(4);
				payload.should.have.property("seq").which.is.eql(2); // 0 is NBIRTH, 1 is DBIRTH
				done();
				//client.end();
			}
			
		});

	}); // it end 

	// Check that 


	// TODO:
	//   Test unknown metric data type
	//   Test NDEATH
	//   Test Invalid DCMD
	
});


/**
 * MQTT Sparkplug B in testing
 */
describe('mqtt sparkplug in node', function () {

	var inExample = [
		{
			"id": "n2",
			"type": "helper",
		},
		{
			"id": "n1",
			"type": "mqtt sparkplug in",
			"name": "",
			"topic": "#", //"spBv1.0/+/DDATA/+/+",
			"qos": "2",
			"broker": "b1",
			"wires": [["n2"]]
		},
		{
			"id": "b1",
			"type": "mqtt-sparkplug-broker",
			"name": "Local Host",
			"deviceGroup": "My Devices",
			"eonName": "Node-Red",
			"broker": "localhost",
			"port": "1883",
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"credentials": {}
		}
	]
	var validMsg = {"timestamp":12345,"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200}

	it('should ouput a subscribed topic', function (done) {

		var testMsg = {
			topic : "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			payload : spPayload.encodePayload(validMsg)
		}

		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						msg.payload.should.deepEqual(validMsg);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish(testMsg.topic, testMsg.payload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});

	it('should decompress a DEFLATE encoded topic', function (done) {

		var testMsg = {
			topic : "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			payload : spPayload.encodePayload(validMsg)
		}

		var compressedPayload = {
            "uuid" : "SPBV1.0_COMPRESSED",
            body : pako.deflate(testMsg.payload),
            metrics : [ {
                "name" : "algorithm", 
                "value" : "DEFLATE", 
                "type" : "string"
            } ]
        };
		compressedPayload = spPayload.encodePayload(compressedPayload);
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						msg.payload.should.deepEqual(validMsg);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish(testMsg.topic, compressedPayload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});

	it('should decompress a GZIP encoded topic', function (done) {

		var testMsg = {
			topic : "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			payload : spPayload.encodePayload(validMsg)
		}

		var compressedPayload = {
            "uuid" : "SPBV1.0_COMPRESSED",
            body : pako.gzip(testMsg.payload),
            metrics : [ {
                "name" : "algorithm", 
                "value" : "GZIP", 
                "type" : "string"
            } ]
        };
		compressedPayload = spPayload.encodePayload(compressedPayload);
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						msg.payload.should.deepEqual(validMsg);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish(testMsg.topic, compressedPayload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});

	it('should error on invalid compression topic', function (done) {

		var compressedPayload = {
            "uuid" : "SPBV1.0_COMPRESSED",
            body : "Hello World!",
            metrics : [ {
                "name" : "algorithm", 
                "value" : "DEFLATE", 
                "type" : "string"
            } ],
			seq : 200
        };
		compressedPayload = spPayload.encodePayload(compressedPayload);
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			helper.load(sparkplugNode, inExample, function () {
				var n2 = helper.getNode("n2");
				var n1 = helper.getNode("n1");
				n1.on('call:error', call => {
					call.should.be.calledWithExactly('mqtt-sparkplug-wrapper.errors.unable-to-decode-message');
					done();
				  });

				n2.on("input", function (msg) {
					try {
					msg.should.have.property('payload');
					if (msg.payload.seq === 200) {
						console.log(payload);
						done();
					}else {
						// Nasty hack, to make sure we publish after node is online. 
						client.publish("spBv1.0/My Devices/DDATA/Node-Red/TEST2", compressedPayload);
					}
					} catch(err) {
						console.log("Error");
					  done(err);
					}
				  });
			});
		});
	});

});

/**  
 * mqtt sparkplug out testing
 */
describe('mqtt sparkplug out node', function () {

	var validMsg = {"timestamp":12345,"metrics":[{"name":"test","type":"Int32","value":100}],"seq":200}
	
	outFlow = [
		{
			"id": "n1",
			"type": "mqtt sparkplug out",
			"topic": "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			"broker": "b1",
			"wires": []
		},
		{
			"id": "b1",
			"type": "mqtt-sparkplug-broker",
			"name": "Local Host",
			"deviceGroup": "My Devices",
			"eonName": "Node-Red",
			"broker": "localhost",
			"port": "1883",
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"enableStoreForward": true,
			"primaryScada": "MY SCADA",
			"credentials": {}
		}
	]

	outFlow2 = [
		{
			"id": "n1",
			"type": "mqtt sparkplug out",
			"topic": "spBv1.0/My Devices/DDATA/Node-Red/TEST2",
			"broker": "b1",
			"wires": []
		},
		{
			"id": "b1",
			"type": "mqtt-sparkplug-broker",
			"name": "Local Host",
			"deviceGroup": "My Devices",
			"eonName": "Node-Red",
			"broker": "localhost",
			"port": "1883",
			"clientid": "",
			"usetls": false,
			"protocolVersion": "4",
			"keepalive": "60",
			"cleansession": true,
			"enableStoreForward": false,
			"primaryScada": "MY SCADA",
			"credentials": {}
		}
	]
	/**
	 * Verify that we outout a topic even though the primary SCADA is offline
	 */
	it('should ouput a publish topic', function (done) {

		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, function () {
					
						n1 = helper.getNode("n1");
						
						setTimeout(() => n1.receive({ payload: validMsg}), 500);
					});
				}
			});
		});

		client.on('message', function (topic, message) {
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("OFFLINE");
			payload.should.deepEqual(validMsg);
			done();
		});
	});

	/**
	 * Verify that we'll buffer if forced enabled,
	 */
	it('should buffer if publish topic', function (done) {

		var n1 = null;
		client = mqtt.connect(testBroker);
		client.on('connect', function () {
			client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
				if (!err) {
					helper.load(sparkplugNode, outFlow, function () {
						n1 = helper.getNode("n1");
						n1.shouldBuffer = true; // Force enable to buffer
						setTimeout(() => n1.receive({ payload: validMsg}), 500);
						setTimeout(() => client.publish("STATE/MY SCADA", "ONLINE", true), 1000);						});
				}
			});
		});

		client.on('message', function (topic, message) {
			var buffer = Buffer.from(message);
			var payload = spPayload.decodePayload(buffer);
			n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
			payload.should.deepEqual(validMsg);
			done();
		});
	});

		/**
	 * Verify that we'll buffer if forced enabled,
	 */
		it('should buffer on Sparkplug B 3.0.0 style State variable', function (done) {

			var n1 = null;
			client = mqtt.connect(testBroker);
			client.on('connect', function () {
				client.subscribe("spBv1.0/My Devices/DDATA/Node-Red/TEST2", function (err) {
					if (!err) {
						helper.load(sparkplugNode, outFlow, function () {
							n1 = helper.getNode("n1");
							n1.shouldBuffer = true; // Force enable to buffer
							setTimeout(() => n1.receive({ payload: validMsg}), 100);
							setTimeout(() => client.publish("spBv1.0/STATE/MY SCADA", JSON.stringify({ online : true, timestamp : new Date() }), true), 700);						});
					}
				});
			});
	
			client.on('message', function (topic, message) {
				var buffer = Buffer.from(message);
				var payload = spPayload.decodePayload(buffer);
				n1.brokerConn.primaryScadaStatus.should.eql("ONLINE");
				payload.should.deepEqual(validMsg);
				done();
			});
		});

});
