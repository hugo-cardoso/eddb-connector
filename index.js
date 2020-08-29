const admin = require('firebase-admin');

const _ = require('underscore');
const fs = require('fs');
const zlib = require('zlib');
const zmq = require("zeromq");
const sock = zmq.socket('sub');
const serviceAccount = require('./firebaseKey.json');

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

sock.connect('tcp://eddn.edcd.io:9500');
sock.subscribe('')

function sendBatch(batch) {
    batch.commit().then(() => {
        console.log('Saved!')
    }).catch(e => console.log(e))
}

let batch = db.batch()
let writeCount = 0

sock.on('message', topic => {
    const topicParsed = JSON.parse(zlib.inflateSync(topic))
    const message = topicParsed.message

    if( topicParsed.$schemaRef == 'https://eddn.edcd.io/schemas/shipyard/2' ||
        topicParsed.$schemaRef == 'https://eddn.edcd.io/schemas/commodity/3' ||
        topicParsed.$schemaRef == 'https://eddn.edcd.io/schemas/journal/1'
    ) {

        if ( writeCount === 20 ) {
            sendBatch(batch)
            batch = db.batch()
            writeCount = 0
        }

        let ships = {}
        let commodities = {}

        if (topicParsed.$schemaRef === 'https://eddn.edcd.io/schemas/journal/1') {

            if (topicParsed.header.softwareName === 'E:D Market Connector [Windows]') {
                if (
                    (message.event === 'Scan' && message.ScanType === 'Detailed') ||
                    (message.event === 'Scan' && message.ScanType === 'AutoScan')
                ) {
                    const dbSystemRef = db.collection('systems').doc(message.StarSystem)

                    if (_.has(message, 'StarType')) {
    
                        let star = {}
                        star = {
                            position: message.StarPos,
                            name: message.BodyName,
                            type: message.StarType,
                            absoluteMagnitude: message.AbsoluteMagnitude,
                            luminosity: message.Luminosity,
                            radius: message.Radius,
                            stellarMass: message.StellarMass,
                            temperature: message.SurfaceTemperature
                        }

                        if ( _.has(message, 'OrbitalPeriod') ) star['orbitalPeriod'] = message.OrbitalPeriod
                        if ( _.has(message, 'OrbitalInclination') ) star['orbitalInclination'] = message.OrbitalInclination
                        
                        batch.set(dbSystemRef, {
                            stars: {
                                [message.BodyName]: star
                            }
                        }, {merge: true})
                        writeCount++
                        console.log(`Star [${writeCount}] ${message.StarSystem}/${message.BodyName}`)
                    }
                }
            }
        }

        if (topicParsed.$schemaRef === 'https://eddn.edcd.io/schemas/shipyard/2') {
            const dbStationRef = db.collection('systems').doc(message.systemName).collection('stations').doc(message.stationName)
            message.ships.forEach(shipName => {
                ships[shipName] = true
            })
            batch.set(dbStationRef, {
                ships: ships
            }, {merge: true})
            writeCount++
            console.log(`Shipyard [${writeCount}] ${message.systemName}/${message.stationName}`)
        }

        if (topicParsed.$schemaRef === 'https://eddn.edcd.io/schemas/commodity/3') {
            const dbStationRef = db.collection('systems').doc(message.systemName).collection('stations').doc(message.stationName)
            topicParsed.message.commodities.forEach(commoditie => {
                commodities[commoditie.name] = commoditie
            })
            batch.set(dbStationRef, {
                commodities: commodities
            }, {merge: true})
            writeCount++
            console.log(`Commodity [${writeCount}] ${message.systemName}/${message.stationName}`)
        }
    }
});