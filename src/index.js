//este script crea una conexion con la api para recibir los datos de tickPrices
import WebSocket from 'ws';
import EventEmitter from 'events';
import dotenv from 'dotenv';
import mysql from 'mysql2';

// Cargar variables de entorno
dotenv.config({ path: '../config/.env' });

const emi = new EventEmitter();

const xApiMainUrl = 'wss://ws.xtb.com/real';
const xApiStreamUrl = 'wss://ws.xtb.com/realStream';
const symbol = 'US100';

let streamSessionId = false;

const streamConnections = [];



const init = () => {
    const socket = new WebSocket(xApiMainUrl, {
        localAddress: '192.168.1.83'
    });

    socket.on('open', (data) => {
        console.log('Conexion establecida con: ', xApiMainUrl);


        //login
        const loginMes = {
            "command": "login",
            "arguments": {
                "userId": process.env.API_USER,
                "password": process.env.API_PASS
            }
        }

        socket.send(JSON.stringify(loginMes))
        // socket.send(JSON.stringify({
        //     "command": "getAllSymbols"
        // }))
    });

    socket.on('close', (data) => {
        const response = JSON.parse(data);
        console.log('MainConnection disconnected => ', response, ' timestamp => ', Date.now());
        init();
    });

    socket.on('error', (data) => {
        const response = JSON.parse(data);
        console.log('MainConnection ERROR => ', response, ' timestamp => ', Date.now());
        init();
    });

    socket.on('message', (data) => {
        const response = JSON.parse(data);
        // console.log('MainConn Response Received:', response);

        if (response.streamSessionId) {
            console.log('streamSessionId Received');
            emi.emit('streamSessionIdReceived');
            streamSessionId = response.streamSessionId;
            setInterval(() => {
                socket.send(JSON.stringify({
                    "command": "ping",
                    "streamSessionId": streamSessionId
                }));
            }, 10000);
        }

    });
}
emi.on('streamSessionIdReceived', () => {
    console.log('Emi has recieved a new streamSessionId');
    createStreamCon();
})

const createStreamCon = () => {
    let conAlive = false;
    const socket = new WebSocket(xApiStreamUrl, {
        localAddress: '192.168.1.83'
    });
    streamConnections.push(socket);
    socket.on('open', (data) => {
        console.log('Conexion establecida con: ', xApiStreamUrl);
        socket.send(JSON.stringify({
            "command": "getKeepAlive",
            "streamSessionId": streamSessionId
        }))
        setTimeout(() => {
            socket.send(JSON.stringify({
                "command": "getTickPrices",
                "streamSessionId": streamSessionId,
                "symbol": symbol,
                "minArrivalTime": 5000,
                "maxLevel": 0
            }));
        }, 220);
        setTimeout(() => {
            socket.send(JSON.stringify({
                "command": "getCandles",
                "streamSessionId": streamSessionId,
                "symbol": symbol
            }));
        }, 220);
    });

    socket.on('close', (data) => {
        const response = JSON.parse(data);
        console.log('StreamConnection disconnected => ', response, ' timestamp => ', Date.now());
    });

    socket.on('error', (data) => {
        const response = JSON.parse(data);
        console.log('StreamConnection ERROR => ', response, ' timestamp => ', Date.now());
    });

    socket.on('message', (data) => {
        const response = JSON.parse(data);


        if (response.command === 'keepAlive') {
            console.log('StreamCon is Alive');
            socket.send(JSON.stringify({
                "command": "ping",
                "streamSessionId": streamSessionId
            }))

        }
        else if (response.command === 'candle') {
            console.log('StreamCon has received a new Candle for : ', symbol);
            // console.log(response)
            emi.emit('newCandle', response.data);
        }
        else if (response.command === 'tickPrices') {
            console.log('StreamCon has received a new tickPrice for : ', symbol);
            // console.log(response.data)
            emi.emit('newTickPrice', response.data);




        } else {
            console.log('Stream Response Received:', response);
        }
    });

}

// const cleanStreamConnections = () => {

//     streamConnections.forEach(index, () => {
//         //deberia saber el comando que esta ejecutando cada conexion para hacer unsuscribe y luego eliminar la conexion
//         // si procede vuelvo a ejecutar el comando 
//         // necesito asociar comandos y conexiones 
//     })
// }

emi.on('newTickPrice', (data) => {
    let symbolId;

    const connection = mysql.createConnection({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME
    });

    // Conectar a la base de datos
    connection.connect((err) => {
        if (err) {
            console.error('Error al conectar a la base de datos:', err.stack);
            return;
        }
        console.log('Conectado a la base de datos!');

        // Select para obtener el ID 
        const query = 'SELECT id FROM symbol WHERE name = ?';
        connection.query(query, [symbol], (err, results) => {
            if (err) {
                console.error('Error en la consulta:', err);
                return;
            }

            if (results.length > 0) {
                symbolId = results[0].id;
                console.log('El ID obtenido es:', symbolId);

                //INSERT INTO TickPrice
                const query = `INSERT INTO TickPrice (id_symbol, ask_price, ask_volume, bid_price, bid_volume, price_level, high_price, low_price, quote_id, spread_raw, spread_table, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                connection.query(query, [symbolId, data.ask, data.askVolume, data.bid, data.bidVolume, data.level, data.high, data.low, data.quoteId, data.spreadRaw, data.spreadTable,new Date(data.timestamp) ], (err, results) => {
                    if (err) {
                        console.error('Error al insertar:', err);
                    } else {
                        console.log('Registro insertado con éxito, ID:', results.insertId);
                    }
                });
    
            } else {
                console.log('No se encontró el símbolo.');
            }

            // Cerrar la conexión después de la consulta
            connection.end();
        });
    });
      
})

emi.on('newCandle', (data) => {
    let symbolId;
        console.log(data);
        const connection = mysql.createConnection({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASS,
            database: process.env.DB_NAME
        });
    
        // Conectar a la base de datos
        connection.connect((err) => {
            if (err) {
                console.error('Error al conectar a la base de datos:', err.stack);
                return;
            }
            console.log('Conectado a la base de datos!');
    
            // Select para obtener el ID 
            const query = 'SELECT id FROM symbol WHERE name = ?';
            connection.query(query, [symbol], (err, results) => {
                if (err) {
                    console.error('Error en la consulta:', err);
                    return;
                }
    
                if (results.length > 0) {
                    symbolId = results[0].id;
                    console.log('El ID obtenido es:', symbolId);
                    //INSERT INTO Candle
                    const query = `INSERT INTO Candle (id_symbol,  start_time, start_ctm, open_price, close_price, high_price, low_price, volume, quote_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                    connection.query(query, [symbolId, new Date(data.ctm), data.ctm, data.open, data.close, data.high, data.low, data.vol, data.quoteId], (err, results) => {
                        if (err) {
                            console.error('Error al insertar:', err);
                        } else {
                            console.log('Registro insertado con éxito, ID:', results.insertId);
                        }
                    }); 
                } else {
                    console.log('No se encontró el símbolo.');
                }
    
                // Cerrar la conexión después de la consulta
                connection.end();
            });
        });
     
})

init();