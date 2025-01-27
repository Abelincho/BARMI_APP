//recibe los datos de eurjpy.txt y genera un fichero json de salida 
import fs from 'fs/promises'
import dotenv from 'dotenv';
import mysql from 'mysql2';


dotenv.config({ path: './config/.env' });

const symbol = 'EURJPY';
const filePath = './eurjpy1.txt';
let symbolId = false; 
let content = '';
let contentArr = [];
async function readFile() {

    try{
        content = await fs.readFile(filePath, 'utf8');
        //console.log(contenido); // Muestra el contenido del archivo
    }catch(err){
        console.error('Error al leer el archivo:', err);
    }
}

async function splitFile(){
    content.split('\n').forEach((element) => {
        const e = element.replace('\r', '').split(';');
        const data = {
            dateTime: buildTimestamp(e[0], e[1]),
            openPrice: e[2],
            highestPrice: e[3],
            lowestPrice: e[4],
            closePrice: e[5],
            volume: e[6]
        };
        contentArr.push(data);
        // console.log(data)
        insertBacktestingCandle(data);
    })
    // console.log(contentArr.length);   
}

function buildTimestamp(date, time){
      // Convertir la fecha de DD/MM/YYYY a YYYY-MM-DD
    const d = date.split('/'); // Divide la fecha en partes [DD, MM, YYYY]
    const mysqlDate = `${d[2]}-${d[1]}-${d[0]}`; // YYYY-MM-DD

    // Combinar fecha y hora
    const timestamp = `${mysqlDate} ${time}`;

    return timestamp; // Devuelve el timestamp en formato compatible con MySQL
}

function insertBacktestingCandle(data){
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
            const query = 'SELECT id FROM Symbol WHERE name = ?';
            connection.query(query, [symbol], (err, results) => {
                if (err) {
                    console.error('Error en la consulta:', err);
                    return;
                }
    
                if (results.length > 0) {
                    symbolId = results[0].id;
                    //console.log('El ID obtenido es:', symbolId);
    
                    //INSERT INTO backtesting_candle
                    const query = `INSERT INTO backtesting_candle (id_symbol, period_datetime, open_price, close_price, high_price, low_price, volume) VALUES (?, ?, ?, ?, ?, ?, ?)`;
                    connection.query(query, [symbolId, data.dateTime, data.openPrice, data.closePrice, data.highestPrice, data.lowestPrice,  data.volume], (err, results) => {
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
}

(async function main() {
    await readFile(); // Leer el archivo
    splitFile(); // Dividir el contenido en líneas
  })();
//lee el json e inserta los registros en la DB de DESA en backtesting_candle