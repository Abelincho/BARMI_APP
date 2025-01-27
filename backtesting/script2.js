import fs from 'fs/promises';
import dotenv from 'dotenv';
import mysql from 'mysql2/promise';

dotenv.config({ path: './config/.env' });

const symbol = 'EURJPY';
const filePath = './eurjpy1.txt';
let symbolId = null;
let contentArr = [];

async function readFile() {
  try {
    const content = await fs.readFile(filePath, 'utf8');
    return content;
  } catch (err) {
    console.error('Error al leer el archivo:', err);
    throw err;
  }
}

function parseFileContent(content) {
  const rows = content.split('\n').filter(line => line.trim() !== ''); // Filtrar líneas vacías
  const parsedData = rows.map(line => {
    const e = line.replace('\r', '').split(';');
    return {
      dateTime: buildTimestamp(e[0], e[1]),
      openPrice: parseFloat(e[2]),
      highestPrice: parseFloat(e[3]),
      lowestPrice: parseFloat(e[4]),
      closePrice: parseFloat(e[5]),
      volume: parseInt(e[6], 10)
    };
  });
  return parsedData;
}

function buildTimestamp(date, time) {
  const [day, month, year] = date.split('/');
  return `${year}-${month}-${day} ${time}`;
}

async function getSymbolId(connection, symbol) {
  const [rows] = await connection.execute('SELECT id FROM Symbol WHERE name = ?', [symbol]);
  if (rows.length > 0) {
    return rows[0].id;
  } else {
    throw new Error(`Símbolo '${symbol}' no encontrado en la base de datos.`);
  }
}

async function insertBacktestingCandles(connection, symbolId, candles) {
  const query = `
    INSERT INTO backtesting_candle
    (id_symbol, period_datetime, open_price, close_price, high_price, low_price, volume)
    VALUES (?, ?, ?, ?, ?, ?, ?)`;

  const promises = candles.map(data => {
    return connection.execute(query, [
      symbolId,
      data.dateTime,
      data.openPrice,
      data.closePrice,
      data.highestPrice,
      data.lowestPrice,
      data.volume
    ]);
  });

  await Promise.all(promises);
  console.log(`${candles.length} registros insertados con éxito.`);
}

async function writeJsonOutput(data) {
  try {
    await fs.writeFile('./output.json', JSON.stringify(data, null, 2));
    console.log('Archivo JSON generado correctamente: output.json');
  } catch (err) {
    console.error('Error al escribir el archivo JSON:', err);
  }
}

(async function main() {
  try {
    // Leer el archivo de texto
    const content = await readFile();

    // Parsear el contenido
    const candles = parseFileContent(content);
    contentArr = candles; // Para usarlo más adelante si es necesario

    // Escribir el archivo JSON de salida
    await writeJsonOutput(candles);

    // Conectar a la base de datos
    const connection = await mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME
    });

    try {
      // Obtener el ID del símbolo
      symbolId = await getSymbolId(connection, symbol);

      // Insertar los datos en la base de datos
      await insertBacktestingCandles(connection, symbolId, candles);
    } finally {
      // Cerrar la conexión a la base de datos
      await connection.end();
    }
  } catch (err) {
    console.error('Error en la ejecución del script:', err);
  }
})();
