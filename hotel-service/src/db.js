const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, '..', 'hotels.db'));

db.exec(`
  CREATE TABLE IF NOT EXISTS hotels (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    location        TEXT NOT NULL,
    price           REAL NOT NULL,
    available_rooms INTEGER NOT NULL
  )
`);

module.exports = db;