const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');
const { RxDBJsonDumpPlugin } = require('rxdb/plugins/json-dump');

addRxPlugin(RxDBJsonDumpPlugin);

const reservationSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id:        { type: 'string', maxLength: 100 },
    user_id:   { type: 'string' },
    hotel_id:  { type: 'string' },
    check_in:  { type: 'string' },
    check_out: { type: 'string' },
    status:    { type: 'string' },
  },
  required: ['id', 'user_id', 'hotel_id', 'check_in', 'check_out', 'status'],
};

let db = null;

const getDb = async () => {
  if (db) return db;

  db = await createRxDatabase({
    name: 'reservationsdb',
    storage: getRxStorageMemory(),
  });

  await db.addCollections({
    reservations: { schema: reservationSchema },
  });

  console.log('RxDB initialisée');
  return db;
};

module.exports = { getDb };