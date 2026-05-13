const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { Kafka } = require('kafkajs');
const db = require('./db');

const packageDef = protoLoader.loadSync(
  path.join(__dirname, '..', 'proto', 'hotel.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const hotelProto = grpc.loadPackageDefinition(packageDef).hotel;

const formatHotel = (h) => ({
  id: h.id, name: h.name, location: h.location,
  price: h.price, available_rooms: h.available_rooms,
});

const createHotel = (call, callback) => {
  const { name, location, price, rooms } = call.request;
  const id = uuidv4();
  db.prepare('INSERT INTO hotels (id, name, location, price, available_rooms) VALUES (?, ?, ?, ?, ?)')
    .run(id, name, location, price, rooms);
  callback(null, formatHotel(db.prepare('SELECT * FROM hotels WHERE id = ?').get(id)));
};

const getHotel = (call, callback) => {
  const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(call.request.id);
  if (!hotel) return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });
  callback(null, formatHotel(hotel));
};

const listHotels = (call, callback) => {
  const { location } = call.request;
  const hotels = location && location.trim() !== ''
    ? db.prepare('SELECT * FROM hotels WHERE location LIKE ?').all(`%${location}%`)
    : db.prepare('SELECT * FROM hotels').all();
  callback(null, { hotels: hotels.map(formatHotel) });
};

const updateHotel = (call, callback) => {
  const { id, name, location, price, rooms } = call.request;
  const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(id);
  if (!hotel) return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });

  db.prepare('UPDATE hotels SET name = ?, location = ?, price = ?, available_rooms = ? WHERE id = ?')
    .run(name || hotel.name, location || hotel.location, price || hotel.price, rooms || hotel.available_rooms, id);

  callback(null, formatHotel(db.prepare('SELECT * FROM hotels WHERE id = ?').get(id)));
};

const deleteHotel = (call, callback) => {
  const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(call.request.id);
  if (!hotel) return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });
  db.prepare('DELETE FROM hotels WHERE id = ?').run(call.request.id);
  callback(null, { success: true, message: 'Hôtel supprimé avec succès' });
};

const updateAvailability = (call, callback) => {
  const { hotel_id, rooms_to_reduce } = call.request;
  const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(hotel_id);
  if (!hotel) return callback({ code: grpc.status.NOT_FOUND, message: 'Hôtel non trouvé' });
  if (hotel.available_rooms < rooms_to_reduce) {
    return callback({ code: grpc.status.FAILED_PRECONDITION, message: 'Pas assez de chambres' });
  }
  db.prepare('UPDATE hotels SET available_rooms = available_rooms - ? WHERE id = ?').run(rooms_to_reduce, hotel_id);
  callback(null, { success: true, message: 'Disponibilité mise à jour' });
};

// Kafka Consumer
const startKafkaConsumer = async () => {
  const kafka = new Kafka({ clientId: 'hotel-service', brokers: ['localhost:9092'] });
  const consumer = kafka.consumer({ groupId: 'hotel-service-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'reservation.created', fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      const reservation = JSON.parse(message.value.toString());
      console.log('Kafka reçu - réservation:', reservation);
      const hotel = db.prepare('SELECT * FROM hotels WHERE id = ?').get(reservation.hotel_id);
      if (hotel && hotel.available_rooms > 0) {
        db.prepare('UPDATE hotels SET available_rooms = available_rooms - 1 WHERE id = ?').run(reservation.hotel_id);
        console.log(`Disponibilité mise à jour pour hôtel ${reservation.hotel_id}`);
      }
    },
  });
  console.log('Hotel Service - Kafka consumer démarré');
};

const server = new grpc.Server();
server.addService(hotelProto.HotelService.service, {
  createHotel, getHotel, listHotels, updateHotel, deleteHotel, updateAvailability
});

server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), async (err) => {
  if (err) { console.error('Erreur:', err); return; }
  console.log('Hotel Service gRPC démarré sur le port 50052');
  try { await startKafkaConsumer(); }
  catch (e) { console.warn('⚠️Kafka non disponible:', e.message); }
});