const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const { Kafka } = require('kafkajs');
const { getDb } = require('./db');

const packageDef = protoLoader.loadSync(
  path.join(__dirname, '..', 'proto', 'reservation.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const reservationProto = grpc.loadPackageDefinition(packageDef).reservation;

let kafkaProducer = null;

const startKafkaProducer = async () => {
  const kafka = new Kafka({ clientId: 'reservation-service', brokers: ['localhost:9092'] });
  kafkaProducer = kafka.producer();
  await kafkaProducer.connect();
  console.log('Reservation Service - Kafka producer connecté');
};

const publishReservationEvent = async (reservation) => {
  if (!kafkaProducer) return;
  try {
    await kafkaProducer.send({
      topic: 'reservation.created',
      messages: [{ key: reservation.id, value: JSON.stringify(reservation) }],
    });
    console.log('📤 Kafka - événement reservation.created envoyé:', reservation.id);
  } catch (err) {
    console.warn('⚠️  Erreur Kafka:', err.message);
  }
};

const createReservation = async (call, callback) => {
  try {
    const db = await getDb();
    const { user_id, hotel_id, check_in, check_out } = call.request;
    const id = uuidv4();
    const reservation = { id, user_id, hotel_id, check_in, check_out, status: 'confirmed' };
    await db.reservations.insert(reservation);
    publishReservationEvent(reservation).catch(e => console.warn('Kafka error:', e.message));
    callback(null, reservation);
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
};

const getReservation = async (call, callback) => {
  try {
    const db = await getDb();
    const doc = await db.reservations.findOne(call.request.id).exec();
    if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: 'Réservation non trouvée' });
    callback(null, doc.toJSON());
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
};

const listByUser = async (call, callback) => {
  try {
    const db = await getDb();
    const docs = await db.reservations.find({ selector: { user_id: call.request.user_id } }).exec();
    callback(null, { reservations: docs.map(d => d.toJSON()) });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
};

const cancelReservation = async (call, callback) => {
  try {
    const db = await getDb();
    const doc = await db.reservations.findOne(call.request.id).exec();
    if (!doc) return callback({ code: grpc.status.NOT_FOUND, message: 'Réservation non trouvée' });
    await doc.patch({ status: 'cancelled' });
    callback(null, { success: true, message: 'Réservation annulée avec succès' });
  } catch (err) {
    callback({ code: grpc.status.INTERNAL, message: err.message });
  }
};

const main = async () => {
  await getDb();
  try { await startKafkaProducer(); }
  catch (e) { console.warn('⚠️Kafka non disponible'); }

  const server = new grpc.Server();
  server.addService(reservationProto.ReservationService.service, {
    createReservation, getReservation, listByUser, cancelReservation
  });

  server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err) => {
    if (err) { console.error('Erreur:', err); return; }
    console.log('Reservation Service gRPC démarré sur le port 50053');
  });
};

main();