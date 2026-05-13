const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const app = express();
app.use(express.json());

// ── Charger les clients gRPC ───────────────────────────────

const opts = { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true };

const userProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '..', 'proto', 'user.proto'), opts)
).user;

const hotelProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '..', 'proto', 'hotel.proto'), opts)
).hotel;

const reservationProto = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(__dirname, '..', 'proto', 'reservation.proto'), opts)
).reservation;

const userClient        = new userProto.UserService('localhost:50051', grpc.credentials.createInsecure());
const hotelClient       = new hotelProto.HotelService('localhost:50052', grpc.credentials.createInsecure());
const reservationClient = new reservationProto.ReservationService('localhost:50053', grpc.credentials.createInsecure());

// ── Helper gRPC → Promise ──────────────────────────────────

const grpcCall = (client, method, request) => {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
};

// ── Routes REST ────────────────────────────────────────────

// Users
app.post('/api/users', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'createUser', req.body);
    res.status(201).json(result);
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.get('/api/users/:id', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'getUser', { id: req.params.id });
    res.json(result);
  } catch (err) { res.status(404).json({ error: err.message }); }
});

app.post('/api/users/login', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'loginUser', req.body);
    res.json(result);
  } catch (err) { res.status(401).json({ error: err.message }); }
});

app.put('/api/users/:id', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'updateUser', { id: req.params.id, ...req.body });
    res.json(result);
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.delete('/api/users/:id', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'deleteUser', { id: req.params.id });
    res.json(result);
  } catch (err) { res.status(404).json({ error: err.message }); }
});

// Hotels
app.post('/api/hotels', async (req, res) => {
  try {
    const result = await grpcCall(hotelClient, 'createHotel', req.body);
    res.status(201).json(result);
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.get('/api/hotels', async (req, res) => {
  try {
    const result = await grpcCall(hotelClient, 'listHotels', { location: req.query.location || '' });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/hotels/:id', async (req, res) => {
  try {
    const result = await grpcCall(hotelClient, 'getHotel', { id: req.params.id });
    res.json(result);
  } catch (err) { res.status(404).json({ error: err.message }); }
});

app.put('/api/hotels/:id', async (req, res) => {
  try {
    const result = await grpcCall(hotelClient, 'updateHotel', { id: req.params.id, ...req.body });
    res.json(result);
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.delete('/api/hotels/:id', async (req, res) => {
  try {
    const result = await grpcCall(hotelClient, 'deleteHotel', { id: req.params.id });
    res.json(result);
  } catch (err) { res.status(404).json({ error: err.message }); }
});

// Reservations
app.post('/api/reservations', async (req, res) => {
  try {
    const result = await grpcCall(reservationClient, 'createReservation', req.body);
    res.status(201).json(result);
  } catch (err) { res.status(400).json({ error: err.message }); }
});

app.get('/api/reservations/:id', async (req, res) => {
  try {
    const result = await grpcCall(reservationClient, 'getReservation', { id: req.params.id });
    res.json(result);
  } catch (err) { res.status(404).json({ error: err.message }); }
});

app.get('/api/reservations/user/:userId', async (req, res) => {
  try {
    const result = await grpcCall(reservationClient, 'listByUser', { user_id: req.params.userId });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/reservations/:id', async (req, res) => {
  try {
    const result = await grpcCall(reservationClient, 'cancelReservation', { id: req.params.id });
    res.json(result);
  } catch (err) { res.status(404).json({ error: err.message }); }
});

// ── GraphQL ────────────────────────────────────────────────

const typeDefs = `#graphql
  type User {
    id: String
    name: String
    email: String
  }

  type Hotel {
    id: String
    name: String
    location: String
    price: Float
    available_rooms: Int
  }

  type Reservation {
    id: String
    user_id: String
    hotel_id: String
    check_in: String
    check_out: String
    status: String
  }

  type DeleteResult {
    success: Boolean
    message: String
  }

  type Query {
    getUser(id: String!): User
    listHotels(location: String): [Hotel]
    getHotel(id: String!): Hotel
    getReservation(id: String!): Reservation
    listReservationsByUser(user_id: String!): [Reservation]
  }

  type Mutation {
    createUser(name: String!, email: String!, password: String!): User
    updateUser(id: String!, name: String, email: String, password: String): User
    deleteUser(id: String!): DeleteResult

    createHotel(name: String!, location: String!, price: Float!, rooms: Int!): Hotel
    updateHotel(id: String!, name: String, location: String, price: Float, rooms: Int): Hotel
    deleteHotel(id: String!): DeleteResult

    createReservation(user_id: String!, hotel_id: String!, check_in: String!, check_out: String!): Reservation
    cancelReservation(id: String!): DeleteResult
  }
`;

const resolvers = {
  Query: {
    getUser:    (_, args) => grpcCall(userClient, 'getUser', { id: args.id }),
    listHotels: (_, args) => grpcCall(hotelClient, 'listHotels', { location: args.location || '' }).then(r => r.hotels),
    getHotel:   (_, args) => grpcCall(hotelClient, 'getHotel', { id: args.id }),
    getReservation: (_, args) => grpcCall(reservationClient, 'getReservation', { id: args.id }),
    listReservationsByUser: (_, args) => grpcCall(reservationClient, 'listByUser', { user_id: args.user_id }).then(r => r.reservations),
  },
  Mutation: {
    createUser:  (_, args) => grpcCall(userClient, 'createUser', args),
    updateUser:  (_, args) => grpcCall(userClient, 'updateUser', args),
    deleteUser:  (_, args) => grpcCall(userClient, 'deleteUser', { id: args.id }),

    createHotel: (_, args) => grpcCall(hotelClient, 'createHotel', args),
    updateHotel: (_, args) => grpcCall(hotelClient, 'updateHotel', args),
    deleteHotel: (_, args) => grpcCall(hotelClient, 'deleteHotel', { id: args.id }),

    createReservation: (_, args) => grpcCall(reservationClient, 'createReservation', args),
    cancelReservation: (_, args) => grpcCall(reservationClient, 'cancelReservation', { id: args.id }),
  },
};

// ── Démarrer ───────────────────────────────────────────────

const startServer = async () => {
  const apolloServer = new ApolloServer({ typeDefs, resolvers });
  await apolloServer.start();

  app.use('/graphql', expressMiddleware(apolloServer));

  app.listen(3000, () => {
    console.log('API Gateway démarré sur http://localhost:3000');
    console.log('REST    → http://localhost:3000/api');
    console.log('GraphQL → http://localhost:3000/graphql');
  });
};

startServer();