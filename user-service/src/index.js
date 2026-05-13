const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const db = require('./db');

const packageDef = protoLoader.loadSync(
  path.join(__dirname, '..', 'proto', 'user.proto'),
  { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
);
const userProto = grpc.loadPackageDefinition(packageDef).user;

const createUser = (call, callback) => {
  const { name, email, password } = call.request;
  const existing = db.prepare('SELECT * FROM users WHERE email = ?').get(email);
  if (existing) {
    return callback({ code: grpc.status.ALREADY_EXISTS, message: 'Email déjà utilisé' });
  }
  const id = uuidv4();
  db.prepare('INSERT INTO users (id, name, email, password) VALUES (?, ?, ?, ?)').run(id, name, email, password);
  callback(null, { id, name, email });
};

const getUser = (call, callback) => {
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(call.request.id);
  if (!user) return callback({ code: grpc.status.NOT_FOUND, message: 'Utilisateur non trouvé' });
  callback(null, { id: user.id, name: user.name, email: user.email });
};

const loginUser = (call, callback) => {
  const { email, password } = call.request;
  const user = db.prepare('SELECT * FROM users WHERE email = ? AND password = ?').get(email, password);
  if (!user) return callback(null, { success: false, message: 'Email ou mot de passe incorrect', userId: '' });
  callback(null, { success: true, message: 'Connexion réussie', userId: user.id });
};

const updateUser = (call, callback) => {
  const { id, name, email, password } = call.request;
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(id);
  if (!user) return callback({ code: grpc.status.NOT_FOUND, message: 'Utilisateur non trouvé' });

  db.prepare('UPDATE users SET name = ?, email = ?, password = ? WHERE id = ?')
    .run(name || user.name, email || user.email, password || user.password, id);

  const updated = db.prepare('SELECT * FROM users WHERE id = ?').get(id);
  callback(null, { id: updated.id, name: updated.name, email: updated.email });
};

const deleteUser = (call, callback) => {
  const { id } = call.request;
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(id);
  if (!user) return callback({ code: grpc.status.NOT_FOUND, message: 'Utilisateur non trouvé' });

  db.prepare('DELETE FROM users WHERE id = ?').run(id);
  callback(null, { success: true, message: 'Utilisateur supprimé avec succès' });
};

const server = new grpc.Server();
server.addService(userProto.UserService.service, {
  createUser, getUser, loginUser, updateUser, deleteUser
});

server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err) => {
  if (err) { console.error('Erreur démarrage User Service:', err); return; }
  console.log('User Service gRPC démarré sur le port 50051');
});