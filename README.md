# Hotel Booking Microservices

Application de réservation d'hôtel basée sur une architecture microservices.

## Architecture

- **API Gateway** (port 3000) — REST + GraphQL → gRPC
- **User Service** (port 50051) — gRPC + SQLite3
- **Hotel Service** (port 50052) — gRPC + SQLite3 + Kafka consumer
- **Reservation Service** (port 50053) — gRPC + RxDB + Kafka producer
- **Kafka Broker** (port 9092) — Docker apache/kafka:4.2.0

## Technologies

- Node.js, gRPC, Protobuf
- REST (Express.js), GraphQL (Apollo Server)
- Apache Kafka 4.2.0 (KRaft mode, Docker)
- SQLite3 (better-sqlite3), RxDB

## Prérequis

- Node.js v18+
- Docker Desktop

## Installation

### 1. Cloner le repo

```bash
git clone https://github.com/manelhattab/hotel-booking-SOA.git
cd hotel-booking-SOA
```

### 2. Installer les dépendances

```bash
cd api-gateway && npm install && cd ..
cd user-service && npm install && cd ..
cd hotel-service && npm install && cd ..
cd reservation-service && npm install && cd ..
```

### 3. Démarrer Kafka

```bash
docker-compose up -d
```

### 4. Démarrer les services (4 terminaux séparés)

```bash
cd user-service && node src/index.js
cd hotel-service && node src/index.js
cd reservation-service && node src/index.js
cd api-gateway && node src/index.js
```

## Endpoints REST

### Users

| Méthode | Endpoint | Description |
|---|---|---|
| POST | /api/users | Créer un utilisateur |
| GET | /api/users/:id | Récupérer un utilisateur |
| PUT | /api/users/:id | Modifier un utilisateur |
| DELETE | /api/users/:id | Supprimer un utilisateur |
| POST | /api/users/login | Connexion |

### Hotels

| Méthode | Endpoint | Description |
|---|---|---|
| POST | /api/hotels | Créer un hôtel |
| GET | /api/hotels | Lister les hôtels |
| GET | /api/hotels?location=Tunis | Rechercher par ville |
| GET | /api/hotels/:id | Récupérer un hôtel |
| PUT | /api/hotels/:id | Modifier un hôtel |
| DELETE | /api/hotels/:id | Supprimer un hôtel |

### Reservations

| Méthode | Endpoint | Description |
|---|---|---|
| POST | /api/reservations | Créer une réservation |
| GET | /api/reservations/:id | Récupérer une réservation |
| GET | /api/reservations/user/:userId | Réservations par utilisateur |
| DELETE | /api/reservations/:id | Annuler une réservation |

## GraphQL

Endpoint : `POST http://localhost:3000/graphql`

### Queries

```graphql
{ listHotels { id name location price available_rooms } }
{ getUser(id: "...") { id name email } }
{ getHotel(id: "...") { id name location price available_rooms } }
{ getReservation(id: "...") { id user_id hotel_id check_in check_out status } }
{ listReservationsByUser(user_id: "...") { id hotel_id check_in check_out status } }
```

### Mutations

```graphql
mutation { createUser(name: "Manel Httab", email: "manel.hattab@gmail.com", password: "manel123456") { id name email } }
mutation { createHotel(name: "Hotel Manel", location: "Sousse", price: 120, rooms: 8) { id name } }
mutation { createReservation(user_id: "...", hotel_id: "...", check_in: "2026-06-01", check_out: "2026-06-05") { id status } }
mutation { cancelReservation(id: "...") { success message } }
mutation { updateUser(id: "...", name: "New Name") { id name email } }
mutation { updateHotel(id: "...", price: 200) { id name price } }
mutation { deleteUser(id: "...") { success message } }
mutation { deleteHotel(id: "...") { success message } }
```

## Kafka

| Topic | Producteur | Consommateur | Déclencheur |
|---|---|---|---|
| reservation.created | Reservation Service | Hotel Service | Création d'une réservation |

**Contenu du message :**

```json
{
  "id": "uuid",
  "user_id": "uuid",
  "hotel_id": "uuid",
  "check_in": "2026-06-01",
  "check_out": "2026-06-05",
  "status": "confirmed"
}
```

**Scénario métier :** Quand un client fait une réservation, le Reservation Service publie un événement sur le topic `reservation.created`. Le Hotel Service consomme cet événement et réduit automatiquement le nombre de chambres disponibles.

## Bases de données

| Service | Type | Technologie |
|---|---|---|
| User Service | SQL | SQLite3 (users.db) |
| Hotel Service | SQL | SQLite3 (hotels.db) |
| Reservation Service | NoSQL | RxDB (in-memory) |

## Fichiers .proto

Les contrats gRPC sont définis dans le dossier `/proto` :

- `user.proto` — UserService (CreateUser, GetUser, UpdateUser, DeleteUser, LoginUser)
- `hotel.proto` — HotelService (CreateHotel, GetHotel, ListHotels, UpdateHotel, DeleteHotel)
- `reservation.proto` — ReservationService (CreateReservation, GetReservation, ListByUser, CancelReservation)
