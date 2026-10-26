# Java gRPC Core Backend Service

A high-performance, distributed backend service built with **Java 21/25**, **gRPC**, and **MongoDB Atlas** for handling real-time chat operations and message storage.

---

## 🛠️ Tech Stack & Architecture

- **Language:** Java 21 / Java 25 (LTS)
- **Protocol:** gRPC (HTTP/2) + Protocol Buffers v3
- **Database:** MongoDB Atlas (Cloud Document Database)
- **Build Tool:** Apache Maven
- **Configuration:** `dotenv-java` for environment variable security

---

## 📁 Project Structure

```text
java-grpc-backend/
├── src/
│   └── main/
│       ├── java/iuh/fit/grpc/
│       │   ├── ChatServiceImpl.java  # Core gRPC Service implementation
│       │   ├── GrpcServerApp.java    # Main entry point for gRPC Server
│       │   └── MongoConfig.java      # MongoDB connection management
│       └── proto/
│           └── chat.proto            # Protocol Buffers contract
├── .env.example                      # Environment variables template
├── .gitignore                        # Git ignore rules
└── pom.xml                           # Maven dependencies & build plugins
```

---

## 🚀 Getting Started

### Prerequisites

- **JDK 21** or **JDK 25**
- **Apache Maven 3.8+**
- MongoDB Atlas account (or local MongoDB running on port `27017`)

### Installation & Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/NguyenTranTrongHuy2912/java-grpc-backend.git
   cd java-grpc-backend
   ```

2. **Configure Environment Variables:**
   Copy `.env.example` to `.env` and fill in your MongoDB connection details:
   ```bash
   cp .env.example .env
   ```
   *Edit `.env`:*
   ```env
   MONGO_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority
   MONGO_DB_NAME=chat_db
   ```

3. **Compile Protocol Buffers & Build Project:**
   ```bash
   mvn clean compile
   ```

4. **Run the gRPC Server:**
   Execute `GrpcServerApp.java` from your IDE or run via Maven:
   ```bash
   mvn exec:java -Dexec.mainClass="iuh.fit.grpc.GrpcServerApp"
   ```

   *Server will start listening on port `50051`:*
   ```text
   Starting Java gRPC Server on port 50051...
   Server started successfully!
   ```

---

## 📡 gRPC Interface Specification

- **Service:** `chat.ChatService` (port `50051`)

| Method                                                        | Description                                                                                                                                                                                                                        |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SendMessage(ChatMessage) returns (ChatResponse)`             | Stores a message. Resending the same `message_id` still succeeds (idempotent). Returns `INVALID_ARGUMENT` if `message_id`, `room_id`, `sender_id`, or `content` is missing.                                                        |
| `GetMessageHistory(HistoryRequest) returns (HistoryResponse)` | Retrieves messages for a `room_id`, newest first. Use `limit` (default 50, max 100) and `before_timestamp` for pagination; `has_more` indicates whether older messages remain. Returns `INVALID_ARGUMENT` if `room_id` is missing. |

---

## ⚙️ Configuration

Copy `.env.example` to `.env`, or pass `MONGO_URI` and `MONGO_DB_NAME` as environment variables when deploying (Docker, cloud).

If either variable is missing, the server fails fast at startup with a clear error message instead of an obscure driver error.