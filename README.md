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

- **Service:** `chat.ChatService`
- **Method:** `SendMessage(ChatMessage) returns (ChatResponse)`
- **Port:** `50051`