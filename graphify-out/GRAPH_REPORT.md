# Graph Report - java-grpc-backend  (2026-11-08)

## Corpus Check
- 11 files · ~5,302 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 4 file(s) not represented in the graph (top: (none) 2, .example 1, .proto 1)

## Summary
- 181 nodes · 362 edges · 8 communities (3 shown, 5 thin omitted)
- Extraction: 99% EXTRACTED · 1% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `70556f0f`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- GrpcServerApp.java
- AuthServiceImpl.java
- OtpService.java
- ChatServiceImpl
- MongoConfig.java
- Java gRPC Core Backend Service
- iuh.fit:java-grpc-backend

## God Nodes (most connected - your core abstractions)
1. `ChatServiceImpl` - 29 edges
2. `AuthServiceImpl` - 17 edges
3. `OtpService` - 14 edges
4. `Env` - 8 edges
5. `MongoConfig` - 8 edges
6. `EmailSender` - 8 edges
7. `ConsoleEmailSender` - 6 edges
8. `SmtpEmailSender` - 6 edges
9. `Java gRPC Core Backend Service` - 6 edges
10. `🚀 Getting Started` - 3 edges

## Surprising Connections (you probably didn't know these)
- `SmtpEmailSender` --implements--> `EmailSender`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/mail/SmtpEmailSender.java → src/main/java/iuh/fit/grpc/mail/EmailSender.java
- `AuthServiceImpl` --references--> `OtpService`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/service/AuthServiceImpl.java → src/main/java/iuh/fit/grpc/service/OtpService.java
- `ConsoleEmailSender` --implements--> `EmailSender`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/mail/ConsoleEmailSender.java → src/main/java/iuh/fit/grpc/mail/EmailSender.java
- `OtpService` --references--> `EmailSender`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/service/OtpService.java → src/main/java/iuh/fit/grpc/mail/EmailSender.java

## Import Cycles
- None detected.

## Communities (8 total, 5 thin omitted)

### Community 0 - "GrpcServerApp.java"
Cohesion: 0.11
Nodes (4): Env, GrpcServerApp, SmtpEmailSender, Authenticator

### Community 3 - "OtpService.java"
Cohesion: 0.08
Nodes (3): ConsoleEmailSender, EmailSender, OtpService

### Community 6 - "Java gRPC Core Backend Service"
Cohesion: 0.22
Nodes (8): ⚙️ Configuration, 🚀 Getting Started, 📡 gRPC Interface Specification, Installation & Setup, Java gRPC Core Backend Service, Prerequisites, 📁 Project Structure, 🛠️ Tech Stack & Architecture

## Knowledge Gaps
- **1 isolated node(s):** `iuh.fit:java-grpc-backend`
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 60 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **5 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ChatServiceImpl` connect `ChatServiceImpl` to `GrpcServerApp.java`, `ChatServiceImpl.java`, `OtpService.java`, `MongoConfig.java`?**
  _High betweenness centrality (0.122) - this node is a cross-community bridge._
- **What connects `iuh.fit:java-grpc-backend` to the rest of the system?**
  _1 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `GrpcServerApp.java` be split into smaller, more focused modules?**
  _Cohesion score 0.10541310541310542 - nodes in this community are weakly interconnected._
- **Why does `AuthServiceImpl` connect `AuthServiceImpl.java` to `GrpcServerApp.java`, `OtpService.java`, `ChatServiceImpl`?**
  _High betweenness centrality (0.058) - this node is a cross-community bridge._
- **Should `AuthServiceImpl.java` be split into smaller, more focused modules?**
  _Cohesion score 0.09309309309309309 - nodes in this community are weakly interconnected._
- **Why does `OtpService` connect `OtpService.java` to `GrpcServerApp.java`, `AuthServiceImpl.java`, `ChatServiceImpl`?**
  _High betweenness centrality (0.051) - this node is a cross-community bridge._
- **Should `ChatServiceImpl.java` be split into smaller, more focused modules?**
  _Cohesion score 0.05555555555555555 - nodes in this community are weakly interconnected._