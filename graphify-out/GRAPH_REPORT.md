# Graph Report - java-grpc-backend  (2026-11-04)

## Corpus Check
- 11 files · ~3,953 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 4 file(s) not represented in the graph (top: (none) 2, .example 1, .proto 1)

## Summary
- 148 nodes · 285 edges · 8 communities (1 shown, 7 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 5 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `1557f7bf`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- GrpcServerApp.java
- AuthServiceImpl.java
- OtpService.java
- ChatServiceImpl
- MongoConfig.java
- iuh.fit:java-grpc-backend

## God Nodes (most connected - your core abstractions)
1. `ChatServiceImpl` - 21 edges
2. `AuthServiceImpl` - 15 edges
3. `OtpService` - 14 edges
4. `EmailSender` - 8 edges
5. `Env` - 6 edges
6. `ConsoleEmailSender` - 6 edges
7. `SmtpEmailSender` - 6 edges
8. `MongoConfig` - 5 edges
9. `GrpcServerApp` - 2 edges
10. `Authenticator` - 2 edges

## Surprising Connections (you probably didn't know these)
- `ConsoleEmailSender` --implements--> `EmailSender`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/mail/ConsoleEmailSender.java → src/main/java/iuh/fit/grpc/mail/EmailSender.java
- `SmtpEmailSender` --implements--> `EmailSender`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/mail/SmtpEmailSender.java → src/main/java/iuh/fit/grpc/mail/EmailSender.java
- `AuthServiceImpl` --references--> `OtpService`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/service/AuthServiceImpl.java → src/main/java/iuh/fit/grpc/service/OtpService.java
- `OtpService` --references--> `EmailSender`  [EXTRACTED]
  src/main/java/iuh/fit/grpc/service/OtpService.java → src/main/java/iuh/fit/grpc/mail/EmailSender.java

## Import Cycles
- None detected.

## Communities (8 total, 7 thin omitted)

### Community 0 - "GrpcServerApp.java"
Cohesion: 0.09
Nodes (5): Env, GrpcServerApp, ConsoleEmailSender, SmtpEmailSender, Authenticator

## Knowledge Gaps
- **1 isolated node(s):** `iuh.fit:java-grpc-backend`
  These have ≤1 connection - possible missing edges. (Counts symbols only; 55 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **7 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `ChatServiceImpl` connect `ChatServiceImpl` to `GrpcServerApp.java`, `AuthServiceImpl.java`, `ChatServiceImpl.java`, `MongoConfig.java`?**
  _High betweenness centrality (0.125) - this node is a cross-community bridge._
- **What connects `iuh.fit:java-grpc-backend` to the rest of the system?**
  _1 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `GrpcServerApp.java` be split into smaller, more focused modules?**
  _Cohesion score 0.08522727272727272 - nodes in this community are weakly interconnected._
- **Why does `AuthServiceImpl` connect `AuthServiceImpl.java` to `GrpcServerApp.java`, `OtpService.java`, `ChatServiceImpl`?**
  _High betweenness centrality (0.070) - this node is a cross-community bridge._
- **Should `AuthServiceImpl.java` be split into smaller, more focused modules?**
  _Cohesion score 0.10967741935483871 - nodes in this community are weakly interconnected._
- **Why does `OtpService` connect `OtpService.java` to `GrpcServerApp.java`, `AuthServiceImpl.java`, `ChatServiceImpl`?**
  _High betweenness centrality (0.070) - this node is a cross-community bridge._
- **Should `ChatServiceImpl.java` be split into smaller, more focused modules?**
  _Cohesion score 0.1 - nodes in this community are weakly interconnected._