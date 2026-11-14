# java-grpc-backend

A Java gRPC backend for a chat application. One server process listens on a single port (50051 by default) and exposes two services: `AuthService` (registration, login, email OTP, profile) and `ChatService` (messages, chat rooms, friends, realtime events). Data is stored in MongoDB. In the three-tier chat project this is the backend tier: the mobile app calls a FastAPI gateway, and the gateway calls this service over gRPC.

## Tech stack

- Java 21 (`pom.xml` compiles with source and target 21)
- gRPC 1.75.0 (`grpc-netty-shaded`, `grpc-protobuf`, `grpc-stub`) and Protocol Buffers 3.25.3
- MongoDB Java sync driver 5.11.1, on MongoDB Atlas or a local MongoDB
- Maven, with `protobuf-maven-plugin` generating Java code from `chat.proto` during the build
- `dotenv-java` for `.env` support
- `bcrypt` (at.favre.lib 0.10.2) for password hashing, `angus-mail` 2.0.3 for email, `slf4j-simple` for logging

## Project structure

```text
java-grpc-backend/
├── src/main/
│   ├── java/iuh/fit/grpc/
│   │   ├── GrpcServerApp.java        # Entry point, registers both services
│   │   ├── config/
│   │   │   ├── Env.java              # Reads configuration (environment, then .env)
│   │   │   └── MongoConfig.java      # MongoDB connection and timeouts
│   │   ├── mail/
│   │   │   ├── EmailSender.java      # Interface for sending OTP emails
│   │   │   ├── ConsoleEmailSender.java  # Prints the OTP to the log (development)
│   │   │   └── SmtpEmailSender.java     # Sends real email over SMTP
│   │   └── service/
│   │       ├── AuthServiceImpl.java  # AuthService
│   │       ├── ChatServiceImpl.java  # ChatService
│   │       └── OtpService.java       # OTP generation, storage and verification
│   └── proto/
│       └── chat.proto                # gRPC contract
├── .env.example                      # Environment variable template
├── .gitignore
└── pom.xml
```

## Getting started

You need JDK 21 or newer, Apache Maven 3.8+, and a MongoDB database (an Atlas account, or a local instance on port 27017). The first build downloads `protoc` and the gRPC plugin for your operating system, so it needs internet access.

1. Clone the repository:
   ```bash
   git clone https://github.com/NguyenTranTrongHuy2912/java-grpc-backend.git
   cd java-grpc-backend
   ```

2. Create `.env` from the template and fill it in:
   ```bash
   cp .env.example .env
   ```
   Generate `OTP_SECRET` with `openssl rand -hex 32`.

3. Compile. This step also generates the Java classes from `chat.proto`:
   ```bash
   mvn clean compile
   ```

4. Run `GrpcServerApp` from your IDE, or with Maven:
   ```bash
   mvn exec:java -Dexec.mainClass="iuh.fit.grpc.GrpcServerApp"
   ```

On success the console prints:

```text
Starting Java gRPC Server on port 50051...
Server started successfully!
```

## Configuration

Settings are read through `dotenv-java`, so a real environment variable takes precedence over the same key in `.env`. When deploying (Docker, Render and similar), pass them as environment variables and skip the `.env` file.

| Variable                     | Required              | Meaning                                                                            |
|------------------------------|-----------------------|------------------------------------------------------------------------------------|
| `MONGO_URI`                  | Yes                   | MongoDB connection string                                                          |
| `MONGO_DB_NAME`              | Yes                   | Database name, for example `chat_db`                                               |
| `OTP_SECRET`                 | Yes                   | Secret key used to hash OTP codes with HMAC-SHA256                                 |
| `MAIL_MODE`                  | No                    | `console` (default) prints the OTP to the log, `smtp` sends real email             |
| `SMTP_HOST`, `SMTP_PORT`     | With `MAIL_MODE=smtp` | SMTP server and port. The template uses `smtp.gmail.com` and `587`                 |
| `SMTP_USER`, `SMTP_PASSWORD` | With `MAIL_MODE=smtp` | Sending account. For Gmail use a 16-character App Password, not the login password |
| `SMTP_FROM`                  | No                    | Sender address. If empty, `SMTP_USER` is used                                      |
| `PORT`                       | No                    | gRPC port, default 50051                                                           |

`PORT` is the exception to the rule above: it is read only from the process environment (`System.getenv`), so putting it in `.env` has no effect.

If `MONGO_URI`, `MONGO_DB_NAME` or `OTP_SECRET` is missing or blank, the server stops during startup with a message naming the variable, instead of failing later with a driver error.

The MongoDB client is configured with a server selection timeout of 8 seconds, a connect timeout of 8 seconds and a read timeout of 12 seconds. These stay below the gateway's 15 second gRPC timeout (`GRPC_TIMEOUT`), so a database problem comes back to the gateway as an error instead of a silent timeout. Idle connections are dropped after 60 seconds, and retryable reads and writes are on.

## AuthService

Accounts are identified by email. Emails are trimmed and lowercased before they are stored or compared, and can be at most 254 characters.

| Method                   | Description                                                                                                                                                             |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `RequestOtp`             | Sends a 6-digit OTP to an email. `purpose` is `OTP_PURPOSE_REGISTER` (default) or `OTP_PURPOSE_RESET_PASSWORD`. Returns `expires_in_seconds` and `resend_after_seconds` |
| `Register`               | Creates an account after verifying the OTP. Returns `user_id` and `display_name`                                                                                        |
| `Login`                  | Logs in with email and password. Returns `user_id` and `display_name`                                                                                                   |
| `ResetPassword`          | Sets a new password using an OTP issued for `reset_password`                                                                                                            |
| `UpdateProfile`          | Changes `display_name` and `avatar_url`. A blank field is left unchanged. If both are blank the call returns `INVALID_ARGUMENT`                                         |
| `ChangePasswordLoggedIn` | Changes the password of a logged-in user, who must supply the current password                                                                                          |

Rules worth knowing:

- Display names are 1 to 50 characters. Passwords are 6 to 64 characters on registration and reset. When a logged-in user changes their password, the new one only needs to be at least 6 characters.
- Passwords are hashed with bcrypt at cost 12. A wrong email and a wrong password both return `UNAUTHENTICATED` with the same message, and the server still checks a dummy hash for unknown emails so response time does not reveal which emails exist.
- An account with `is_active = false` cannot log in and gets `PERMISSION_DENIED`.
- Registering an email that already exists returns `ALREADY_EXISTS`.
- `RequestOtp` for password reset always succeeds, even for an unregistered email, and only sends a mail if the account exists. This keeps outsiders from probing which emails have accounts.
- Every user has a `role`, either `USER` or `ADMIN`.

### OTP

- Codes have 6 digits, live for 300 seconds, and work once.
- A new code can be requested at most once every 60 seconds, otherwise the call returns `RESOURCE_EXHAUSTED`.
- At most 5 attempts are allowed. After that the code is deleted and a new one must be requested.
- Each (purpose, email) pair has its own record, so a registration code cannot be used to reset a password.
- Only an HMAC-SHA256 hash of the code is stored. Records expire on their own through a TTL index on `expires_at`.
- If sending the email fails, the record is deleted so the user can retry immediately, and the client receives `UNAVAILABLE`.

## ChatService

Every call carries a `user_id` in the request. The service does not validate tokens itself, the gateway does that before calling in. `chat.proto` also notes that Java is the only place that writes room data.

### Messages

| Method              | Description                                                |
|---------------------|------------------------------------------------------------|
| `SendMessage`       | Stores a message and pushes an event to the room's members |
| `GetMessageHistory` | Returns a room's history, newest first                     |

- `SendMessage` requires `message_id`, `room_id` and `sender_id`, and the sender must belong to the room (`PERMISSION_DENIED` otherwise, `NOT_FOUND` if the room does not exist). A `TEXT` message needs `content`. `IMAGE` and `VIDEO` messages need a `media_url` that starts with `https://` and is at most 500 characters.
- Resending the same `message_id` still succeeds and returns the timestamp saved the first time. It does not create a second message and does not fire another event.
- `GetMessageHistory` requires `room_id` and `user_id` (who must be a member). `limit` defaults to 50 and is capped at 100. Use `before_timestamp` to page backwards, and `has_more` tells you whether older messages remain.
- Old messages without a `type` field are treated as `TEXT`.

### Rooms

| Method           | Description                                                                     |
|------------------|---------------------------------------------------------------------------------|
| `CreateRoom`     | Creates a one-to-one or group room                                              |
| `ListRooms`      | Lists a user's rooms, most recently active first, up to 200                     |
| `MarkRoomRead`   | Marks a room as read. `read_up_to_timestamp = 0` means up to the latest message |
| `LeaveRoom`      | Leaves a group room                                                             |
| `AddRoomMembers` | Adds people to a group                                                          |
| `UpdateRoomName` | Renames a group                                                                 |
| `SearchUsers`    | Searches users                                                                  |

- A direct room needs exactly one other member. If the two users already have a room, `CreateRoom` returns the existing one instead of creating another. The `direct_key` is the two user ids sorted and joined with a colon, backed by a unique index.
- A group room needs a name of 1 to 50 characters and at least 3 members including the creator, with a maximum of 100. Unknown users return `NOT_FOUND`.
- Any member of a group can add people and rename it. People already in the group are skipped, and `added_count` is the number of people actually added.
- `LeaveRoom` only applies to groups. Calling it again after leaving still succeeds. If the leaving user is the owner (`created_by`), ownership passes to the first remaining member, skipping the `hq_ai_bot` bot.
- Leaving, adding members and renaming each post a system message with `sender_id = "system"` and fire `EVENT_ROOM_UPDATED`.
- `unread_count` on `Room` is computed for the viewer: messages newer than their read marker that they did not send themselves.
- The last-message preview is "Đã gửi một ảnh" for images and "[Video]" for videos.
- `SearchUsers` needs a query of 2 to 100 characters. A query containing `@` searches by email prefix, anything else searches by name. `limit` defaults to 10 and is capped at 20, and the requester is excluded from the results.

### Friends

| Method                 | Description                                                                                                                   |
|------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `SendFriendRequest`    | Sends a request. You cannot add yourself. Returns `ALREADY_EXISTS` if the two are already friends or a pending request exists |
| `RespondFriendRequest` | The receiver accepts or rejects. Accepting creates a two-way friendship                                                       |
| `ListFriends`          | Lists a user's friends                                                                                                        |
| `ListFriendRequests`   | Lists pending requests sent to the user, newest first                                                                         |

### Realtime events

`SubscribeEvents(StreamEventsRequest)` is a server-streaming call. A client opens one stream per `user_id` and receives `ChatEvent` messages until it cancels. A user can hold several streams at once (several devices), and each event goes to all of them.

| Event                   | When it is sent                                                                                                                     |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `EVENT_NEW_MESSAGE`     | A new message in a room, sent to every member                                                                                       |
| `EVENT_ROOM_UPDATED`    | A room changed (member left, members added, renamed). A user who just left also gets one so the app can drop the room from its list |
| `EVENT_FRIEND_REQUEST`  | Sent to the receiver of a friend request                                                                                            |
| `EVENT_FRIEND_ACCEPTED` | Sent to the original sender when the request is accepted                                                                            |

`EVENT_READ_RECEIPT` is defined in `chat.proto`, but the current code never emits it.

If writing to a stream fails, the server removes it and ends it with `UNAVAILABLE`, so the gateway knows to have the app reconnect. The list of open streams lives in the memory of one process. If you run several server instances, a client only receives events handled by the instance it is connected to.

## MongoDB data

| Collection        | Contents                                                                                                                         | Indexes                                                       |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| `users`           | Accounts: `account` (email), `display_name`, `password_hash`, `avatar_url`, `role`, `is_active`, `created_at`                    | unique on `account`                                           |
| `otp_codes`       | OTPs: `_id` as `<purpose>:<email>`, `code_hash`, `attempts`, `last_sent_at`, `expires_at`                                        | TTL on `expires_at`                                           |
| `messages`        | Messages: `room_id`, `sender_id`, `content`, `timestamp`, `type`, `media_*` fields                                               | `room_id` ascending, `timestamp` descending                   |
| `rooms`           | Rooms: `type` (`DIRECT` or `GROUP`), `name`, `member_ids`, `created_by`, `last_message`, `last_read`, `updated_at`, `direct_key` | `member_ids` with `updated_at`, unique sparse on `direct_key` |
| `friend_requests` | Requests: `sender_id`, `receiver_id`, `status` (`PENDING`, `ACCEPTED`, `REJECTED`), `created_at`                                 | `receiver_id` with `status`                                   |
| `friendships`     | Each friendship is stored twice, once per direction                                                                              | unique on `user_id` with `friend_id`                          |

The `ChatService` indexes are created on a background thread at startup. If that step fails the server keeps running, logs the error, and does not retry.

## gRPC status codes

| Code                 | Situations                                                      |
|----------------------|-----------------------------------------------------------------|
| `INVALID_ARGUMENT`   | Missing or invalid input, wrong or expired OTP                  |
| `UNAUTHENTICATED`    | Wrong email or password, wrong current password                 |
| `PERMISSION_DENIED`  | Not a member of the room, account disabled                      |
| `NOT_FOUND`          | Room, user or friend request does not exist                     |
| `ALREADY_EXISTS`     | Email already registered, already friends, request already sent |
| `RESOURCE_EXHAUSTED` | OTP requested again too soon                                    |
| `UNAVAILABLE`        | Email could not be sent, realtime stream broke                  |
| `INTERNAL`           | Database error or an unexpected failure                         |

## Changing the gRPC contract

Edit `src/main/proto/chat.proto` and run `mvn clean compile` to regenerate the code. The generated Java package is `iuh.fit.grpc.chat`. The gateway and the mobile app need the matching updated `.proto` file.