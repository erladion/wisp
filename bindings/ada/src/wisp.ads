--  Idiomatic Ada binding for the Wisp message broker client.
--
--  A thick wrapper over the C ABI in common/connectionapi.h: Ada strings,
--  exceptions instead of error codes, and plain Ada procedures as
--  subscription callbacks. Subprogram names mirror the C ABI's, so the
--  header documentation applies 1:1; the thin mapping lives in Wisp.C_API.
--
--  The connection is process-global, like the underlying C API:
--  Init_Connection once, use freely from any task, Shutdown_Connection at
--  exit. Payload Strings are treated as raw bytes (binary-safe, no encoding
--  assumed).

package Wisp is

   Wisp_Error : exception;
   --  Raised when the underlying library reports failure. The exception
   --  message names the operation and the C error code.

   procedure Init_Connection
     (Address              : String;            --  e.g. "tcp://127.0.0.1:5555"
      Client_Id            : String   := "";    --  "" lets the library choose
      Keepalive_Time_Ms    : Positive := 3_000;   --  heartbeat interval; keep below the broker's 10 s zombie timeout
      Keepalive_Timeout_Ms : Positive := 10_000); --  offline after this much broker silence
   --  Open the connection. Returns before it finishes coming online; use
   --  Wait_For_Connection to block for it (see also Is_Connected).

   procedure Wait_For_Connection (Timeout_Ms : Positive := 5_000);
   --  Block until the connection is up, raising Wisp_Error if the broker
   --  cannot be reached within Timeout_Ms. A timeout is not terminal - the
   --  connection keeps being retried in the background.

   procedure Shutdown_Connection;

   function Is_Connected return Boolean;
   --  True while the broker connection is up.

   procedure Send_Data (Topic : String; Data : String);
   --  Publish Data on Topic (fire and forget).

   procedure Send_Message (Topic : String; Text : String);
   --  Publish Text on Topic (fire and forget). Convenience over Send_Data
   --  for NUL-free text payloads.

   procedure Set_Cluster (Name : String);

   --  Publish on Topic, naming Reply_Topic for a responder to answer on - the
   --  non-blocking half of request/reply. Register a handler on Reply_Topic
   --  first, send, and handle the answer there. Send_Request below does the
   --  same and then blocks, which a message handler must not do: it would
   --  stall the thread delivering the reply.
   --
   --  Nothing expires here; unregister the reply topic when you stop waiting.
   --  Raises Wisp_Error if Reply_Topic is empty, over 512 bytes, or starts with
   --  "__" - a broker drops reserved keys rather than routing them, so such an
   --  answer would be lost in silence.
   procedure Send_Data_With_Reply
     (Topic : String; Data : String; Reply_Topic : String);

   --  A reply topic unique to this request, derived from Request_Topic and
   --  within the broker's 512-byte topic limit.
   function Make_Reply_Topic (Request_Topic : String) return String;
   --  Move the broker to a different discovery cluster at runtime. Name must be
   --  1-64 bytes without '|'; raises Wisp_Error if it is rejected or there is no
   --  connection. Any connected client may do this — the broker re-targets its
   --  beacons and re-meshes. No effect on a broker started without discovery.

   procedure Reply_To_Sender (Data : String);
   --  Reply to the sender of the message currently being handled; only
   --  meaningful from inside a subscription handler.

   function Send_Request
     (Topic        : String;
      Payload      : String;
      Timeout_Ms   : Positive := 5_000;
      Max_Response : Positive := 65_536) return String;
   --  Send Payload on Topic and block for the reply. Raises Wisp_Error on
   --  timeout, when offline, or if the response does not fit in
   --  Max_Response bytes (the message then names the required size).

   --  Protobuf payloads --------------------------------------------------
   --
   --  A protobuf message travels packed in a google.protobuf.Any naming its
   --  type, so a receiver can check what it has before parsing (PROTOCOL.md,
   --  "Payload frame"). Ada has no protobuf implementation of its own: encode
   --  the message with whatever codec you have - protobuf-c bound through
   --  Interfaces.C is the usual answer, and the tree already generates C
   --  bindings for its own types - and pass the bytes here with the full type
   --  name, e.g. "broker.SystemStats". The envelope is written on the C side by
   --  the same encoder the C++ client uses, so what a C++ subscriber receives
   --  is byte for byte what a C++ publisher would have sent.
   --
   --  Send_Data with unframed bytes still works and stays supported. What it
   --  gives up is the type check: a receiver cannot then tell a mismatch from a
   --  valid message, and proto3 parsing is permissive enough to hand back a
   --  plausible-looking wrong answer instead of failing.

   procedure Send_Any (Topic : String; Type_Name : String; Value : String);
   --  Publish Value on Topic, packed as Type_Name (fire and forget).

   procedure Send_Any_With_Reply
     (Topic : String; Type_Name : String; Value : String; Reply_Topic : String);
   --  The non-blocking half of request/reply, packed; the Reply_Topic rules of
   --  Send_Data_With_Reply apply unchanged.

   procedure Reply_To_Sender_Any (Type_Name : String; Value : String);
   --  Reply to the sender of the message being handled, packed; only
   --  meaningful from inside a subscription handler.

   function Send_Request_Any
     (Topic        : String;
      Type_Name    : String;
      Value        : String;
      Timeout_Ms   : Positive := 5_000;
      Max_Response : Positive := 65_536) return String;
   --  Send a packed request and block for the reply. The reply is returned as
   --  it arrived - raw, since a responder picks its own encoding - so use
   --  Any_Type_Name/Any_Value on it if you expect a packed answer. Raises
   --  Wisp_Error on the same conditions as Send_Request.

   function Any_Type_Name (Data : String) return String;
   --  The protobuf type name a payload claims, e.g. "broker.SystemStats", or
   --  "" when Data is not a packed payload at all (raw bytes, JSON, a bare
   --  serialized message). Since a broker forwards both kinds untouched, this
   --  is how a handler tells them apart.

   function Any_Value (Data : String) return String;
   --  The packed message inside Data, as a slice of Data itself - no copy.
   --  Empty both for a payload that is not packed and for one wrapping a
   --  message that serializes to nothing, so test Any_Type_Name to tell those
   --  apart.

   type Handler is access procedure (Topic : String; Data : String);
   --  Must designate a library-level procedure. Handlers run on the
   --  library's worker thread, not on any Ada task: keep them short and
   --  synchronize access to shared state. Exceptions raised inside a
   --  handler are discarded (they must not propagate into C).

   type Origin is (Local, Mesh, Any);
   for Origin use (Local => 1, Mesh => 2, Any => 3);
   --  Where a message a callback wants may come from: published by a client
   --  of the same broker, or carried in across a peer link. A bitmask in the
   --  C ABI, so Any is the two together.

   procedure Register_Callback
     (Topic    : String;
      Callback : not null Handler;
      Scope    : Origin := Any);
   --  Register Callback for Topic, triggered only by messages of the origins
   --  in Scope.
   --
   --  Registrations on one topic may differ: the broker is asked for their
   --  union and each callback is filtered on delivery, so a local-only
   --  handler stays local-only beside a wildcard subscription that wants
   --  everything. Against a broker predating scopes everything widens to
   --  Any - an unrecognized subscription is widened, never dropped.

   procedure Unregister_Callback (Topic : String; Callback : not null Handler);
   --  Remove a registration made with Register_Callback. A handler already
   --  running when this returns may still complete its current message.

   type Any_Handler is access procedure (Topic, Type_Name, Value : String);
   --  Same rules as Handler: must designate a library-level procedure, runs on
   --  the library's worker thread, exceptions raised inside are discarded.
   --  Type_Name and Value are slices of the delivered payload and must not be
   --  saved past the call - copy what you need.

   procedure Register_Any_Callback
     (Topic    : String;
      Callback : not null Any_Handler;
      Scope    : Origin := Any);
   --  Register Callback for Topic, receiving the payload already split into its
   --  protobuf type name and the packed message. Payloads on Topic that are not
   --  packed never reach it; register a plain Handler as well if the topic
   --  carries both. Scope filters by origin exactly as Register_Callback does.

   procedure Unregister_Any_Callback
     (Topic : String; Callback : not null Any_Handler);
   --  Remove a registration made with Register_Any_Callback. A handler already
   --  running when this returns may still complete its current message.

   type Log_Level is (Debug, Info, Warning, Error);

   procedure Set_Log_Level (Level : Log_Level);
   --  Discard library log output below Level. The WISP_LOG_LEVEL environment
   --  variable ("debug", "info", "warn", "error") sets the starting level;
   --  unset logs everything.

   type Log_Handler is access procedure (Level : Log_Level; Message : String);
   --  Must designate a library-level procedure. Like Handler, it runs on the
   --  library's worker threads: keep it short, synchronize access to shared
   --  state, and do not call back into Wisp from it. Exceptions raised
   --  inside are discarded.

   procedure Set_Log_Handler (Callback : Log_Handler);
   --  Route library log output into Callback instead of stdout/stderr; null
   --  restores the default output. The Set_Log_Level filter applies either
   --  way.

end Wisp;
