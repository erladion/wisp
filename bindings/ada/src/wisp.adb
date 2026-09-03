with Ada.Unchecked_Conversion;
with Ada.Unchecked_Deallocation;

with Interfaces.C;         use Interfaces.C;
with Interfaces.C.Strings; use Interfaces.C.Strings;

with System;
with System.Storage_Elements;

with Wisp.C_API;

package body Wisp is

   function Error_Name (Code : int) return String is
     (case Code is
         when C_API.ERROR_GENERIC          => "generic failure",
         when C_API.ERROR_NO_CONNECTION    => "no connection",
         when C_API.ERROR_INVALID_ARGS     => "invalid arguments",
         when C_API.ERROR_SEND_FAILED      => "send failed",
         when C_API.ERROR_BUFFER_TOO_SMALL => "buffer too small",
         when C_API.ERROR_TIMEOUT          => "timeout",
         when others                       => "error" & int'Image (Code));

   procedure Check (Code : int; Operation : String) is
   begin
      if Code /= C_API.SUCCESS then
         declare
            Detail : constant String := Value (C_API.Last_Error_Message);
         begin
            raise Wisp_Error with Operation & " failed: "
              & (if Detail = "" then Error_Name (Code) else Detail);
         end;
      end if;
   end Check;

   ---------------------
   -- Init_Connection --
   ---------------------

   procedure Init_Connection
     (Address              : String;
      Client_Id            : String   := "";
      Keepalive_Time_Ms    : Positive := 3_000;
      Keepalive_Timeout_Ms : Positive := 10_000)
   is
      C_Address : chars_ptr := New_String (Address);
      C_Client  : chars_ptr :=
        (if Client_Id = "" then Null_Ptr else New_String (Client_Id));
      Config    : aliased constant C_API.Connection_Config :=
        (Address              => C_Address,
         Client_Id            => C_Client,
         Protocol             => C_API.PROTOCOL_ZMQ,
         Keepalive_Time_Ms    => int (Keepalive_Time_Ms),
         Keepalive_Timeout_Ms => int (Keepalive_Timeout_Ms));
      Code : constant int := C_API.Init_Connection (Config'Access);
   begin
      Free (C_Address);
      Free (C_Client);
      Check (Code, "Init_Connection");
   end Init_Connection;

   -------------------------
   -- Wait_For_Connection --
   -------------------------

   procedure Wait_For_Connection (Timeout_Ms : Positive := 5_000) is
   begin
      Check (C_API.Wait_For_Connection (int (Timeout_Ms)),
             "Wait_For_Connection");
   end Wait_For_Connection;

   -------------------------
   -- Shutdown_Connection --
   -------------------------

   procedure Shutdown_Connection is
   begin
      C_API.Shutdown_Connection;
   end Shutdown_Connection;

   ------------------
   -- Is_Connected --
   ------------------

   function Is_Connected return Boolean is
   begin
      return C_API.Is_Connected /= 0;
   end Is_Connected;

   ---------------
   -- Send_Data --
   ---------------

   procedure Send_Data (Topic : String; Data : String) is
      C_Topic : chars_ptr := New_String (Topic);
      Code    : constant int :=
        C_API.Send_Data (C_Topic, Data'Address, Data'Length);
   begin
      Free (C_Topic);
      Check (Code, "Send_Data");
   end Send_Data;

   ------------------
   -- Send_Message --
   ------------------

   procedure Send_Message (Topic : String; Text : String) is
      C_Topic : chars_ptr := New_String (Topic);
      C_Text  : chars_ptr := New_String (Text);
      Code    : constant int := C_API.Send_Message (C_Topic, C_Text);
   begin
      Free (C_Topic);
      Free (C_Text);
      Check (Code, "Send_Message");
   end Send_Message;

   -----------------
   -- Set_Cluster --
   -----------------

   procedure Set_Cluster (Name : String) is
      C_Name : chars_ptr   := New_String (Name);
      Code   : constant int := C_API.Set_Cluster (C_Name);
   begin
      Free (C_Name);
      Check (Code, "Set_Cluster");
   end Set_Cluster;

   ---------------------
   -- Reply_To_Sender --
   ---------------------

   procedure Reply_To_Sender (Data : String) is
      Code : constant int :=
        C_API.Reply_To_Sender (Data'Address, Data'Length);
   begin
      Check (Code, "Reply_To_Sender");
   end Reply_To_Sender;

   ------------------
   -- Send_Request --
   ------------------

   procedure Send_Data_With_Reply
     (Topic : String; Data : String; Reply_Topic : String)
   is
      C_Topic : chars_ptr    := New_String (Topic);
      C_Reply : chars_ptr    := New_String (Reply_Topic);
      Code    : constant int :=
        C_API.Send_Data_With_Reply
          (Topic       => C_Topic,
           Data        => Data'Address,
           Len         => Data'Length,
           Reply_Topic => C_Reply);
   begin
      Free (C_Topic);
      Free (C_Reply);
      Check (Code, "Send_Data_With_Reply");
   end Send_Data_With_Reply;

   function Make_Reply_Topic (Request_Topic : String) return String is
      --  Past the broker's 512-byte topic limit plus the uuid the C ABI
      --  appends, so the call never has to be repeated for capacity.
      Capacity : constant := 1_024;

      C_Topic : chars_ptr    := New_String (Request_Topic);
      Buffer  : String (1 .. Capacity) := (others => ASCII.NUL);
      Out_Len : aliased int  := 0;
      Code    : constant int :=
        C_API.Make_Reply_Topic
          (Request_Topic  => C_Topic,
           Out_Buffer     => Buffer'Address,
           Out_Buffer_Cap => int (Capacity),
           Out_Len        => Out_Len'Access);
   begin
      Free (C_Topic);
      Check (Code, "Make_Reply_Topic");
      --  Out_Len counts the terminating NUL, which an Ada string does not want.
      return Buffer (1 .. Natural (Out_Len) - 1);
   end Make_Reply_Topic;

   function Send_Request
     (Topic        : String;
      Payload      : String;
      Timeout_Ms   : Positive := 5_000;
      Max_Response : Positive := 65_536) return String
   is
      type String_Ptr is access String;
      procedure Free_Buffer is
        new Ada.Unchecked_Deallocation (String, String_Ptr);

      C_Topic : chars_ptr   := New_String (Topic);
      Buffer  : String_Ptr  := new String (1 .. Max_Response);
      Out_Len : aliased int := 0;
      Code    : constant int :=
        C_API.Send_Request
          (Topic          => C_Topic,
           Payload        => Payload'Address,
           Payload_Len    => Payload'Length,
           Out_Buffer     => Buffer.all'Address,
           Out_Buffer_Cap => int (Max_Response),
           Out_Len        => Out_Len'Access,
           Timeout_Ms     => int (Timeout_Ms));
   begin
      Free (C_Topic);

      if Code /= C_API.SUCCESS then
         Free_Buffer (Buffer);
         Check (Code, "Send_Request");
      end if;

      declare
         Response : constant String := Buffer (1 .. Natural (Out_Len));
      begin
         Free_Buffer (Buffer);
         return Response;
      end;
   end Send_Request;

   ----------------------
   -- Protobuf payloads --
   ----------------------

   procedure Send_Any (Topic : String; Type_Name : String; Value : String) is
      C_Topic : chars_ptr := New_String (Topic);
      C_Type  : chars_ptr := New_String (Type_Name);
      Code    : constant int :=
        C_API.Send_Any (C_Topic, C_Type, Value'Address, Value'Length);
   begin
      Free (C_Topic);
      Free (C_Type);
      Check (Code, "Send_Any");
   end Send_Any;

   procedure Send_Any_With_Reply
     (Topic : String; Type_Name : String; Value : String; Reply_Topic : String)
   is
      C_Topic : chars_ptr := New_String (Topic);
      C_Type  : chars_ptr := New_String (Type_Name);
      C_Reply : chars_ptr := New_String (Reply_Topic);
      Code    : constant int :=
        C_API.Send_Any_With_Reply
          (Topic       => C_Topic,
           Type_Name   => C_Type,
           Value       => Value'Address,
           Len         => Value'Length,
           Reply_Topic => C_Reply);
   begin
      Free (C_Topic);
      Free (C_Type);
      Free (C_Reply);
      Check (Code, "Send_Any_With_Reply");
   end Send_Any_With_Reply;

   procedure Reply_To_Sender_Any (Type_Name : String; Value : String) is
      C_Type : chars_ptr := New_String (Type_Name);
      Code   : constant int :=
        C_API.Reply_To_Sender_Any (C_Type, Value'Address, Value'Length);
   begin
      Free (C_Type);
      Check (Code, "Reply_To_Sender_Any");
   end Reply_To_Sender_Any;

   function Send_Request_Any
     (Topic        : String;
      Type_Name    : String;
      Value        : String;
      Timeout_Ms   : Positive := 5_000;
      Max_Response : Positive := 65_536) return String
   is
      type String_Ptr is access String;
      procedure Free_Buffer is
        new Ada.Unchecked_Deallocation (String, String_Ptr);

      C_Topic : chars_ptr   := New_String (Topic);
      C_Type  : chars_ptr   := New_String (Type_Name);
      Buffer  : String_Ptr  := new String (1 .. Max_Response);
      Out_Len : aliased int := 0;
      Code    : constant int :=
        C_API.Send_Request_Any
          (Topic          => C_Topic,
           Type_Name      => C_Type,
           Value          => Value'Address,
           Len            => Value'Length,
           Out_Buffer     => Buffer.all'Address,
           Out_Buffer_Cap => int (Max_Response),
           Out_Len        => Out_Len'Access,
           Timeout_Ms     => int (Timeout_Ms));
   begin
      Free (C_Topic);
      Free (C_Type);

      if Code /= C_API.SUCCESS then
         Free_Buffer (Buffer);
         Check (Code, "Send_Request_Any");
      end if;

      declare
         Response : constant String := Buffer (1 .. Natural (Out_Len));
      begin
         Free_Buffer (Buffer);
         return Response;
      end;
   end Send_Request_Any;

   --  Locates the two Any fields inside Data. The C side returns addresses
   --  into Data itself, so both are turned back into index ranges and the
   --  callers below return slices of Data - no copy anywhere. On a payload
   --  that is not packed, Found is False and the ranges are empty.
   procedure Split_Any
     (Data        : String;
      Found       : out Boolean;
      Type_First  : out Positive;
      Type_Last   : out Natural;
      Value_First : out Positive;
      Value_Last  : out Natural)
   is
      use type System.Storage_Elements.Storage_Offset;

      Name_At   : aliased System.Address := System.Null_Address;
      Value_At  : aliased System.Address := System.Null_Address;
      Name_Len  : aliased int            := 0;
      Value_Len : aliased int            := 0;
      Code      : constant int :=
        C_API.Read_Any
          (Payload       => Data'Address,
           Len           => Data'Length,
           Out_Type_Name => Name_At'Access,
           Out_Type_Len  => Name_Len'Access,
           Out_Value     => Value_At'Access,
           Out_Value_Len => Value_Len'Access);
   begin
      Found       := Code = C_API.SUCCESS;
      Type_First  := Data'First;
      Type_Last   := Data'First - 1;
      Value_First := Data'First;
      Value_Last  := Data'First - 1;

      if not Found then
         return;
      end if;

      Type_First := Data'First + Natural (Name_At - Data'Address);
      Type_Last  := Type_First + Natural (Name_Len) - 1;
      Value_First := Data'First + Natural (Value_At - Data'Address);
      Value_Last  := Value_First + Natural (Value_Len) - 1;
   end Split_Any;

   function Any_Type_Name (Data : String) return String is
      Found                   : Boolean;
      Type_First, Value_First : Positive;
      Type_Last, Value_Last   : Natural;
   begin
      Split_Any (Data, Found, Type_First, Type_Last, Value_First, Value_Last);
      return (if Found then Data (Type_First .. Type_Last) else "");
   end Any_Type_Name;

   function Any_Value (Data : String) return String is
      Found                   : Boolean;
      Type_First, Value_First : Positive;
      Type_Last, Value_Last   : Natural;
   begin
      Split_Any (Data, Found, Type_First, Type_Last, Value_First, Value_Last);
      return (if Found then Data (Value_First .. Value_Last) else "");
   end Any_Value;

   --------------------------------------------------
   -- Register_Callback and Unregister_Callback --
   --------------------------------------------------

   --  A Handler value is a library-level code pointer, so it can serve
   --  directly as the C-side User_Data: Dispatch converts it back to call
   --  it, and it doubles as the registration identity for
   --  Unregister_Callback. No allocation, nothing to free.

   function To_Handler is new Ada.Unchecked_Conversion (System.Address, Handler);
   function To_Address is new Ada.Unchecked_Conversion (Handler, System.Address);

   procedure Dispatch
     (Topic     : chars_ptr;
      Data      : System.Address;
      Len       : int;
      User_Data : System.Address)
     with Convention => C;
   --  Trampoline the C library invokes on its worker thread.

   procedure Dispatch
     (Topic     : chars_ptr;
      Data      : System.Address;
      Len       : int;
      User_Data : System.Address)
   is
      Payload : String (1 .. Natural (Len))
        with Import, Address => Data;
   begin
      To_Handler (User_Data) (Value (Topic), Payload);
   exception
      when others =>
         null;  --  exceptions must not cross the C boundary
   end Dispatch;

   procedure Register_Callback
     (Topic    : String;
      Callback : not null Handler;
      Scope    : Origin := Any)
   is
      C_Topic : chars_ptr := New_String (Topic);
   begin
      C_API.Register_Callback_Scoped
        (C_Topic, Dispatch'Access, To_Address (Callback), int (Origin'Enum_Rep (Scope)));
      Free (C_Topic);
   end Register_Callback;

   procedure Unregister_Callback (Topic : String; Callback : not null Handler) is
      C_Topic : chars_ptr := New_String (Topic);
   begin
      C_API.Unregister_Callback (C_Topic, To_Address (Callback));
      Free (C_Topic);
   end Unregister_Callback;

   --  Same scheme again for Any_Handler, with the envelope split before the
   --  handler is called. A distinct access type means its registrations cannot
   --  be confused with a plain Handler's, here or in the C ABI's User_Data.

   function To_Any_Handler is
     new Ada.Unchecked_Conversion (System.Address, Any_Handler);
   function To_Any_Address is
     new Ada.Unchecked_Conversion (Any_Handler, System.Address);

   procedure Dispatch_Any
     (Topic     : chars_ptr;
      Data      : System.Address;
      Len       : int;
      User_Data : System.Address)
     with Convention => C;
   --  Trampoline the C library invokes on its worker thread.

   procedure Dispatch_Any
     (Topic     : chars_ptr;
      Data      : System.Address;
      Len       : int;
      User_Data : System.Address)
   is
      Payload : String (1 .. Natural (Len))
        with Import, Address => Data;

      Found                   : Boolean;
      Type_First, Value_First : Positive;
      Type_Last, Value_Last   : Natural;
   begin
      Split_Any (Payload, Found, Type_First, Type_Last, Value_First, Value_Last);
      if not Found then
         return;  --  a raw payload is not this handler's business
      end if;

      To_Any_Handler (User_Data)
        (Value (Topic),
         Payload (Type_First .. Type_Last),
         Payload (Value_First .. Value_Last));
   exception
      when others =>
         null;  --  exceptions must not cross the C boundary
   end Dispatch_Any;

   procedure Register_Any_Callback
     (Topic    : String;
      Callback : not null Any_Handler;
      Scope    : Origin := Any)
   is
      C_Topic : chars_ptr := New_String (Topic);
   begin
      C_API.Register_Callback_Scoped
        (C_Topic, Dispatch_Any'Access, To_Any_Address (Callback),
         int (Origin'Enum_Rep (Scope)));
      Free (C_Topic);
   end Register_Any_Callback;

   procedure Unregister_Any_Callback
     (Topic : String; Callback : not null Any_Handler)
   is
      C_Topic : chars_ptr := New_String (Topic);
   begin
      C_API.Unregister_Callback (C_Topic, To_Any_Address (Callback));
      Free (C_Topic);
   end Unregister_Any_Callback;

   -------------
   -- Logging --
   -------------

   --  Same scheme as Subscribe: a Log_Handler is a library-level code
   --  pointer, carried through the C side as User_Data and converted back
   --  in the trampoline. No allocation, nothing to free.

   function To_Log_Handler is
     new Ada.Unchecked_Conversion (System.Address, Log_Handler);
   function To_Log_Address is
     new Ada.Unchecked_Conversion (Log_Handler, System.Address);

   procedure Log_Dispatch
     (Level     : int;
      Message   : chars_ptr;
      User_Data : System.Address)
     with Convention => C;
   --  Trampoline the C library invokes on its worker threads.

   procedure Log_Dispatch
     (Level     : int;
      Message   : chars_ptr;
      User_Data : System.Address)
   is
      Ada_Level : constant Log_Level :=
        (case Level is
            when C_API.WISP_LOG_DEBUG   => Debug,
            when C_API.WISP_LOG_INFO    => Info,
            when C_API.WISP_LOG_WARNING => Warning,
            when others                 => Error);
   begin
      To_Log_Handler (User_Data) (Ada_Level, Value (Message));
   exception
      when others =>
         null;  --  exceptions must not cross the C boundary
   end Log_Dispatch;

   procedure Set_Log_Level (Level : Log_Level) is
   begin
      C_API.Set_Log_Level
        (case Level is
            when Debug   => C_API.WISP_LOG_DEBUG,
            when Info    => C_API.WISP_LOG_INFO,
            when Warning => C_API.WISP_LOG_WARNING,
            when Error   => C_API.WISP_LOG_ERROR);
   end Set_Log_Level;

   procedure Set_Log_Handler (Callback : Log_Handler) is
   begin
      if Callback = null then
         C_API.Set_Log_Handler (null, System.Null_Address);
      else
         C_API.Set_Log_Handler (Log_Dispatch'Access, To_Log_Address (Callback));
      end if;
   end Set_Log_Handler;

end Wisp;
