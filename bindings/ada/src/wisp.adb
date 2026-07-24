with Ada.Unchecked_Conversion;
with Ada.Unchecked_Deallocation;

with Interfaces.C;         use Interfaces.C;
with Interfaces.C.Strings; use Interfaces.C.Strings;

with System;

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

   -------------
   -- Connect --
   -------------

   procedure Connect
     (Address              : String;
      Client_Id            : String   := "";
      Keepalive_Time_Ms    : Positive := 3_000;
      Keepalive_Timeout_Ms : Positive := 10_000;
      Connect_Timeout_Ms   : Natural  := 5_000)
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
      Check (Code, "Connect");

      if Connect_Timeout_Ms > 0 then
         Check (C_API.Wait_For_Connection (int (Connect_Timeout_Ms)), "Connect");
      end if;
   end Connect;

   --------------
   -- Shutdown --
   --------------

   procedure Shutdown is
   begin
      C_API.Shutdown_Connection;
   end Shutdown;

   ------------------
   -- Is_Connected --
   ------------------

   function Is_Connected return Boolean is
   begin
      return C_API.Is_Connected /= 0;
   end Is_Connected;

   ----------
   -- Send --
   ----------

   procedure Send (Topic : String; Data : String) is
      C_Topic : chars_ptr := New_String (Topic);
      Code    : constant int :=
        C_API.Send_Data (C_Topic, Data'Address, Data'Length);
   begin
      Free (C_Topic);
      Check (Code, "Send");
   end Send;

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

   -----------
   -- Reply --
   -----------

   procedure Reply (Data : String) is
      Code : constant int :=
        C_API.Reply_To_Sender (Data'Address, Data'Length);
   begin
      Check (Code, "Reply");
   end Reply;

   -------------
   -- Request --
   -------------

   function Request
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
         Check (Code, "Request");
      end if;

      declare
         Response : constant String := Buffer (1 .. Natural (Out_Len));
      begin
         Free_Buffer (Buffer);
         return Response;
      end;
   end Request;

   -------------------------------
   -- Subscribe and Unsubscribe --
   -------------------------------

   --  A Handler value is a library-level code pointer, so it can serve
   --  directly as the C-side User_Data: Dispatch converts it back to call
   --  it, and it doubles as the registration identity for Unsubscribe.
   --  No allocation, nothing to free.

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

   procedure Subscribe (Topic : String; Callback : not null Handler) is
      C_Topic : chars_ptr := New_String (Topic);
   begin
      C_API.Register_Callback (C_Topic, Dispatch'Access, To_Address (Callback));
      Free (C_Topic);
   end Subscribe;

   procedure Unsubscribe (Topic : String; Callback : not null Handler) is
      C_Topic : chars_ptr := New_String (Topic);
   begin
      C_API.Unregister_Callback (C_Topic, To_Address (Callback));
      Free (C_Topic);
   end Unsubscribe;

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
