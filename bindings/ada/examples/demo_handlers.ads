--  Subscription handlers for the demo. They live in a library-level package
--  because Wisp.Handler values must designate library-level procedures.

package Demo_Handlers is

   procedure Print (Topic : String; Data : String);
   --  Log the message to standard output.

   procedure Echo (Topic : String; Data : String);
   --  Reply to the sender with the payload it sent.

   procedure Print_Any (Topic, Type_Name, Value : String);
   --  Log a packed protobuf payload: the type it claims and its size. Wisp
   --  never parses it, so neither does this - decoding is the application's
   --  business, and its own concern which codec does it.

end Demo_Handlers;
