--  Subscription handlers for the demo. They live in a library-level package
--  because Wisp.Handler values must designate library-level procedures.

package Demo_Handlers is

   procedure Print (Topic : String; Data : String);
   --  Log the message to standard output.

   procedure Echo (Topic : String; Data : String);
   --  Reply to the sender with the payload it sent.

end Demo_Handlers;
