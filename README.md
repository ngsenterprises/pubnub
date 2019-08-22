
Distributed median algorithm using the Pubnub async api.

Objects of the PubNub CallBack object are created to represent 
distributed nodes.

A supervisor object is subscribed to 4 worker objects and those worker
objects are subscribed to the superviser object.

The supervisor sends a message for the workers to do a pivot on a common
value and send the length of the segments back to the supervisor. Eventually 
the pivot will be the median index and the algorithm will terminate.
 

