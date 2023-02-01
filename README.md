# Safe Scheduler

Safe scheduler is a simple event scheduler that finds the most suitable time for two (or more) people to meet depending 
on their availability.  
It uses [Flowframe](https://github.com/owenarden/flowframe), which is a Scala compiler plugin that performs fine-grained
 information flow analysis on code to detect privacy violations.
To do this, FlowFrame enforces information flow control policies in Scala Spark applications, specified by @Policy annotations
 that specify purpose limitations. Refer to the Flowframe documentation to learn more.  

Safe scheduler uses policies to ensure that only relevant information (like invitees, start and end time of their
 respective events, etc) is accessible when finding a suitable meeting time - as opposed to event details which is
 irrelevant (and potentially confidential) in the context. 
 
 
 

