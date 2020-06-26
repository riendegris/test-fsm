This project is a test bed, for playing with a finite state machine, and the publication
of states using 0MQ. The idea is pretty simple from the client side... You start an action, which
will require several steps to complete. Each of these intermediate step is a state in a finite
state machine. When the state machine changes state, it publishes a notification that is has
reached a new state. The overall goal is to keep the user notified of the progress made towards the
completion of a long running task.

This experiment is done in the context of mimirsbrunn. Consequently, states and events reflect
actions such as file download, indexing into elasticsearch and more.

## Install

This project relies on 0MQ for the [pub/sub
mechanism](http://zguide.zeromq.org/page:chapter1#Getting-the-Message-Out). So you need a zeromq
library on your system. For Ubuntu you can install `sudo apt install libzmqpp-dev`, for Archlinux,
that would be `pacman -S zeromq`, or `zeromq-devel` on CentOS
