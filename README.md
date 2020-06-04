# substrate-tools [![CircleCI](https://circleci.com/gh/uw-labs/substrate-tools.svg?style=svg)](https://circleci.com/gh/uw-labs/substrate-tools)
Collection of middleware, wrappers and other tools for substrate.

## Wrappers

### Ack Ordering
Is a message source wrapper that allows the user to acknowledge messages in any order and it will ensure
messages are sent to the actual message source in the same order they are consumed.

### Async
Is an async message source wrapper that allows the user to utilise a handler pattern for interacting
with an async message source. It removes the need to manually handle the message and acknowledgement
channels.

### Instrumented
Provides wrappers for both message source and message sink that add prometheus metrics labeled with topic and status (either success or error).

### Multi
Is a message source wrapper that wraps any number of sources. It consumes messages from all of them and passes them on to the user.
It ensures that the acknowledgements are passed to the correct source.

## Other

### Message
Provides a simple implementation of the `substrate.Message` interface.

### Mock
Provides a mock message source that can be used in testing as is done in this repo.
