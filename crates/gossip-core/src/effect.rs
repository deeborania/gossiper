//! Effects returned by protocol state machines.

use crate::NodeId;

/// An action requested by the protocol core
///
/// The core does not execute the effects itself. A caller, runtime adapter, or simulator
/// is responsible for interpreting and executing them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect<Message, Event> {
    /// Send a protocol message to another node
    Send {
        /// The target node.
        target: NodeId,

        /// The message to send
        message: Message,
    },

    /// Emit an event to the application using the protocol.
    Emit(Event),
}

impl<Message, Event> Effect<Message, Event> {
    /// Maps the message inside this effect  while leaving events unchanged
    pub fn map_message<NextMessage>(
        self,
        map: impl FnOnce(Message) -> NextMessage,
    ) -> Effect<NextMessage, Event> {
        match self {
            Self::Send { target, message } => Effect::Send {
                target,
                message: map(message),
            },
            Self::Emit(event) => Effect::Emit(event),
        }
    }

    /// Maps the event inside this effect while leaving messages unchanged.
    pub fn map_event<NextEvent>(
        self,
        map: impl FnOnce(Event) -> NextEvent,
    ) -> Effect<Message, NextEvent> {
        match self {
            Self::Send { target, message } => Effect::Send { target, message },
            Self::Emit(event) => Effect::Emit(map(event)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Effect;
    use crate::NodeId;

    #[test]
    fn send_effect_contains_target_and_message() {
        let effect: Effect<&str, ()> = Effect::Send {
            target: NodeId::from("node-b"),
            message: "hello",
        };

        assert_eq!(
            effect,
            Effect::Send {
                target: NodeId::from("node-b"),
                message: "hello"
            }
        );
    }

    #[test]
    fn map_message_changes_only_message() {
        let effect: Effect<&str, ()> = Effect::Send {
            target: NodeId::from("node-b"),
            message: "hello",
        };

        let mapped = effect.map_message(|message| message.len());

        assert_eq!(
            mapped,
            Effect::Send {
                target: NodeId::from("node-b"),
                message: 5
            }
        );
    }

    #[test]
    fn map_event_changes_only_event() {
        let effect: Effect<(), &str> = Effect::Emit("joined");

        let mapped = effect.map_event(|event| event.len());

        assert_eq!(mapped, Effect::Emit(6));
    }
}
