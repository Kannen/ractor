// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

use std::any::Any;
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::AtomicU8;
#[cfg(feature = "statistics")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
#[cfg(feature = "statistics")]
use std::sync::Arc;
use std::sync::Mutex;

use crate::actor::messages::StopMessage;
#[cfg(feature = "derived-actor-from-cell")]
use crate::actor::request_derived::DerivedProvider;
#[cfg(feature = "derived-actor-from-cell")]
use crate::actor::request_derived::DerivedProviderType;
use crate::actor::supervision::SupervisionTree;
use crate::concurrency as mpsc;
use crate::concurrency::MpscUnboundedReceiver as InputPortReceiver;
use crate::concurrency::MpscUnboundedSender as InputPort;
use crate::concurrency::OneshotReceiver;
use crate::concurrency::OneshotSender as OneshotInputPort;
use crate::message::BoxedMessage;
use crate::message::LocalOrSerialized;
#[cfg(feature = "cluster")]
use crate::message::SerializedMessage;
use crate::Actor;
use crate::ActorId;
use crate::ActorName;
use crate::ActorStatus;
use crate::GroupName;
use crate::Message;
use crate::MessagingErr;
use crate::ScopeName;
use crate::Signal;
use crate::SupervisionEvent;

/// A muxed-message wrapper which allows the message port to receive either a message or a drain
/// request which is a point-in-time marker that the actor's input channel should be drained
pub(crate) enum MuxedMessage<T: Any + Send> {
    Drain,
    Message(BoxedMessage<T>),
}
pub(crate) trait GenericInputPort: Sync + Send + Any + 'static {
    fn send_drain(&self) -> Result<(), MessagingErr<()>>;
    #[cfg(feature = "cluster")]
    fn send_serialized(
        &self,
        message: SerializedMessage,
    ) -> Result<(), MessagingErr<SerializedMessage>>;
}
impl<T: Any + Send> GenericInputPort for InputPort<MuxedMessage<T>> {
    fn send_drain(&self) -> Result<(), MessagingErr<()>> {
        self.send(MuxedMessage::Drain)
            .map_err(|_| MessagingErr::SendErr(()))
    }
    #[cfg(feature = "cluster")]
    fn send_serialized(
        &self,
        message: SerializedMessage,
    ) -> Result<(), MessagingErr<SerializedMessage>> {
        let boxed = BoxedMessage {
            msg: LocalOrSerialized::Serialized(message),
            span: None,
        };
        Ok(self
            .send(MuxedMessage::Message(boxed))
            .map_err(|e| match e.0 {
                MuxedMessage::Message(m) => MessagingErr::SendErr(m.msg.into_serialized().unwrap()),
                _ => panic!("Expected a boxed message but got a drain message"),
            })?)
    }
}

#[derive(Default)]
pub(crate) struct MemberShip {
    pub(crate) scope_groups: Vec<(ScopeName, GroupName)>,
    pub(crate) listened_groups: Vec<(ScopeName, GroupName)>,
    pub(crate) listened_scopes: Vec<ScopeName>,
}

#[cfg(feature = "statistics")]
#[derive(Debug, Default, Clone)]
pub(crate) struct Statistics {
    pub(crate) message_queue_len: Arc<AtomicUsize>,
}

// The inner-properties of an Actor
pub(crate) struct ActorProperties {
    pub(crate) id: ActorId,
    pub(crate) name: Option<ActorName>,
    pub(crate) status: AtomicU8,
    pub(crate) wait_handler: mpsc::Notify,
    pub(crate) signal: Mutex<Option<OneshotInputPort<Signal>>>,
    pub(crate) stop: Mutex<Option<OneshotInputPort<StopMessage>>>,
    pub(crate) supervision: InputPort<SupervisionEvent>,
    pub(crate) message: Box<dyn GenericInputPort>,
    pub(crate) tree: SupervisionTree,
    pub(crate) type_id: std::any::TypeId,
    #[cfg(feature = "cluster")]
    pub(crate) supports_remoting: bool,
    #[cfg(feature = "derived-actor-from-cell")]
    pub(crate) derived_provider: Box<dyn DerivedProvider>,
    pub(crate) member_ship: Mutex<Option<MemberShip>>,
    #[cfg(feature = "statistics")]
    pub(crate) statistics: Statistics,
}

impl ActorProperties {
    pub(crate) fn new<TActor: Actor>(
        name: Option<ActorName>,
    ) -> (
        Self,
        OneshotReceiver<Signal>,
        OneshotReceiver<StopMessage>,
        InputPortReceiver<SupervisionEvent>,
        InputPortReceiver<MuxedMessage<TActor::Msg>>,
    )
    where
        TActor: Actor,
    {
        Self::new_remote::<TActor>(name, crate::actor::actor_id::get_new_local_id())
    }

    pub(crate) fn new_remote<TActor: Actor>(
        name: Option<ActorName>,
        id: ActorId,
    ) -> (
        Self,
        OneshotReceiver<Signal>,
        OneshotReceiver<StopMessage>,
        InputPortReceiver<SupervisionEvent>,
        InputPortReceiver<MuxedMessage<TActor::Msg>>,
    )
    where
        TActor: Actor,
    {
        let (tx_signal, rx_signal) = mpsc::oneshot();
        let (tx_stop, rx_stop) = mpsc::oneshot();
        let (tx_supervision, rx_supervision) = mpsc::mpsc_unbounded();
        let (tx_message, rx_message) = mpsc::mpsc_unbounded();
        (
            Self {
                id,
                name,
                status: AtomicU8::new(ActorStatus::Unstarted as u8),
                signal: Mutex::new(Some(tx_signal)),
                wait_handler: mpsc::Notify::new(),
                stop: Mutex::new(Some(tx_stop)),
                supervision: tx_supervision,
                message: Box::new(tx_message),
                tree: SupervisionTree::default(),
                type_id: std::any::TypeId::of::<TActor::Msg>(),
                #[cfg(feature = "cluster")]
                supports_remoting: TActor::Msg::serializable(),
                #[cfg(feature = "derived-actor-from-cell")]
                derived_provider: Box::new(DerivedProviderType::<TActor>::new()),
                member_ship: Mutex::new(Some(MemberShip::default())),
                #[cfg(feature = "statistics")]
                statistics: Statistics::default(),
            },
            rx_signal,
            rx_stop,
            rx_supervision,
            rx_message,
        )
    }
    #[cfg(feature = "statistics")]
    pub(crate) fn statistics(&self) -> &Statistics {
        &self.statistics
    }
    /// Declare removal of membership to scope/group.
    pub(crate) fn can_monitor(&self) -> bool {
        let Ok(lk) = self.member_ship.lock() else {
            return false;
        };
        lk.is_some()
    }
    /// Declare removal of membership to scope/group.
    pub(crate) fn remove_member_ship(&self, scope: ScopeName, group: GroupName) {
        let Ok(mut lk) = self.member_ship.lock() else {
            return;
        };
        if let Some(v) = &mut *lk {
            v.scope_groups.retain(|(s, g)| *s != scope || *g != group);
        }
    }
    /// Declare membership to scope/group.
    /// If it return false, the insertion should be abandonned.
    pub(crate) fn add_member_ship(&self, scope: ScopeName, group: GroupName) -> bool {
        let Ok(mut lk) = self.member_ship.lock() else {
            return false;
        };
        if let Some(v) = &mut *lk {
            if !v
                .scope_groups
                .iter()
                .any(|(s, g)| *s == scope && *g == group)
            {
                v.scope_groups.push((scope, group))
            }
            true
        } else {
            false
        }
    }
    /// Declare removal of listening to scope/group.
    pub(crate) fn remove_listen_group<S, G>(&self, scope: &S, group: &G)
    where
        S: Hash + Eq + ?Sized,
        G: Hash + Eq + ?Sized,
        ScopeName: Borrow<S>,
        GroupName: Borrow<G>,
    {
        let Ok(mut lk) = self.member_ship.lock() else {
            return;
        };
        if let Some(v) = &mut *lk {
            v.listened_groups.retain(|(s, g)| {
                <ScopeName as Borrow<S>>::borrow(s) != scope
                    || <GroupName as Borrow<G>>::borrow(g) != group
            });
        }
    }
    /// Declare listening scope/group.
    /// If it return false, the insertion should be abandonned.
    pub(crate) fn add_listen_group(&self, scope: ScopeName, group: GroupName) -> bool {
        let Ok(mut lk) = self.member_ship.lock() else {
            return false;
        };
        if let Some(v) = &mut *lk {
            if !v
                .listened_groups
                .iter()
                .any(|(s, g)| *s == scope && *g == group)
            {
                v.listened_groups.push((scope, group))
            }
            true
        } else {
            false
        }
    }
    /// Declare removal of listening to scope.
    pub(crate) fn remove_listen_scope<S>(&self, scope: &S)
    where
        S: Hash + Eq + ?Sized,
        ScopeName: Borrow<S>,
    {
        let Ok(mut lk) = self.member_ship.lock() else {
            return;
        };
        if let Some(v) = &mut *lk {
            v.listened_scopes.retain(|s| s.borrow() != scope);
        }
    }
    // Declare listening scope.
    // If it return false, the insertion should be abandonned.
    pub(crate) fn add_listen_scope(&self, scope: ScopeName) -> bool {
        let Ok(mut lk) = self.member_ship.lock() else {
            return false;
        };
        if let Some(v) = &mut *lk {
            if !v.listened_scopes.iter().any(|s| *s == scope) {
                v.listened_scopes.push(scope)
            }
            true
        } else {
            false
        }
    }
    pub(crate) fn remove_member_ship_ability(&self) -> MemberShip {
        let Ok(mut lk) = self.member_ship.lock() else {
            return MemberShip::default();
        };
        if let Some(v) = &mut *lk {
            std::mem::take(v)
        } else {
            MemberShip::default()
        }
    }

    pub(crate) fn get_status(&self) -> ActorStatus {
        match self.status.load(Ordering::SeqCst) {
            0u8 => ActorStatus::Unstarted,
            1u8 => ActorStatus::Starting,
            2u8 => ActorStatus::Running,
            3u8 => ActorStatus::Upgrading,
            4u8 => ActorStatus::Draining,
            5u8 => ActorStatus::Stopping,
            _ => ActorStatus::Stopped,
        }
    }

    pub(crate) fn set_status(&self, status: ActorStatus) {
        self.status.store(status as u8, Ordering::SeqCst);
    }

    pub(crate) fn send_signal(&self, signal: Signal) -> Result<(), MessagingErr<()>> {
        self.signal
            .lock()
            .unwrap()
            .take()
            .map_or(Err(MessagingErr::ChannelClosed), |prt| {
                prt.send(signal).map_err(|_| MessagingErr::ChannelClosed)
            })
    }

    pub(crate) fn send_supervisor_evt(
        &self,
        message: SupervisionEvent,
    ) -> Result<(), MessagingErr<SupervisionEvent>> {
        self.supervision.send(message).map_err(|e| e.into())
    }

    pub(crate) fn send_message<TMessage>(
        &self,
        message: TMessage,
    ) -> Result<(), MessagingErr<TMessage>>
    where
        TMessage: Message,
    {
        let status = self.get_status();
        if status >= ActorStatus::Draining {
            // if currently draining, stopping or stopped: reject messages directly.
            return Err(MessagingErr::SendErr(message));
        }

        let boxed = message
            .box_message(&self.id)
            .map_err(|_e| MessagingErr::InvalidActorType)?;

        match boxed {
            #[cfg(feature = "cluster")]
            BoxedMessage {
                span: _,
                msg: LocalOrSerialized::Serialized(m),
            } => self.message.send_serialized(m).map_err(|e| match e {
                MessagingErr::SendErr(m) => MessagingErr::SendErr(
                    TMessage::from_boxed(BoxedMessage {
                        span: None,
                        msg: LocalOrSerialized::Serialized(m),
                    })
                    .unwrap(),
                ),
                MessagingErr::ChannelClosed => MessagingErr::ChannelClosed,
                MessagingErr::InvalidActorType => MessagingErr::InvalidActorType,
            }),
            boxed => {
                let sender: &InputPort<MuxedMessage<TMessage>> = {
                    let ptr: &dyn Any = &*self.message;
                    ptr.downcast_ref().ok_or(MessagingErr::InvalidActorType)?
                };
                sender
                    .send(MuxedMessage::Message(boxed))
                    .map_err(|e| match e.0 {
                        MuxedMessage::Message(m) => {
                            MessagingErr::SendErr(TMessage::from_boxed(m).unwrap())
                        }
                        _ => panic!("Expected a boxed message but got a drain message"),
                    })
            }
        }
    }
    #[allow(unsafe_code)]
    /// ## SAFETY
    /// Shall only be called on an actor cell refering to a local
    /// actor whose message type is TMessage
    pub(crate) unsafe fn send_local_message_unchecked<TMessage>(
        &self,
        message: TMessage,
    ) -> Result<(), MessagingErr<TMessage>>
    where
        TMessage: Message,
    {
        let status = self.get_status();
        if status >= ActorStatus::Draining {
            // if currently draining, stopping or stopped: reject messages directly.
            return Err(MessagingErr::SendErr(message));
        }
        #[allow(unsafe_code)]
        // SAFETY: safe as long as function contract is ensured by caller
        let sender: &InputPort<MuxedMessage<TMessage>> = unsafe {
            let ptr: &dyn Any = &*self.message;
            &*(ptr as *const dyn Any as *const InputPort<MuxedMessage<TMessage>>)
        };
        let span = {
            #[cfg(feature = "message_span_propogation")]
            {
                Some(tracing::Span::current())
            }
            #[cfg(not(feature = "message_span_propogation"))]
            {
                None
            }
        };
        let boxed = BoxedMessage {
            msg: LocalOrSerialized::Local(message),
            span,
        };
        sender
            .send(MuxedMessage::Message(boxed))
            .map_err(|e| match e.0 {
                MuxedMessage::Message(m) => MessagingErr::SendErr(TMessage::from_boxed(m).unwrap()),
                _ => panic!("Expected a boxed message but got a drain message"),
            })
    }

    pub(crate) fn drain(&self) -> Result<(), MessagingErr<()>> {
        let _ = self
            .status
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |f| {
                if f < (ActorStatus::Stopping as u8) {
                    Some(ActorStatus::Draining as u8)
                } else {
                    None
                }
            });
        self.message.send_drain()
    }

    /// Start draining, and wait for the actor to exit
    pub(crate) async fn drain_and_wait(&self) -> Result<(), MessagingErr<()>> {
        let rx = self.wait_handler.notified();
        self.drain()?;
        rx.await;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn send_serialized(
        &self,
        message: SerializedMessage,
    ) -> Result<(), Box<MessagingErr<SerializedMessage>>> {
        self.message.send_serialized(message).map_err(Box::new)
    }

    pub(crate) fn send_stop(
        &self,
        reason: Option<String>,
    ) -> Result<(), MessagingErr<StopMessage>> {
        let msg = reason.map(StopMessage::Reason).unwrap_or(StopMessage::Stop);
        self.stop
            .lock()
            .unwrap()
            .take()
            .map_or(Err(MessagingErr::ChannelClosed), |prt| {
                prt.send(msg).map_err(|_| MessagingErr::ChannelClosed)
            })
    }

    /// Send the stop signal, threading in a OneShot sender which notifies when the shutdown is completed
    pub(crate) async fn send_stop_and_wait(
        &self,
        reason: Option<String>,
    ) -> Result<(), MessagingErr<StopMessage>> {
        let rx = self.wait_handler.notified();
        self.send_stop(reason)?;
        rx.await;
        Ok(())
    }

    /// Wait for the actor to exit
    pub(crate) async fn wait(&self) {
        let rx = self.wait_handler.notified();
        rx.await;
    }

    /// Send the kill signal, threading in a OneShot sender which notifies when the shutdown is completed
    pub(crate) async fn send_signal_and_wait(
        &self,
        signal: Signal,
    ) -> Result<(), MessagingErr<()>> {
        // first bind the wait handler
        let rx = self.wait_handler.notified();
        let _ = self.send_signal(signal);
        rx.await;
        Ok(())
    }

    pub(crate) fn notify_stop_listener(&self) {
        self.wait_handler.notify_waiters();
        // make sure that any future caller immediately returns by pre-storing
        // a notify permit (i.e. the actor stops, but you are only start waiting
        // after the actor has already notified it's dead.)
        self.wait_handler.notify_one();
    }
}
