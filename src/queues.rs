use serde::Serialize;
use slab::Slab;
use std::borrow::Cow;
use std::collections::hash_map::{Entry, HashMap};
use std::collections::HashSet;
use std::collections::VecDeque;
use tokio::sync::oneshot::{channel, Receiver, Sender};
use uuid::Uuid;

use crate::narrow::{matches_narrow, Narrow};
use crate::notice::{ClientEvent, SpecialClientEvent};
use crate::types::RealmId;
use crate::types::UserId;

pub type QueueId = Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ClientKey(usize);

pub type EventId = i64;

#[derive(Clone, Serialize)]
pub struct ClientEventEntry {
    id: EventId,
    #[serde(flatten)]
    event: ClientEvent,
}

pub struct Queue {
    events: VecDeque<ClientEventEntry>,
    next_event_id: EventId,
    sender: Option<Sender<()>>,
}

impl Queue {
    pub fn peek_events(&mut self, last_event_id: Option<EventId>) -> Vec<ClientEventEntry> {
        self.sender.take();
        if let Some(last_event_id) = last_event_id {
            while let Some(event) = self.events.front() {
                if event.id > last_event_id {
                    break;
                }
                self.events.pop_front();
            }
        }
        self.events.iter().cloned().collect()
    }

    pub fn wait_for_events(&mut self) -> Option<Receiver<()>> {
        self.events.is_empty().then(|| {
            let (sender, receiver) = channel();
            self.sender.replace(sender);
            receiver
        })
    }
}

pub struct ClientInfo {
    pub user_profile_id: UserId,
    pub realm_id: RealmId,
    pub event_types: Option<HashSet<String>>,
    pub client_type_name: Cow<'static, str>,
    pub apply_markdown: bool,
    pub client_gravatar: bool,
    pub slim_presence: bool,
    pub simplified_presence_events: bool,
    pub all_public_streams: bool,
    pub queue_timeout: u32,
    pub narrow: Narrow,
    pub bulk_message_deletion: bool,
    pub stream_typing_notifications: bool,
    pub user_settings_object: bool,
    pub pronouns_field_type_supported: bool,
    pub linkifier_url_template: bool,
}

pub struct Client {
    info: ClientInfo,
    pub queue_id: QueueId,
    pub queue: Queue,
}

impl Client {
    pub fn info(&self) -> &ClientInfo {
        &self.info
    }

    fn accepts_type(&self, event_type: &str) -> bool {
        !self
            .info
            .event_types
            .as_ref()
            .is_some_and(|event_types| !event_types.contains(event_type))
    }

    // TODO: Refactor so we don't need this function
    pub fn accepts_messages(&self) -> bool {
        self.accepts_type("message")
    }

    pub fn accepts_event(&self, event: &ClientEvent) -> bool {
        match event {
            ClientEvent::Special(SpecialClientEvent::Message { message, flags, .. }) => {
                self.accepts_type("message") && matches_narrow(message, flags, &self.info.narrow)
            }
            ClientEvent::Special(SpecialClientEvent::UpdateMessage { .. }) => {
                self.accepts_type("update_message")
            }
            ClientEvent::Special(SpecialClientEvent::DeleteMessage { .. }) => {
                self.accepts_type("delete_message")
            }
            ClientEvent::Special(
                SpecialClientEvent::Presence { .. } | SpecialClientEvent::PresenceModern { .. },
            ) => self.accepts_type("presence"),
            ClientEvent::Special(SpecialClientEvent::CustomProfileFields { .. }) => {
                self.accepts_type("custom_profile_fields")
            }
            ClientEvent::Other { r#type, .. } if !self.accepts_type(r#type) => false,
            ClientEvent::Other { r#type, attrs } => {
                match r#type.as_str() {
                    // Suppress muted_topics events for clients that support
                    // user_topic. This allows clients to request both the
                    // user_topic and muted_topics event types, and receive the
                    // duplicate muted_topics data only on older servers that
                    // don't support user_topic.
                    "muted_topics" => !self.accepts_type("user_topic"),
                    // Typing notifications for stream messages are only
                    // delivered if the stream_typing_notifications
                    // client_capability is enabled, for backwards
                    // compatibility.
                    "typing" => {
                        !attrs.contains_key("stream_id") || self.info.stream_typing_notifications
                    }
                    // 'update_display_settings' and
                    // 'update_global_notifications' events are sent only if
                    // user_settings_object is false, otherwise only
                    // 'user_settings' event is sent.
                    "update_display_settings" | "update_global_notifications" => {
                        !self.info.user_settings_object
                    }
                    _ => true,
                }
            }
        }
    }

    pub fn add_event(&mut self, event: ClientEvent) {
        self.queue.events.push_back(ClientEventEntry {
            id: self.queue.next_event_id,
            event,
        });
        self.queue.next_event_id += 1;
        if let Some(sender) = self.queue.sender.take() {
            let _ = sender.send(());
        }
    }
}

pub struct Queues {
    clients: Slab<Client>,
    clients_by_queue_id: HashMap<QueueId, ClientKey>,
    user_clients: HashMap<UserId, HashSet<ClientKey>>,
    realm_clients_all_streams: HashMap<RealmId, HashSet<ClientKey>>,
}

impl Queues {
    pub fn new() -> Queues {
        Queues {
            clients: Slab::new(),
            clients_by_queue_id: HashMap::new(),
            user_clients: HashMap::new(),
            realm_clients_all_streams: HashMap::new(),
        }
    }

    pub fn register(&mut self, info: ClientInfo) -> QueueId {
        let ClientInfo {
            user_profile_id: user_id,
            realm_id,
            all_public_streams,
            ..
        } = info;
        let narrow_empty = info.narrow.is_empty();

        let queue_id = Uuid::new_v4();
        let client_key = ClientKey(self.clients.insert(Client {
            info,
            queue_id,
            queue: Queue {
                events: VecDeque::new(),
                next_event_id: 0,
                sender: None,
            },
        }));
        assert!(self
            .clients_by_queue_id
            .insert(queue_id, client_key)
            .is_none());
        self.user_clients
            .entry(user_id)
            .or_default()
            .insert(client_key);
        if all_public_streams || !narrow_empty {
            self.realm_clients_all_streams
                .entry(realm_id)
                .or_default()
                .insert(client_key);
        }

        queue_id
    }

    pub fn get(&self, key: ClientKey) -> &Client {
        &self.clients[key.0]
    }

    pub fn get_mut(&mut self, key: ClientKey) -> &mut Client {
        &mut self.clients[key.0]
    }

    pub fn by_id(&mut self, user_id: UserId, queue_id: &QueueId) -> Option<&mut Client> {
        let client = &mut self.clients[self.clients_by_queue_id.get(queue_id)?.0];
        (client.info.user_profile_id == user_id).then_some(client)
    }

    pub fn for_user(&self, user_id: UserId) -> Option<&HashSet<ClientKey>> {
        self.user_clients.get(&user_id)
    }

    pub fn for_realm_all_streams(&self, realm_id: RealmId) -> Option<&HashSet<ClientKey>> {
        self.realm_clients_all_streams.get(&realm_id)
    }

    pub fn delete(&mut self, user_id: UserId, queue_id: QueueId) -> bool {
        let Entry::Occupied(entry) = self.clients_by_queue_id.entry(queue_id) else {
            return false;
        };
        let client_key = *entry.get();
        let client = &self.clients[client_key.0];
        if client.info.user_profile_id != user_id {
            return false;
        }

        entry.remove();
        if let Entry::Occupied(mut entry) = self.user_clients.entry(client.info.user_profile_id) {
            let clients = entry.get_mut();
            if clients.remove(&client_key) && clients.is_empty() {
                entry.remove();
            }
        }
        if let Entry::Occupied(mut entry) =
            self.realm_clients_all_streams.entry(client.info.realm_id)
        {
            let clients = entry.get_mut();
            if clients.remove(&client_key) && clients.is_empty() {
                entry.remove();
            }
        }
        self.clients.remove(client_key.0);
        true
    }
}
