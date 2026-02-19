#![allow(clippy::unused_async)]

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Form;
use axum_extra::{headers, TypedHeader};
use constant_time_eq::constant_time_eq;
use serde::{Deserialize, Serialize};
use serde_with::{json::JsonString, serde_as};
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use crate::app_error::AppError;
use crate::app_state::AppState;
use crate::auth::AuthContext;
use crate::narrow::Narrow;
use crate::queues::{ClientEventEntry, ClientInfo, QueueId};
use crate::response::{json_error, json_error_code, json_success, ErrorCode};
use crate::types::{RealmId, UserId};

type EventId = i64;

#[allow(dead_code, clippy::struct_excessive_bools)]
#[serde_as]
#[derive(Debug, Deserialize)]
pub struct GetEventsRequest {
    user_client: Option<Cow<'static, str>>,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    last_event_id: Option<EventId>,
    queue_id: Option<QueueId>,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    apply_markdown: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    client_gravatar: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    slim_presence: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    simplified_presence_events: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    all_public_streams: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    event_types: Option<HashSet<String>>,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    dont_block: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    narrow: Narrow,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    lifespan_secs: u32,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    bulk_message_deletion: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    stream_typing_notifications: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    user_settings_object: bool,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    pronouns_field_type_supported: Option<bool>,
    #[serde_as(as = "JsonString")]
    #[serde(default)]
    linkifier_url_template: bool,
}

#[derive(Serialize)]
struct GetEventsResponse {
    events: Vec<ClientEventEntry>,
    queue_id: QueueId,
}

fn bad_queue_id(queue_id: &QueueId) -> Response {
    (
        StatusCode::BAD_REQUEST,
        json_error_code(
            format!("Bad event queue id: {queue_id}"),
            ErrorCode::BadEventQueueId,
        ),
    )
        .into_response()
}

fn get_events_response(events: Vec<ClientEventEntry>, queue_id: QueueId) -> Response {
    (
        TypedHeader(headers::CacheControl::new().with_no_store().with_private()),
        json_success(GetEventsResponse { events, queue_id }),
    )
        .into_response()
}

async fn get_events_backend(
    state: Arc<AppState>,
    user_profile_id: UserId,
    realm_id: RealmId,
    args: GetEventsRequest,
) -> Result<Response, AppError> {
    tracing::debug!("get_events_backend user={user_profile_id:?} {args:?}");

    if args.all_public_streams {
        // TODO/boq: check if the user is allowed to access public streams
    }

    let (queue_id, receiver) = {
        let mut queues = state.queues.lock().unwrap();

        if let Some(queue_id) = args.queue_id {
            let Some(client) = queues.by_id(user_profile_id, &queue_id) else {
                return Ok(bad_queue_id(&queue_id));
            };

            let events = client.queue.peek_events(args.last_event_id);
            if !events.is_empty() || args.dont_block {
                return Ok(get_events_response(events, queue_id));
            }

            if let Some(receiver) = client.queue.wait_for_events() {
                (queue_id, receiver)
            } else {
                let events = client.queue.peek_events(args.last_event_id);
                return Ok(get_events_response(events, queue_id));
            }
        } else if args.dont_block {
            let events = vec![];
            let info = ClientInfo {
                user_profile_id,
                realm_id,
                event_types: args.event_types,
                client_type_name: args.user_client.unwrap_or(Cow::Borrowed("unknown-client")), // TODO/boq: detect from User-Agent
                apply_markdown: args.apply_markdown,
                client_gravatar: args.client_gravatar,
                slim_presence: args.slim_presence,
                simplified_presence_events: args.simplified_presence_events,
                all_public_streams: args.all_public_streams,
                queue_timeout: args.lifespan_secs,
                narrow: args.narrow,
                bulk_message_deletion: args.bulk_message_deletion,
                stream_typing_notifications: args.stream_typing_notifications,
                user_settings_object: args.user_settings_object,
                pronouns_field_type_supported: args.pronouns_field_type_supported.unwrap_or(true), // TODO/boq: detect from User-Agent
                linkifier_url_template: args.linkifier_url_template,
            };
            let queue_id = queues.register(info);
            return Ok(get_events_response(events, queue_id));
        } else {
            return Ok((
                StatusCode::BAD_REQUEST,
                json_error("Missing 'queue_id' argument"),
            )
                .into_response());
        }
    };

    let mut shutdown_rx = state.shutdown_rx.clone();
    let events = tokio::select! {
        result = receiver => {
            if result.is_ok() {
                let mut queues = state.queues.lock().unwrap();
                let Some(client) = queues.by_id(user_profile_id, &queue_id) else {
                    return Ok(bad_queue_id(&queue_id));
                };
                client.queue.peek_events(args.last_event_id)
            } else {
                vec![]
            }
        }
        () = shutdown_rx.wait() => vec![],
    };

    Ok(get_events_response(events, queue_id))
}

/// Handle `GET /`.
pub async fn get_root() -> &'static str {
    "boq 🤖\n"
}

/// Handle `POST /notify_tornado`.
pub async fn post_notify_tornado() {
    // TODO/boq: notify queues
}

/// Handle `GET /json/events` and `GET /api/v1/events`.
pub async fn get_events(
    State(state): State<Arc<AppState>>,
    Extension(AuthContext { user_id, realm_id }): Extension<AuthContext>,
    Form(args): Form<GetEventsRequest>,
) -> impl IntoResponse {
    // TODO/boq: check user port

    get_events_backend(state, user_id, realm_id, args).await
}

#[derive(Debug, Deserialize)]
pub struct DeleteEventsRequest {
    queue_id: QueueId,
}

/// Handle `GET /json/events` and `GET /api/v1/events`.
pub async fn delete_events(
    State(state): State<Arc<AppState>>,
    Extension(AuthContext { user_id, .. }): Extension<AuthContext>,
    Form(args): Form<DeleteEventsRequest>,
) -> Response {
    let mut queues = state.queues.lock().unwrap();
    if queues.delete(user_id, args.queue_id) {
        json_success(()).into_response()
    } else {
        bad_queue_id(&args.queue_id)
    }
}

#[derive(Debug, Deserialize)]
pub struct PostEventsInternalRequest {
    secret: String,
    user_profile_id: UserId,
    #[serde(flatten)]
    args: GetEventsRequest,
}

/// Handle `POST /api/v1/events/internal`.
pub async fn post_events_internal(
    State(state): State<Arc<AppState>>,
    Form(PostEventsInternalRequest {
        secret,
        user_profile_id,
        mut args,
    }): Form<PostEventsInternalRequest>,
) -> Result<Response, AppError> {
    tracing::debug!("post_events_internal user_profile_id={user_profile_id} args={args:?}");

    if !constant_time_eq(secret.as_bytes(), state.shared_secret.as_bytes()) {
        return Ok((StatusCode::FORBIDDEN, json_error("Access denied")).into_response());
    }

    let db = state.db_pool.get().await?;
    let sql = "SELECT realm_id FROM zerver_userprofile WHERE id = $1";
    let Some(session_row) = db.query_opt(sql, &[&user_profile_id]).await? else {
        return Ok(StatusCode::UNAUTHORIZED.into_response());
    };

    let realm_id: RealmId = session_row.get(0);

    args.user_client.get_or_insert_with(|| "internal".into());

    // TODO/boq: publish UserActivity record

    get_events_backend(state, user_profile_id, realm_id, args).await
}
