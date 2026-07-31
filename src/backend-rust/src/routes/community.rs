use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::Response,
    routing::{get, post, put},
};
use paladinscat_core::database::Database;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::{error::ApiError, request::RequestId};

use super::identity::{Session, as_i64, json_response, parse_id, require_session, simple_error};

#[derive(Clone)]
struct CommunityState {
    database: Database,
    twitch: TwitchClient,
}

#[derive(Clone)]
struct TwitchClient {
    http: reqwest::Client,
    state: Arc<Mutex<TwitchState>>,
}

struct TwitchState {
    token: Option<(String, Instant)>,
    category_id: Option<String>,
    streams: Option<(Value, Instant)>,
}

#[derive(Deserialize)]
struct TwitchToken {
    access_token: Option<String>,
    expires_in: Option<u64>,
}

impl TwitchClient {
    fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(8))
                .build()
                .expect("Twitch HTTP client"),
            state: Arc::new(Mutex::new(TwitchState {
                token: None,
                category_id: None,
                streams: None,
            })),
        }
    }

    async fn token(&self, client_id: &str, secret: &str) -> Result<String, reqwest::Error> {
        if let Some((token, expires)) = self.state.lock().await.token.as_ref()
            && *expires > Instant::now()
        {
            return Ok(token.clone());
        }
        let payload = self
            .http
            .post("https://id.twitch.tv/oauth2/token")
            .form(&[
                ("client_id", client_id),
                ("client_secret", secret),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await?
            .error_for_status()?
            .json::<TwitchToken>()
            .await?;
        let token = payload.access_token.unwrap_or_default();
        let expires = Instant::now()
            + Duration::from_secs(payload.expires_in.unwrap_or(60).saturating_sub(60));
        self.state.lock().await.token = Some((token.clone(), expires));
        Ok(token)
    }

    async fn streams(&self) -> Value {
        let Some(client_id) = std::env::var("TWITCH_CLIENT_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return json!({"configured":false,"streams":[]});
        };
        let Some(secret) = std::env::var("TWITCH_CLIENT_SECRET")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return json!({"configured":false,"streams":[]});
        };
        if let Some((streams, expires)) = self.state.lock().await.streams.as_ref()
            && *expires > Instant::now()
        {
            return streams.clone();
        }
        let result = self
            .fetch_streams(&client_id, &secret)
            .await
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "unable to load Paladins Twitch streams");
                json!({"configured":true,"streams":[]})
            });
        self.state.lock().await.streams =
            Some((result.clone(), Instant::now() + Duration::from_secs(60)));
        result
    }

    async fn fetch_streams(&self, client_id: &str, secret: &str) -> Result<Value, reqwest::Error> {
        let token = self.token(client_id, secret).await?;
        let category = if let Some(category) = self.state.lock().await.category_id.clone() {
            category
        } else {
            let games = self
                .http
                .get("https://api.twitch.tv/helix/games?name=Paladins")
                .bearer_auth(&token)
                .header("Client-Id", client_id)
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            let category = games
                .get("data")
                .and_then(Value::as_array)
                .and_then(|rows| rows.first())
                .and_then(|row| row.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            self.state.lock().await.category_id = Some(category.clone());
            category
        };
        if category.is_empty() {
            return Ok(json!({"configured":true,"streams":[]}));
        }
        let response = self
            .http
            .get(format!(
                "https://api.twitch.tv/helix/streams?game_id={category}&first=11"
            ))
            .bearer_auth(&token)
            .header("Client-Id", client_id)
            .send()
            .await?
            .error_for_status()?
            .json::<Value>()
            .await?;
        let streams = response
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|row| {
                row.get("user_login")
                    .and_then(Value::as_str)
                    .is_some_and(|login| !login.eq_ignore_ascii_case("paladins2ttv"))
            })
            .take(10)
            .map(|row| {
                let login = row
                    .get("user_login")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let tags = row
                    .get("tags")
                    .and_then(Value::as_array)
                    .map(|rows| rows.iter().take(3).cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                json!({
                    "userLogin":login,
                    "userName":row.get("user_name").cloned().unwrap_or(Value::Null),
                    "title":row.get("title").cloned().unwrap_or(Value::Null),
                    "viewerCount":row.get("viewer_count").cloned().unwrap_or(Value::Null),
                    "language":row.get("language").cloned().unwrap_or(Value::Null),
                    "thumbnailUrl":row.get("thumbnail_url").and_then(Value::as_str).unwrap_or_default()
                        .replace("{width}","320").replace("{height}","180"),
                    "tags":tags,
                    "url":format!("https://www.twitch.tv/{}",url::form_urlencoded::byte_serialize(login.as_bytes()).collect::<String>())
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"configured":true,"streams":streams}))
    }
}

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/community/streams", get(streams))
        .route("/community/posts", get(posts).post(create_post))
        .route(
            "/community/posts/{id}",
            get(post_detail).put(update_post).delete(delete_post),
        )
        .route("/community/posts/{id}/comments", post(create_comment))
        .route(
            "/community/comments/{id}",
            put(update_comment).delete(delete_comment),
        )
        .route("/community/posts/{id}/like", post(like_post))
        .with_state(CommunityState {
            database,
            twitch: TwitchClient::new(),
        })
}

async fn streams(State(state): State<CommunityState>) -> Response {
    let mut response = json_response(StatusCode::OK, state.twitch.streams().await);
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30, s-maxage=60"),
    );
    response
}

async fn posts(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = query
        .get("limit")
        .and_then(|value| parse_id(value))
        .unwrap_or(20)
        .min(100);
    let rows = state
        .database
        .query_json(
            "SELECT p.id,p.user_id,p.title,p.content,p.build_id,p.likes,p.view_count,p.created_at, \
               u.username,u.linked_player_id,tl.post_id AS tier_list_id \
             FROM posts p JOIN users u ON u.id=p.user_id LEFT JOIN tier_lists tl ON tl.post_id=p.id \
             ORDER BY p.created_at DESC LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(StatusCode::OK, Value::Array(rows)))
}

fn text(body: &Value, field: &str) -> String {
    body.get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

async fn create_post(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let title = text(&body, "title");
    let content = text(&body, "content");
    if title.is_empty() || content.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Title and content are required",
        ));
    }
    let build_id = body
        .get("build_id")
        .filter(|value| !value.is_null())
        .and_then(|value| as_i64(Some(value)));
    let mut post = state
        .database
        .one_json(
            "INSERT INTO posts(user_id,title,content,build_id) VALUES($1,$2,$3,$4) RETURNING *",
            &[&session.user_id, &title, &content, &build_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::internal(&request_id))?;
    if let Some(object) = post.as_object_mut() {
        object.insert("username".to_owned(), Value::String(session.username));
        object.insert(
            "linked_player_id".to_owned(),
            session
                .linked_player_id
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
    }
    Ok(json_response(StatusCode::OK, post))
}

async fn post_detail(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid post id"));
    };
    let post = state
        .database
        .one_json(
            "UPDATE posts p SET view_count=p.view_count+1 FROM users u WHERE p.id=$1 AND u.id=p.user_id \
             RETURNING p.*,u.username,u.linked_player_id,(SELECT tl.post_id FROM tier_lists tl WHERE tl.post_id=p.id) AS tier_list_id",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(post) = post else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Post not found"));
    };
    let comments = state
        .database
        .query_json(
            "SELECT c.*,u.username,u.linked_player_id FROM comments c JOIN users u ON u.id=c.user_id \
             WHERE c.post_id=$1 ORDER BY c.created_at",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"post":post,"comments":comments}),
    ))
}

async fn edit_permission(
    state: &CommunityState,
    table: &str,
    id: i64,
    session: &Session,
    request_id: &RequestId,
) -> Result<Result<(), Response>, ApiError> {
    let label = if table == "posts" { "Post" } else { "Comment" };
    let row = state
        .database
        .one_json(&format!("SELECT user_id FROM {table} WHERE id=$1"), &[&id])
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let Some(row) = row else {
        return Ok(Err(simple_error(
            StatusCode::NOT_FOUND,
            format!("{label} not found"),
        )));
    };
    if as_i64(row.get("user_id")) != Some(i64::from(session.user_id)) && !session.is_admin {
        return Ok(Err(simple_error(
            StatusCode::FORBIDDEN,
            format!("Not allowed to edit this {}", label.to_ascii_lowercase()),
        )));
    }
    Ok(Ok(()))
}

async fn update_post(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid post id"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if let Err(response) = edit_permission(&state, "posts", id, &session, &request_id).await? {
        return Ok(response);
    }
    let title = text(&body, "title");
    let content = text(&body, "content");
    if title.is_empty() || content.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Title and content are required",
        ));
    }
    let post = state
        .database
        .one_json(
            "UPDATE posts p SET title=$2,content=$3 FROM users u WHERE p.id=$1 AND u.id=p.user_id RETURNING p.*,u.username",
            &[&id, &title, &content],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .unwrap_or(Value::Null);
    Ok(json_response(StatusCode::OK, post))
}

async fn delete_post(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid post id"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if let Err(response) = edit_permission(&state, "posts", id, &session, &request_id).await? {
        return Ok(response);
    }
    state
        .database
        .query_json("DELETE FROM posts WHERE id=$1 RETURNING id", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"deleted":true,"id":id}),
    ))
}

async fn create_comment(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid post id"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let content = text(&body, "content");
    if content.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Comment content is required",
        ));
    }
    let parent_id = body
        .get("parent_id")
        .filter(|value| !value.is_null())
        .and_then(|value| as_i64(Some(value)));
    let post = state
        .database
        .one_json("SELECT id,user_id FROM posts WHERE id=$1", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(post) = post else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Post not found"));
    };
    let mut comment = state
        .database
        .one_json(
            "INSERT INTO comments(post_id,user_id,parent_id,content) VALUES($1,$2,$3,$4) RETURNING *",
            &[&id, &session.user_id, &parent_id, &content],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let comment_id = as_i64(comment.get("id")).unwrap_or_default();
    let owner = as_i64(post.get("user_id")).unwrap_or_default();
    if owner != i64::from(session.user_id) {
        state
            .database
            .query_json(
                "INSERT INTO user_notifications(user_id,actor_user_id,type,post_id,comment_id) \
                 VALUES($1,$2,'community_comment',$3,$4) ON CONFLICT(user_id,comment_id) DO NOTHING",
                &[&owner, &session.user_id, &id, &comment_id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
    }
    if let Some(object) = comment.as_object_mut() {
        object.insert("username".to_owned(), Value::String(session.username));
        object.insert(
            "linked_player_id".to_owned(),
            session
                .linked_player_id
                .map(Value::from)
                .unwrap_or(Value::Null),
        );
    }
    Ok(json_response(StatusCode::OK, comment))
}

async fn update_comment(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid comment id"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if let Err(response) = edit_permission(&state, "comments", id, &session, &request_id).await? {
        return Ok(response);
    }
    let content = text(&body, "content");
    if content.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Comment content is required",
        ));
    }
    let comment = state
        .database
        .one_json(
            "UPDATE comments c SET content=$2 FROM users u WHERE c.id=$1 AND u.id=c.user_id RETURNING c.*,u.username,u.linked_player_id",
            &[&id, &content],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .unwrap_or(Value::Null);
    Ok(json_response(StatusCode::OK, comment))
}

async fn delete_comment(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid comment id"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if let Err(response) = edit_permission(&state, "comments", id, &session, &request_id).await? {
        return Ok(response);
    }
    state
        .database
        .query_json("DELETE FROM comments WHERE id=$1 RETURNING id", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"deleted":true,"id":id}),
    ))
}

async fn like_post(
    State(state): State<CommunityState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid post id"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    if state
        .database
        .one_json("SELECT id FROM posts WHERE id=$1", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .is_none()
    {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Post not found"));
    }
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let existing = transaction
        .query_opt(
            "SELECT 1 FROM user_post_likes WHERE user_id=$1 AND post_id=$2 FOR UPDATE",
            &[&session.user_id, &id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?
        .is_some();
    if existing {
        transaction
            .execute(
                "DELETE FROM user_post_likes WHERE user_id=$1 AND post_id=$2",
                &[&session.user_id, &id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?;
    } else {
        transaction
            .execute(
                "INSERT INTO user_post_likes(user_id,post_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
                &[&session.user_id, &id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?;
    }
    let row = transaction
        .query_one(
            if existing {
                "UPDATE posts SET likes=GREATEST(likes-1,0) WHERE id=$1 RETURNING likes"
            } else {
                "UPDATE posts SET likes=likes+1 WHERE id=$1 RETURNING likes"
            },
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let likes = row.get::<_, i32>("likes");
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"liked":!existing,"likes":likes}),
    ))
}
