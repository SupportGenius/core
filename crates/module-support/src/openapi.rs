//! The `OpenAPI` 3.1 document `GET /v1/support/openapi.json` serves (issue
//! #34).
//!
//! It is built from the modules' declared [`Surface`]s — the same
//! declaration `GET /__surface` composes and every route here is checked
//! against — so a route declared in a `surface()` appears here and a route
//! that is not, does not. Nothing is discovered from the `axum::Router`
//! (an axum router is opaque); the coverage is a test's job, over the same
//! declarations.

use cratefield_core::{Action, RoutePolicy, Surface};
use http::Method;
use serde_json::{Map, Value, json};

/// Builds the document from `(module name, surface)` pairs. Each action
/// mounts at `/v1/<module><action.path>`, which is axum's own nesting and
/// the `{param}` path syntax `OpenAPI` already shares.
pub(crate) fn document(modules: &[(String, Surface)]) -> Value {
    let mut paths = Map::new();
    for (module, surface) in modules {
        for action in &surface.actions {
            let path = format!("/v1/{module}{}", action.path);
            let operation = operation(module, action);
            let entry = paths
                .entry(path)
                .or_insert_with(|| Value::Object(Map::new()));
            if let Value::Object(methods) = entry {
                methods.insert(action.method.as_str().to_lowercase(), operation);
            }
        }
    }
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "SupportGenius API",
            "version": env!("CARGO_PKG_VERSION"),
        },
        "paths": paths,
        "components": {
            "securitySchemes": {
                "tenantKey": { "type": "http", "scheme": "bearer" },
            },
        },
    })
}

/// One action's operation object: path parameters, a `requestBody` (or
/// `GET` query parameters) from its input schema, the tenant-key security
/// where the route demands it, and its success responses.
fn operation(module: &str, action: &Action) -> Value {
    let mut parameters: Vec<Value> = Vec::new();
    for name in path_params(&format!("/v1/{module}{}", action.path)) {
        parameters.push(json!({
            "name": name,
            "in": "path",
            "required": true,
            "schema": { "type": "string" },
        }));
    }

    let mut request_body = None;
    if let Some(schema) = &action.input {
        let schema = serde_json::to_value(schema).unwrap_or(Value::Null);
        if action.method == Method::GET {
            // A `GET` takes its input as query parameters; each is only as
            // precise as the field's own schema, which is enough for a
            // contract reader. A field is required exactly when the schema
            // lists it, so an `Option` field stays optional.
            let required = required_fields(&schema);
            if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
                for (name, field) in properties {
                    parameters.push(json!({
                        "name": name,
                        "in": "query",
                        "required": required.contains(&name.as_str()),
                        "schema": field,
                    }));
                }
            }
        } else {
            request_body = Some(json!({
                "required": true,
                "content": { "application/json": { "schema": schema } },
            }));
        }
    }

    let mut operation = Map::new();
    operation.insert(
        "operationId".to_owned(),
        json!(format!("{module}_{}", action.name.replace('-', "_"))),
    );
    operation.insert("summary".to_owned(), json!(action.name));
    if !parameters.is_empty() {
        operation.insert("parameters".to_owned(), json!(parameters));
    }
    if let Some(request_body) = request_body {
        operation.insert("requestBody".to_owned(), request_body);
    }
    if action.policy == RoutePolicy::ApiKey {
        operation.insert("security".to_owned(), json!([{ "tenantKey": [] }]));
    }

    let success = success(action);
    let mut responses = Map::new();
    let mut ok = Map::new();
    ok.insert("description".to_owned(), json!("Success"));
    let media = success.media.unwrap_or(APPLICATION_JSON);
    if let Some(output) = &action.output {
        ok.insert(
            "content".to_owned(),
            json!({
                media: {
                    "schema": serde_json::to_value(output).unwrap_or(Value::Null),
                },
            }),
        );
    } else if success.media.is_some() {
        // A body with a known media type but no schema (the widget script).
        ok.insert("content".to_owned(), json!({ media: {} }));
    }
    responses.insert(success.status.to_owned(), Value::Object(ok));
    if let Some(created) = success.also {
        responses.insert(created.to_owned(), json!({ "description": "Created" }));
    }
    operation.insert("responses".to_owned(), Value::Object(responses));

    Value::Object(operation)
}

/// The media type of every body unless an action says otherwise.
const APPLICATION_JSON: &str = "application/json";

/// What an action really answers on success. Core's surface vocabulary —
/// [`Outcome`](cratefield_core::Outcome) — names a response *shape*
/// (`Json`, `Accepted`, `Redirect`) and carries neither a status code nor
/// a media type, so a route whose answer is not the default `200
/// application/json` cannot say so in its surface; [`success`] lists them
/// (issue #34), each read from the handler it describes.
struct Success {
    /// The primary success status.
    status: &'static str,
    /// Media type of the primary body; `None` is the default — JSON when
    /// the action declares an output schema, no body otherwise.
    media: Option<&'static str>,
    /// A second success status carrying no body (a create's `201`).
    also: Option<&'static str>,
}

impl Success {
    /// The default: `200`, JSON.
    const OK: Self = Self {
        status: "200",
        media: None,
        also: None,
    };
}

/// The success answers that differ from the default.
fn success(action: &Action) -> Success {
    match action.name.as_str() {
        // A create answers `201 Created` (a tenant, key, connector, upload
        // or publishable key).
        "create-tenant"
        | "create-key"
        | "create-connector"
        | "create-upload"
        | "create-publishable-key" => Success {
            status: "201",
            ..Success::OK
        },
        // `ingest-source` answers `201` on a create, `200` when a repeated
        // `external_id` replaces an existing source.
        "ingest-source" => Success {
            also: Some("201"),
            ..Success::OK
        },
        // Upload completion is asynchronous: `202 Accepted`, no body.
        "complete-upload" => Success {
            status: "202",
            ..Success::OK
        },
        // Deletes answer `204 No Content`, no body.
        "delete-source" | "delete-key" | "delete-destinations" | "delete-admin-destinations" => {
            Success {
                status: "204",
                ..Success::OK
            }
        }
        // The widget script is served as JavaScript, not JSON.
        "w-js" => Success {
            media: Some("application/javascript"),
            ..Success::OK
        },
        _ => Success::OK,
    }
}

/// The property names a schema lists as `required`.
fn required_fields(schema: &Value) -> Vec<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

/// The `{name}` segments of a path, in order.
fn path_params(path: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = path;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            break;
        };
        names.push(after[..close].to_owned());
        rest = &after[close + 1..];
    }
    names
}

#[cfg(test)]
mod tests {
    use schemars::JsonSchema;

    use super::*;

    /// A `GET` input: one required field, one optional.
    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Params {
        query: String,
        limit: Option<u32>,
    }

    /// The answers the document used to get wrong: a `GET`'s query
    /// parameters carry the schema's `required` list, `ingest-source`
    /// answers `201` as well as `200`, and a route whose real success
    /// status is not `200` says so (the `202` upload, the `204` delete).
    #[test]
    fn documents_required_query_fields_and_the_real_success_statuses() {
        let surface = Surface::new()
            .action(Action::get("search", "/search").input::<Params>())
            .action(Action::post("ingest-source", "/sources").policy(RoutePolicy::ApiKey))
            .action(Action::post(
                "complete-upload",
                "/uploads/{upload_id}/complete",
            ))
            .action(Action::delete("delete-source", "/sources/{source_id}"));
        let document = document(&[("support".to_owned(), surface)]);

        let parameters = document["paths"]["/v1/support/search"]["get"]["parameters"]
            .as_array()
            .expect("query parameters");
        let required = |name: &str| {
            parameters
                .iter()
                .find(|parameter| parameter["name"] == name)
                .expect("the parameter is documented")["required"]
                .clone()
        };
        assert_eq!(required("query"), json!(true));
        assert_eq!(required("limit"), json!(false));

        let create = &document["paths"]["/v1/support/sources"]["post"]["responses"];
        assert!(create["200"].is_object(), "the replace answers 200");
        assert!(create["201"].is_object(), "the create answers 201 too");

        let upload =
            &document["paths"]["/v1/support/uploads/{upload_id}/complete"]["post"]["responses"];
        assert!(upload["202"].is_object(), "completion is accepted, not ok");
        assert!(upload.get("200").is_none(), "and is not also a 200");

        let delete = &document["paths"]["/v1/support/sources/{source_id}"]["delete"]["responses"];
        assert!(delete["204"].is_object(), "a delete has no content");
        assert!(delete.get("200").is_none());
    }
}
