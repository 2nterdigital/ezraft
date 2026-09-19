//! Cluster-token authentication for the internal port
//!
//! Every route on the internal port - `/raft/*`, `/api/*`, the RPC prefix, and any route an
//! embedder adds through [`crate::raft::EzKv::serve_with`] - requires
//! `Authorization: Bearer <cluster token>`. [`require_token`] wraps the whole `App`
//! (`crate::server::bind`) with a middleware that checks it before any handler runs: a missing,
//! malformed, or wrong credential answers `401` and never reaches a route. The credential itself
//! is compared in constant time (`subtle`, not `==`), and neither the configured token nor
//! whatever the caller presented is ever logged.

use actix_web::HttpResponse;
use actix_web::body::MessageBody;
use actix_web::dev::Service;
use actix_web::dev::ServiceRequest;
use actix_web::dev::ServiceResponse;
use actix_web::dev::Transform;
use actix_web::http::header::AUTHORIZATION;
use actix_web::middleware::Next;
use actix_web::middleware::from_fn;
use subtle::ConstantTimeEq;

/// The scheme every internal-port request must present the cluster token under
const BEARER_PREFIX: &str = "Bearer ";

/// Build the actix middleware that guards every route it wraps behind `token`
///
/// `token` is this node's [`crate::EzConfig::cluster_token`]. Wrapping the whole `App` with it
/// (rather than individual routes) is what makes the guarantee apply to routes an embedder adds
/// through `serve_with` too, without embedders having to remember to wrap their own.
pub(crate) fn require_token<S, B>(
    token: String,
) -> impl Transform<S, ServiceRequest, Response = ServiceResponse<impl MessageBody>, Error = actix_web::Error, InitError = ()>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = actix_web::Error> + 'static,
    B: MessageBody + 'static,
{
    from_fn(move |req: ServiceRequest, next: Next<B>| {
        let token = token.clone();
        async move {
            if presents_token(&req, &token) {
                Ok(next.call(req).await?.map_into_left_body())
            } else {
                let resp = req.into_response(HttpResponse::Unauthorized().finish());
                Ok(resp.map_into_right_body())
            }
        }
    })
}

/// Whether `req` carries `Authorization: Bearer <token>` for exactly this `token`
///
/// Header presence and shape - whether the `Bearer ` scheme is even there - are checked in plain
/// time: that much is public protocol shape, not secret. Only the credential bytes themselves,
/// once extracted, are compared in constant time.
fn presents_token(req: &ServiceRequest, token: &str) -> bool {
    let Some(value) = req.headers().get(AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some(presented) = value.strip_prefix(BEARER_PREFIX) else {
        return false;
    };

    constant_time_eq(presented.as_bytes(), token.as_bytes())
}

/// Constant-time byte comparison (`subtle`, not `==`)
///
/// A length mismatch still short-circuits (`subtle::ConstantTimeEq`'s own documented behavior for
/// slices): the token's length is not the secret, only its content is.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

#[cfg(test)]
mod tests {
    use actix_web::test::TestRequest;

    use super::*;

    fn request_with_header(value: Option<&str>) -> ServiceRequest {
        let mut req = TestRequest::default();
        if let Some(value) = value {
            req = req.insert_header((AUTHORIZATION, value));
        }
        req.to_srv_request()
    }

    #[test]
    fn no_authorization_header_is_not_authorized() {
        assert!(!presents_token(&request_with_header(None), "secret"));
    }

    #[test]
    fn wrong_scheme_is_not_authorized() {
        assert!(!presents_token(&request_with_header(Some("Basic secret")), "secret"));
    }

    #[test]
    fn wrong_token_is_not_authorized() {
        assert!(!presents_token(&request_with_header(Some("Bearer wrong")), "secret"));
    }

    #[test]
    fn a_strict_prefix_of_the_token_is_not_authorized() {
        assert!(!presents_token(&request_with_header(Some("Bearer sec")), "secret"));
    }

    #[test]
    fn a_longer_string_starting_with_the_token_is_not_authorized() {
        assert!(!presents_token(
            &request_with_header(Some("Bearer secretplus")),
            "secret"
        ));
    }

    #[test]
    fn the_matching_token_is_authorized() {
        assert!(presents_token(&request_with_header(Some("Bearer secret")), "secret"));
    }
}
