use ruxio_cluster::ring::NodeId;
use ruxio_protocol::frame::Frame;

use crate::connection::ConnectionPool;
use crate::error::{self, NoServerAvailableSnafu, RetriesExhaustedSnafu};
use crate::membership::ClientMembership;
use crate::response::Response;

use crate::error::ServerSnafu;
/// Route a request to the correct server and handle retries/redirects.
///
/// Flow:
/// 1. Hash the URI to find the owning node via consistent hash ring
/// 2. Send the request to that node
/// 3. On success: return the response
/// 4. On redirect: follow the redirect target (stale ring)
/// 5. On retriable error: invalidate connection, retry
/// 6. On non-retriable error: return immediately
pub(crate) async fn route_and_execute(
    uri: &str,
    frame: Frame,
    membership: &ClientMembership,
    pool: &ConnectionPool,
    max_retries: u32,
) -> error::Result<Response> {
    let mut target_override: Option<NodeId> = None;

    for _attempt in 0..=max_retries {
        // Pick target: explicit override (from redirect) or hash ring
        let node = if let Some(ref target) = target_override {
            target.clone()
        } else {
            match membership.owner(uri) {
                Some(n) => n,
                None => {
                    return Err(NoServerAvailableSnafu {
                        uri: uri.to_string(),
                    }
                    .build())
                }
            }
        };

        // Re-encode the frame for each attempt (Frame is consumed by encode)
        let request_frame = Frame {
            msg_type: frame.msg_type,
            request_id: frame.request_id,
            payload: frame.payload.clone(),
        };

        match pool.send_to(&node, request_frame).await {
            Ok(Response::Redirect { host, port }) => {
                // Server says we have a stale ring — follow the redirect
                tracing::debug!("Redirect for {uri} → {host}:{port}");
                target_override = Some(NodeId::new(host, port));
                continue;
            }
            Ok(Response::Error {
                error_code,
                message,
            }) => {
                if error_code.retriable {
                    tracing::debug!("Retriable error for {uri} on {node}: {message}, retrying");
                    pool.invalidate(&node);
                    target_override = None; // go back to hash ring
                    continue;
                }
                return Err(ServerSnafu {
                    error_code,
                    message,
                }
                .build());
            }
            Ok(response) => return Ok(response),
            Err(e) => {
                tracing::debug!("Connection error for {uri} on {node}: {e}, retrying");
                pool.invalidate(&node);
                target_override = None;
                continue;
            }
        }
    }

    Err(RetriesExhaustedSnafu {
        uri: uri.to_string(),
        max: max_retries,
    }
    .build())
}
