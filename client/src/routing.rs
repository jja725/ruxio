use ruxio_cluster::ring::NodeId;
use ruxio_protocol::frame::Frame;

use crate::connection::ConnectionPool;
use crate::error::{self, NoServerAvailableSnafu, RetriesExhaustedSnafu};
use crate::membership::ClientMembership;
use crate::response::Response;

use crate::error::ServerSnafu;

/// Route a request to the correct server and handle retries/redirects.
///
/// Uses failed-worker blacklisting (like Alluxio's retry logic): on failure,
/// the failed node is blacklisted and the next candidate from the hash ring
/// is tried. On redirect, the redirect target is used directly.
pub(crate) async fn route_and_execute(
    uri: &str,
    frame: Frame,
    membership: &ClientMembership,
    pool: &ConnectionPool,
    max_retries: u32,
) -> error::Result<Response> {
    let mut target_override: Option<NodeId> = None;
    let mut failed_nodes: Vec<NodeId> = Vec::new();

    for _attempt in 0..=max_retries {
        // Pick target: explicit override (from redirect) or hash ring
        let node = if let Some(ref target) = target_override {
            target.clone()
        } else {
            match pick_node(membership, uri, &failed_nodes) {
                Some(n) => n,
                None => {
                    return Err(NoServerAvailableSnafu {
                        uri: uri.to_string(),
                    }
                    .build())
                }
            }
        };

        let request_frame = Frame {
            msg_type: frame.msg_type,
            request_id: frame.request_id,
            payload: frame.payload.clone(),
        };

        match pool.send_to(&node, request_frame).await {
            Ok(Response::Redirect { host, port }) => {
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
                    failed_nodes.push(node);
                    target_override = None;
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
                failed_nodes.push(node);
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

/// Pick the best available node, skipping any that have already failed.
///
/// Uses `get_nodes()` to get multiple candidates from the hash ring,
/// then returns the first one not in the blacklist. Falls back to the
/// least recently failed node if all candidates have failed.
fn pick_node(membership: &ClientMembership, uri: &str, failed: &[NodeId]) -> Option<NodeId> {
    if failed.is_empty() {
        return membership.owner(uri);
    }

    // Get multiple candidates from the ring — try up to the total node count
    let candidates = membership.candidates(uri, failed.len() + 1);
    candidates.into_iter().find(|n| !failed.contains(n))
}
