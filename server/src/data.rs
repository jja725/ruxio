use anyhow::Result;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

use ruxio_cluster::membership::ClusterMembership;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::{
    BatchReadRequest, BatchScanRequest, ErrorResponse, GetMetadataRequest, MetadataResponse,
    ReadRangeRequest, RedirectResponse, ScanRequest,
};
use ruxio_storage::cache::CacheManager;

/// Serve the binary data plane on the given address.
pub async fn serve_data_plane(
    addr: &str,
    mut cache_manager: CacheManager,
    membership: ClusterMembership,
) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    info!("Data plane listening on {addr}");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        info!("New data plane connection from {peer_addr}");

        if let Err(e) = handle_connection(stream, &mut cache_manager, &membership).await {
            warn!("Connection error from {peer_addr}: {e}");
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    cache_manager: &mut CacheManager,
    membership: &ClusterMembership,
) -> Result<()> {
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        let (result, read_buf) = stream.read(buf).await;
        buf = read_buf;
        let n = result?;
        if n == 0 {
            return Ok(());
        }

        reader.feed(&buf[..n]);

        while let Some(frame) = reader.next_frame()? {
            let response_frames = process_frame(frame, cache_manager, membership).await;
            for resp in response_frames {
                let encoded = resp.encode();
                let (result, _) = stream.write_all(encoded.to_vec()).await;
                result?;
            }
        }
    }
}

async fn process_frame(
    frame: Frame,
    cache_manager: &mut CacheManager,
    membership: &ClusterMembership,
) -> Vec<Frame> {
    let request_id = frame.request_id;

    match frame.msg_type {
        MessageType::ReadRange => {
            let req: ReadRangeRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid read request: {e}"),
                        },
                    )];
                }
            };

            // Check consistent hash ring
            if !membership.is_local(&req.uri) {
                if let Some(owner) = membership.owner(&req.uri) {
                    let parts: Vec<&str> = owner.0.split(':').collect();
                    let host = parts.first().unwrap_or(&"unknown").to_string();
                    let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(8081);
                    return vec![Frame::new_json(
                        MessageType::Redirect,
                        request_id,
                        &RedirectResponse {
                            target_host: host,
                            target_data_port: port,
                        },
                    )];
                }
            }

            // Serve range read
            match cache_manager.read_range(&req).await {
                Ok(data) => {
                    vec![
                        Frame::new_raw(MessageType::DataChunk, request_id, data),
                        Frame::done(request_id),
                    ]
                }
                Err(e) => {
                    vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 500,
                            message: format!("Read failed: {e}"),
                        },
                    )]
                }
            }
        }

        MessageType::BatchRead => {
            let batch_req: BatchReadRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid batch read request: {e}"),
                        },
                    )];
                }
            };

            let mut frames = Vec::new();
            for req in &batch_req.reads {
                match cache_manager.read_range(req).await {
                    Ok(data) => {
                        frames.push(Frame::new_raw(MessageType::DataChunk, request_id, data));
                    }
                    Err(e) => {
                        error!("Batch read error for {} offset {}: {e}", req.uri, req.offset);
                    }
                }
            }
            frames.push(Frame::done(request_id));
            frames
        }

        MessageType::GetMetadata => {
            let req: GetMetadataRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid metadata request: {e}"),
                        },
                    )];
                }
            };

            match cache_manager.get_metadata(&req.uri).await {
                Ok(meta) => {
                    // Send metadata info frame, then raw footer bytes
                    let meta_resp = MetadataResponse {
                        uri: req.uri,
                        file_size: meta.file_size,
                        footer_size: meta.footer_bytes.len() as u64,
                    };
                    vec![
                        Frame::new_json(MessageType::Metadata, request_id, &meta_resp),
                        Frame::new_raw(MessageType::DataChunk, request_id, meta.footer_bytes),
                        Frame::done(request_id),
                    ]
                }
                Err(e) => {
                    vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 500,
                            message: format!("Metadata fetch failed: {e}"),
                        },
                    )]
                }
            }
        }

        MessageType::Scan => {
            let req: ScanRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid scan request: {e}"),
                        },
                    )];
                }
            };

            // Check consistent hash ring
            if !membership.is_local(&req.uri) {
                if let Some(owner) = membership.owner(&req.uri) {
                    let parts: Vec<&str> = owner.0.split(':').collect();
                    let host = parts.first().unwrap_or(&"unknown").to_string();
                    let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(8081);
                    return vec![Frame::new_json(
                        MessageType::Redirect,
                        request_id,
                        &RedirectResponse {
                            target_host: host,
                            target_data_port: port,
                        },
                    )];
                }
            }

            match cache_manager.scan(&req).await {
                Ok(pages) => {
                    let mut frames = Vec::with_capacity(pages.len() + 1);
                    for page_data in pages {
                        frames.push(Frame::new_raw(MessageType::DataChunk, request_id, page_data));
                    }
                    frames.push(Frame::done(request_id));
                    frames
                }
                Err(e) => {
                    vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 500,
                            message: format!("Scan failed: {e}"),
                        },
                    )]
                }
            }
        }

        MessageType::BatchScan => {
            let batch_req: BatchScanRequest = match serde_json::from_slice(&frame.payload) {
                Ok(r) => r,
                Err(e) => {
                    return vec![Frame::new_json(
                        MessageType::Error,
                        request_id,
                        &ErrorResponse {
                            code: 400,
                            message: format!("Invalid batch scan request: {e}"),
                        },
                    )];
                }
            };

            let mut frames = Vec::new();
            for req in &batch_req.scans {
                match cache_manager.scan(req).await {
                    Ok(pages) => {
                        for data in pages {
                            frames.push(Frame::new_raw(MessageType::DataChunk, request_id, data));
                        }
                    }
                    Err(e) => {
                        error!("Batch scan error for {}: {e}", req.uri);
                    }
                }
            }
            frames.push(Frame::done(request_id));
            frames
        }

        _ => {
            vec![Frame::new_json(
                MessageType::Error,
                request_id,
                &ErrorResponse {
                    code: 400,
                    message: format!("Unexpected message type: {:?}", frame.msg_type),
                },
            )]
        }
    }
}
