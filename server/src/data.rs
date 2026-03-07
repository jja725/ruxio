use std::cell::RefCell;
use std::rc::Rc;

use anyhow::Result;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;
use tracing::{error, info, warn};

use ruxio_cluster::membership::ClusterMembership;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::{
    BatchReadRequest, BatchScanRequest, ErrorResponse, GetMetadataRequest, MetadataResponse,
    ReadRangeRequest, RedirectResponse, ScanRequest,
};
use ruxio_storage::cache::CacheManager;
use ruxio_storage::zero_copy;

/// Handle a single TCP connection.
pub async fn serve_connection(
    mut stream: TcpStream,
    cache_manager: Rc<RefCell<CacheManager>>,
    membership: Rc<ClusterMembership>,
) {
    let mut reader = FrameReader::new();
    let mut buf = vec![0u8; 128 * 1024];

    loop {
        let (result, read_buf) = stream.read(buf).await;
        buf = read_buf;
        let n = match result {
            Ok(0) => return,
            Ok(n) => n,
            Err(e) => {
                warn!("Read error: {e}");
                return;
            }
        };
        reader.feed(&buf[..n]);

        while let Some(frame) = reader.next_frame().unwrap() {
            let response_frames =
                process_frame(frame, &cache_manager, &membership, &mut stream).await;
            for resp in response_frames {
                let encoded = resp.encode();
                let (result, _) = stream.write_all(encoded.to_vec()).await;
                if result.is_err() {
                    return;
                }
            }
        }
    }
}

async fn process_frame(
    frame: Frame,
    cache_manager: &Rc<RefCell<CacheManager>>,
    membership: &Rc<ClusterMembership>,
    stream: &mut TcpStream,
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

            // Zero-copy path: sendfile for aligned 4MB reads
            let page_offset = (req.offset / (4 * 1024 * 1024)) * (4 * 1024 * 1024);
            if req.length == 4 * 1024 * 1024 && req.offset == page_offset {
                let file_info = cache_manager
                    .borrow_mut()
                    .get_page_file(&req.uri, page_offset);
                if let Some((file_path, file_size)) = file_info {
                    if zero_copy::send_file_to_socket(&file_path, file_size, request_id, stream)
                        .await
                        .is_ok()
                    {
                        return vec![];
                    }
                }
            }

            // Buffered path
            let result = cache_manager.borrow_mut().read_range(&req).await;
            match result {
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
                let result = cache_manager.borrow_mut().read_range(req).await;
                match result {
                    Ok(data) => {
                        frames.push(Frame::new_raw(MessageType::DataChunk, request_id, data));
                    }
                    Err(e) => {
                        error!(
                            "Batch read error for {} offset {}: {e}",
                            req.uri, req.offset
                        );
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

            let result = cache_manager.borrow_mut().get_metadata(&req.uri).await;
            match result {
                Ok(meta) => {
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

            let result = cache_manager.borrow_mut().scan(&req).await;
            match result {
                Ok(pages) => {
                    let mut frames = Vec::with_capacity(pages.len() + 1);
                    for page_data in pages {
                        frames.push(Frame::new_raw(
                            MessageType::DataChunk,
                            request_id,
                            page_data,
                        ));
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
                let result = cache_manager.borrow_mut().scan(req).await;
                match result {
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
