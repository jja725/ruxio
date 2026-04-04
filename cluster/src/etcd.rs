// etcd-based membership implementation.
// See Step 1c for the full implementation.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::ring::NodeId;
use crate::service::{MembershipError, MembershipEvent, MembershipService};

/// Configuration for etcd-based membership.
#[derive(Debug, Clone)]
pub struct EtcdConfig {
    /// etcd server endpoints (e.g., `["http://localhost:2379"]`).
    pub endpoints: Vec<String>,
    /// Key prefix for node registration (default: `/ruxio/nodes/`).
    pub prefix: String,
    /// Lease TTL in seconds (default: 10).
    pub lease_ttl_secs: u64,
}

impl Default for EtcdConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["http://localhost:2379".to_string()],
            prefix: "/ruxio/nodes/".to_string(),
            lease_ttl_secs: 10,
        }
    }
}

/// Command sent from monoio threads to the tokio background thread.
enum EtcdCommand {
    Join {
        reply: mpsc::Sender<Result<(), MembershipError>>,
    },
    Leave {
        reply: mpsc::Sender<Result<(), MembershipError>>,
    },
    GetMembers {
        reply: mpsc::Sender<Result<Vec<NodeId>, MembershipError>>,
    },
    Shutdown,
}

/// etcd-based membership service.
///
/// Runs a dedicated background thread with a tokio runtime for etcd operations.
/// Communication between monoio workers and the tokio thread uses `std::sync::mpsc`.
pub struct EtcdMembership {
    self_id: NodeId,
    config: EtcdConfig,
    cmd_tx: mpsc::Sender<EtcdCommand>,
    /// Senders for broadcasting membership events to subscribers.
    subscribers: Arc<Mutex<Vec<mpsc::Sender<MembershipEvent>>>>,
    /// Background thread handle.
    _bg_handle: std::thread::JoinHandle<()>,
}

impl EtcdMembership {
    pub fn new(self_id: NodeId, config: EtcdConfig) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<EtcdCommand>();
        let subscribers: Arc<Mutex<Vec<mpsc::Sender<MembershipEvent>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let subscribers_bg = subscribers.clone();
        let self_id_bg = self_id.clone();
        let config_bg = config.clone();

        let bg_handle = std::thread::Builder::new()
            .name("ruxio-etcd".to_string())
            .spawn(move || {
                Self::background_thread(self_id_bg, config_bg, cmd_rx, subscribers_bg);
            })
            .expect("failed to spawn etcd background thread");

        Self {
            self_id,
            config,
            cmd_tx,
            subscribers,
            _bg_handle: bg_handle,
        }
    }

    fn background_thread(
        self_id: NodeId,
        config: EtcdConfig,
        cmd_rx: mpsc::Receiver<EtcdCommand>,
        subscribers: Arc<Mutex<Vec<mpsc::Sender<MembershipEvent>>>>,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for etcd");

        rt.block_on(async move {
            let client = match etcd_client::Client::connect(&config.endpoints, None).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("Failed to connect to etcd: {e}");
                    // Drain commands with errors
                    while let Ok(cmd) = cmd_rx.recv() {
                        match cmd {
                            EtcdCommand::Join { reply } => {
                                let _ = reply
                                    .send(Err(MembershipError::ConnectionFailed(e.to_string())));
                                return;
                            }
                            EtcdCommand::Leave { reply } => {
                                let _ = reply.send(Ok(()));
                                return;
                            }
                            EtcdCommand::GetMembers { reply } => {
                                let _ = reply
                                    .send(Err(MembershipError::ConnectionFailed(e.to_string())));
                            }
                            EtcdCommand::Shutdown => return,
                        }
                    }
                    return;
                }
            };

            let mut client = client;
            let mut lease_id: Option<i64> = None;
            let mut keepalive_handle: Option<tokio::task::JoinHandle<()>> = None;

            // Spawn watch task
            let watch_prefix = config.prefix.clone();
            let subscribers_watch = subscribers.clone();
            let mut watch_client = client.clone();
            tokio::spawn(async move {
                let options = etcd_client::WatchOptions::new().with_prefix();
                match watch_client
                    .watch(watch_prefix.as_bytes(), Some(options))
                    .await
                {
                    Ok((_watcher, mut watch_stream)) => {
                        while let Some(resp) = watch_stream.message().await.unwrap_or(None) {
                            for event in resp.events() {
                                let kv = match event.kv() {
                                    Some(kv) => kv,
                                    None => continue,
                                };
                                let key_str = std::str::from_utf8(kv.key()).unwrap_or_default();
                                // Extract node_id from key: prefix + node_id
                                let node_id_str =
                                    key_str.strip_prefix(&watch_prefix).unwrap_or(key_str);
                                let node_id = NodeId(node_id_str.to_string());

                                let event = match event.event_type() {
                                    etcd_client::EventType::Put => MembershipEvent::Joined(node_id),
                                    etcd_client::EventType::Delete => {
                                        MembershipEvent::Left(node_id)
                                    }
                                };

                                let subs = subscribers_watch.lock().unwrap();
                                for tx in subs.iter() {
                                    let _ = tx.send(event.clone());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("etcd watch failed: {e}");
                    }
                }
            });

            // Process commands
            loop {
                let cmd = match cmd_rx.recv() {
                    Ok(cmd) => cmd,
                    Err(_) => break, // All senders dropped
                };

                match cmd {
                    EtcdCommand::Join { reply } => {
                        let result = Self::do_join(
                            &mut client,
                            &self_id,
                            &config,
                            &mut lease_id,
                            &mut keepalive_handle,
                        )
                        .await;
                        let _ = reply.send(result);
                    }
                    EtcdCommand::Leave { reply } => {
                        let result =
                            Self::do_leave(&mut client, &mut lease_id, &mut keepalive_handle).await;
                        let _ = reply.send(result);
                    }
                    EtcdCommand::GetMembers { reply } => {
                        let result = Self::do_get_members(&mut client, &config).await;
                        let _ = reply.send(result);
                    }
                    EtcdCommand::Shutdown => {
                        // Revoke lease if active
                        if let Some(lid) = lease_id.take() {
                            let _ = client.lease_revoke(lid).await;
                        }
                        if let Some(h) = keepalive_handle.take() {
                            h.abort();
                        }
                        break;
                    }
                }
            }
        });
    }

    async fn do_join(
        client: &mut etcd_client::Client,
        self_id: &NodeId,
        config: &EtcdConfig,
        lease_id: &mut Option<i64>,
        keepalive_handle: &mut Option<tokio::task::JoinHandle<()>>,
    ) -> Result<(), MembershipError> {
        // Grant lease
        let lease = client
            .lease_grant(config.lease_ttl_secs as i64, None)
            .await
            .map_err(|e| MembershipError::ConnectionFailed(format!("lease_grant: {e}")))?;
        let lid = lease.id();
        *lease_id = Some(lid);

        // Put key with lease
        let key = format!("{}{}", config.prefix, self_id.0);
        let value = serde_json::to_string(&serde_json::json!({
            "id": self_id.0,
            "joined_at": chrono::Utc::now().to_rfc3339(),
        }))
        .unwrap_or_else(|_| format!(r#"{{"id":"{}"}}"#, self_id.0));

        let put_options = etcd_client::PutOptions::new().with_lease(lid);
        client
            .put(key.as_bytes(), value.as_bytes(), Some(put_options))
            .await
            .map_err(|e| MembershipError::ConnectionFailed(format!("put: {e}")))?;

        // Start keepalive
        let (mut keeper, mut stream) = client
            .lease_keep_alive(lid)
            .await
            .map_err(|e| MembershipError::ConnectionFailed(format!("keepalive: {e}")))?;

        let keepalive_interval = Duration::from_secs(config.lease_ttl_secs / 3);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(keepalive_interval).await;
                if let Err(e) = keeper.keep_alive().await {
                    tracing::warn!("etcd keepalive failed: {e}");
                    break;
                }
                // Drain response
                match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
                    Ok(Ok(Some(_))) => {}
                    Ok(Ok(None)) => {
                        tracing::warn!("etcd keepalive stream ended");
                        break;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("etcd keepalive response error: {e}");
                        break;
                    }
                    Err(_) => {
                        tracing::warn!("etcd keepalive response timeout");
                    }
                }
            }
        });
        *keepalive_handle = Some(handle);

        tracing::info!("Registered with etcd: key={key}, lease_id={lid}");
        Ok(())
    }

    async fn do_leave(
        client: &mut etcd_client::Client,
        lease_id: &mut Option<i64>,
        keepalive_handle: &mut Option<tokio::task::JoinHandle<()>>,
    ) -> Result<(), MembershipError> {
        if let Some(h) = keepalive_handle.take() {
            h.abort();
        }
        if let Some(lid) = lease_id.take() {
            client
                .lease_revoke(lid)
                .await
                .map_err(|e| MembershipError::ConnectionFailed(format!("lease_revoke: {e}")))?;
            tracing::info!("Deregistered from etcd: lease_id={lid}");
        }
        Ok(())
    }

    async fn do_get_members(
        client: &mut etcd_client::Client,
        config: &EtcdConfig,
    ) -> Result<Vec<NodeId>, MembershipError> {
        let options = etcd_client::GetOptions::new().with_prefix();
        let resp = client
            .get(config.prefix.as_bytes(), Some(options))
            .await
            .map_err(|e| MembershipError::ConnectionFailed(format!("get: {e}")))?;

        let mut members = Vec::new();
        for kv in resp.kvs() {
            let key_str = std::str::from_utf8(kv.key()).unwrap_or_default();
            let node_id_str = key_str.strip_prefix(&config.prefix).unwrap_or(key_str);
            if !node_id_str.is_empty() {
                members.push(NodeId(node_id_str.to_string()));
            }
        }
        Ok(members)
    }

    fn send_cmd(&self, cmd: EtcdCommand) {
        let _ = self.cmd_tx.send(cmd);
    }
}

impl MembershipService for EtcdMembership {
    fn join(&self) -> Result<(), MembershipError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_cmd(EtcdCommand::Join { reply: reply_tx });
        reply_rx
            .recv()
            .map_err(|_| MembershipError::Internal("bg thread died".to_string()))?
    }

    fn leave(&self) -> Result<(), MembershipError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_cmd(EtcdCommand::Leave { reply: reply_tx });
        reply_rx
            .recv()
            .map_err(|_| MembershipError::Internal("bg thread died".to_string()))?
    }

    fn get_live_members(&self) -> Result<Vec<NodeId>, MembershipError> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.send_cmd(EtcdCommand::GetMembers { reply: reply_tx });
        reply_rx
            .recv()
            .map_err(|_| MembershipError::Internal("bg thread died".to_string()))?
    }

    fn subscribe(&self) -> mpsc::Receiver<MembershipEvent> {
        let (tx, rx) = mpsc::channel();
        self.subscribers.lock().unwrap().push(tx);
        rx
    }

    fn self_id(&self) -> &NodeId {
        &self.self_id
    }
}

impl Drop for EtcdMembership {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(EtcdCommand::Shutdown);
    }
}
