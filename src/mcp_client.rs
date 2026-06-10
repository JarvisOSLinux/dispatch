use crate::error::{DispatchError, Result};
use tokio::process::Command;
use tracing::{debug, warn};

/// Client for invoking dmcp commands.
/// dispatch delegates all MCP server management to dmcp.
pub struct DmcpClient;

impl DmcpClient {
    /// Check that dmcp is available on PATH.
    pub async fn check_available() -> Result<()> {
        let output = Command::new("dmcp")
            .arg("paths")
            .output()
            .await
            .map_err(|_| DispatchError::DmcpNotFound)?;

        if !output.status.success() {
            return Err(DispatchError::DmcpNotFound);
        }
        Ok(())
    }

    /// Call a tool on an MCP server via `dmcp call <server> <tool> --args <json>`.
    /// Returns the stdout output as a string.
    pub async fn call_tool(server: &str, tool: &str, params: &serde_json::Value) -> Result<String> {
        debug!(server, tool, "calling dmcp tool");
        let mut cmd = Command::new("dmcp");
        cmd.arg("call").arg(server).arg(tool);

        if !params.is_null() && params != &serde_json::json!({}) {
            cmd.arg("--args").arg(params.to_string());
        }

        let output = cmd.output().await.map_err(|e| {
            warn!(server, tool, error = %e, "failed to spawn dmcp");
            DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e))
        })?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            debug!(server, tool, "dmcp call succeeded");
            Ok(stdout.trim().to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let msg = if stderr.is_empty() { stdout } else { stderr };
            warn!(server, tool, error = %msg.trim(), "dmcp call failed");
            Err(DispatchError::DmcpError(msg.trim().to_string()))
        }
    }

    /// Browse servers via `dmcp browse -k <keywords> --json`.
    pub async fn browse(keywords: &[String]) -> Result<serde_json::Value> {
        let mut cmd = Command::new("dmcp");
        cmd.arg("browse").arg("--json");
        for kw in keywords {
            cmd.arg("-k").arg(kw);
        }

        let output = cmd
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp browse: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Single vector search via `dmcp browse --vector <json> --top-k N --min-score F --json`.
    pub async fn browse_vector(
        vector: &[f64],
        top_k: u64,
        min_score: f64,
    ) -> Result<serde_json::Value> {
        let vec_json = serde_json::to_string(vector)
            .map_err(|e| DispatchError::DmcpError(format!("failed to serialize vector: {}", e)))?;
        let output = Command::new("dmcp")
            .arg("browse")
            .arg("--vector")
            .arg(&vec_json)
            .arg("--top-k")
            .arg(top_k.to_string())
            .arg("--min-score")
            .arg(min_score.to_string())
            .arg("--json")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp browse: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Batch vector search via `dmcp browse --vectors <json> --top-k N --min-score F --json`.
    pub async fn browse_vectors(
        vectors: &[Vec<f64>],
        top_k: u64,
        min_score: f64,
    ) -> Result<serde_json::Value> {
        let vecs_json = serde_json::to_string(vectors)
            .map_err(|e| DispatchError::DmcpError(format!("failed to serialize vectors: {}", e)))?;
        let output = Command::new("dmcp")
            .arg("browse")
            .arg("--vectors")
            .arg(&vecs_json)
            .arg("--top-k")
            .arg(top_k.to_string())
            .arg("--min-score")
            .arg(min_score.to_string())
            .arg("--json")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp browse: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Get visible server count via `dmcp count`.
    pub async fn server_count() -> Result<u64> {
        let output = Command::new("dmcp")
            .arg("count")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout
                .trim()
                .parse::<u64>()
                .map_err(|e| DispatchError::DmcpError(format!("invalid count from dmcp: {}", e)))
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Get the registry's embedding model spec via `dmcp embedding-spec`.
    pub async fn embedding_spec() -> Result<serde_json::Value> {
        let output = Command::new("dmcp")
            .arg("embedding-spec")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp embedding-spec: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Refresh the local vector index via `dmcp sync-index`.
    pub async fn sync_index() -> Result<String> {
        let output = Command::new("dmcp")
            .arg("sync-index")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// Index a non-approved server via `dmcp index-server <id> --vectors <json>`.
    pub async fn index_server(server_id: &str, vectors: &[Vec<f64>]) -> Result<String> {
        let vecs_json = serde_json::to_string(vectors)
            .map_err(|e| DispatchError::DmcpError(format!("failed to serialize vectors: {}", e)))?;
        let output = Command::new("dmcp")
            .arg("index-server")
            .arg(server_id)
            .arg("--vectors")
            .arg(&vecs_json)
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }

    /// List tools for a server via `dmcp tools <id> --json`.
    pub async fn list_tools(server: &str) -> Result<serde_json::Value> {
        let output = Command::new("dmcp")
            .arg("tools")
            .arg(server)
            .arg("--json")
            .output()
            .await
            .map_err(|e| DispatchError::DmcpError(format!("failed to spawn dmcp: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        if output.status.success() {
            serde_json::from_str(stdout.trim()).map_err(|e| {
                DispatchError::DmcpError(format!("invalid JSON from dmcp tools: {}", e))
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(DispatchError::DmcpError(stderr.trim().to_string()))
        }
    }
}
