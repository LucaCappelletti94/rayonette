//! Boot-time node role (PLAN.md R2).
//!
//! When a process starts in agent mode it reads its own children file and runs
//! as a leaf (serving tasks over stdio) or, if it names any children, as a relay
//! over them: it ships the source bundle it was built from down to each child and
//! coordinates them. The child list lives only on the node (decentralization), so
//! nothing about a relay's subtree is configured upstream.

use std::io;
use std::path::{Path, PathBuf};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::agent::{serve, Registry};
use crate::fleet::Launch;
use crate::framing::Connection;
use crate::process::agent_connection;
use crate::relay::{relay_with_source, ChildSource};
use crate::ssh::{parse_host_list, Ssh, SshConfig};

/// What a node needs at boot.
///
/// The `registry` runs tasks when the node is a leaf; `source`, `binary_name`,
/// and `toolchain` are the build inputs a relay cascades to its children (a relay
/// re-ships the very `__rayonet_source()` bundle it was itself built from, so the
/// content-addressed cache stays consistent down the tree).
#[derive(Debug)]
pub struct NodeConfig {
    /// The task handlers this node serves as a leaf.
    registry: Registry,
    /// The crate source tarball to ship to children (a relay's `__rayonet_source()`).
    source: Vec<u8>,
    /// The agent binary name to build on children.
    binary_name: String,
    /// The rust toolchain to build children with.
    toolchain: String,
}

impl NodeConfig {
    /// Assemble a node's boot configuration from its leaf task `registry`, the
    /// crate `source` tarball a relay cascades to its children, and the
    /// `binary_name` and `toolchain` those children are built with.
    #[must_use]
    pub const fn new(
        registry: Registry,
        source: Vec<u8>,
        binary_name: String,
        toolchain: String,
    ) -> Self {
        Self {
            registry,
            source,
            binary_name,
            toolchain,
        }
    }
}

/// The children file path: `$RAYONET_CHILDREN` if set, else
/// `$HOME/.config/rayonet/children`.
fn children_path() -> Option<PathBuf> {
    if let Some(path) = std::env::var_os("RAYONET_CHILDREN") {
        return Some(PathBuf::from(path));
    }
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/rayonet/children"))
}

/// Read `path` as a host list, treating an absent or unreadable file as no
/// children (a pure leaf).
fn read_children_file(path: &Path) -> Vec<SshConfig> {
    std::fs::read_to_string(path).map_or_else(|_| Vec::new(), |body| parse_host_list(&body))
}

/// This node's children, read from its children file (empty if it has none).
#[must_use]
pub fn load_children() -> Vec<SshConfig> {
    children_path().map_or_else(Vec::new, |path| read_children_file(&path))
}

/// Build one ssh `build`-launcher per child, each shipping the node's source.
fn child_launchers(children: Vec<SshConfig>, config: &NodeConfig) -> Vec<Ssh> {
    children
        .into_iter()
        .map(|child| {
            Ssh::build(
                child,
                config.source.clone(),
                config.toolchain.clone(),
                config.binary_name.clone(),
            )
        })
        .collect()
}

/// A [`ChildSource`] backed by the node's children file: each poll re-reads the
/// file and yields ssh launchers for entries not already in the subtree, so a
/// relay picks up children added to its file after it started (R6 elastic
/// membership). An absent path (no `$HOME`) simply never grows.
struct FileChildSource {
    path: Option<PathBuf>,
    source: Vec<u8>,
    toolchain: String,
    binary_name: String,
}

impl ChildSource<Ssh> for FileChildSource {
    fn poll(&mut self, present: &[String]) -> Vec<Ssh> {
        let Some(path) = &self.path else {
            return Vec::new();
        };
        read_children_file(path)
            .into_iter()
            .map(|child| {
                Ssh::build(
                    child,
                    self.source.clone(),
                    self.toolchain.clone(),
                    self.binary_name.clone(),
                )
            })
            .filter(|ssh| !present.contains(&ssh.label()))
            .collect()
    }
}

/// Run over `parent` as a leaf (no children) or a relay (cascading to children).
/// A relay re-reads its children file as it runs, so it absorbs children added to
/// the file after it started.
async fn dispatch<P>(
    parent: Connection<P>,
    children: Vec<SshConfig>,
    config: NodeConfig,
) -> io::Result<()>
where
    P: AsyncRead + AsyncWrite + Unpin + Send,
{
    if children.is_empty() {
        return serve(parent, config.registry).await;
    }
    let launchers = child_launchers(children, &config);
    let source = FileChildSource {
        path: children_path(),
        source: config.source,
        toolchain: config.toolchain,
        binary_name: config.binary_name,
    };
    relay_with_source(parent, launchers, source).await
}

/// Run this node in agent mode over its stdio.
///
/// A leaf if it has no children file, else a relay over the children it names
/// (which reports its subtree's state up to its parent).
///
/// A binary's `main` must call [`agent_main`], never this, because only
/// `agent_main` exits the process when serving ends, which avoids the
/// `tokio::io::stdin` blocking-thread hang on a graceful self-termination (see
/// its docs for the full reason). This lower-level form returns its result and
/// exists for the node and relay tests, which drive it directly.
///
/// # Errors
/// Returns an error on a protocol violation or a transport failure.
pub async fn run_node(config: NodeConfig) -> io::Result<()> {
    dispatch(agent_connection(), load_children(), config).await
}

/// The process exit code for an agent run: 0 on success, 1 on error (logged to
/// stderr, which the parent captures verbatim).
fn agent_exit_code(result: io::Result<()>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("rayonet agent: {error}");
            1
        }
    }
}

/// Run this node as an agent over its stdio, then terminate the process.
///
/// This is the entry point an agent binary's `main` should call once
/// [`crate::process::is_agent`] is true. It runs the node ([`run_node`]) and,
/// when serving ends, exits the process rather than returning.
///
/// Exiting directly is deliberate, not a shortcut. An agent reads its parent
/// over `tokio::io::stdin`, which tokio drives from a blocking thread that
/// cannot be cancelled while a read is outstanding. A live parent holds the
/// agent's stdin open, so once the agent has nothing left to do (most
/// importantly a relay that has lost its whole subtree and is tearing down) a
/// graceful runtime shutdown would block forever on that thread and the process
/// would never close its stdout. The parent, waiting on that stdout for
/// end-of-stream, would then hang too. Exiting closes stdout at once, which is
/// exactly what lets the parent observe the agent's departure and reroute.
/// Returning from `run_node` first guarantees the agent's final frames were
/// flushed to the parent, so nothing is truncated.
pub async fn agent_main(config: NodeConfig) -> ! {
    std::process::exit(agent_exit_code(run_node(config).await));
}

#[cfg(test)]
mod tests {
    use super::{
        child_launchers, children_path, dispatch, load_children, FileChildSource, NodeConfig,
    };
    use crate::agent::{handler, Registry};
    use crate::fleet::Launch;
    use crate::protocol::{FromAgent, ToAgent, PROTOCOL_VERSION};
    use crate::relay::ChildSource;
    use crate::ssh::{parse_host_list, SshConfig};
    use crate::testing::connection_pair;

    #[test]
    fn file_child_source_yields_only_children_not_already_present() {
        let dir = std::env::temp_dir();
        let file = dir.join("rayonet-childsource-test");
        std::fs::write(&file, "alpha\nbeta\n").unwrap();
        let mut source = FileChildSource {
            path: Some(file.clone()),
            source: Vec::new(),
            toolchain: "stable".to_string(),
            binary_name: "consumer".to_string(),
        };
        // alpha is already in the subtree, so only beta is new.
        let new = source.poll(&["alpha".to_string()]);
        assert_eq!(new.len(), 1);
        assert_eq!(new[0].label(), "beta");
        // With both present the re-read adds nothing.
        assert!(source
            .poll(&["alpha".to_string(), "beta".to_string()])
            .is_empty());
        std::fs::remove_file(&file).ok();

        // No path (no $HOME) means a relay that never grows.
        let mut rootless = FileChildSource {
            path: None,
            source: Vec::new(),
            toolchain: "stable".to_string(),
            binary_name: "consumer".to_string(),
        };
        assert!(rootless.poll(&[]).is_empty());
    }

    fn config(registry: Registry) -> NodeConfig {
        NodeConfig::new(
            registry,
            b"source-bundle".to_vec(),
            "consumer".to_string(),
            "stable".to_string(),
        )
    }

    #[test]
    fn children_path_and_loading_follow_the_env_override() {
        // One serial test owns the RAYONET_CHILDREN env to avoid racing peers.
        let dir = std::env::temp_dir();
        let file = dir.join("rayonet-children-test");
        std::fs::write(&file, "mac\n# comment\nbox=~/.ssh/k\n").unwrap();

        std::env::set_var("RAYONET_CHILDREN", &file);
        assert_eq!(children_path(), Some(file.clone()));
        assert_eq!(load_children().len(), 2);

        // Without the override the path falls back under $HOME.
        std::env::remove_var("RAYONET_CHILDREN");
        std::env::set_var("HOME", "/home/test");
        let fallback = children_path().unwrap();
        assert!(
            fallback.ends_with(".config/rayonet/children"),
            "{fallback:?}"
        );

        std::fs::remove_file(&file).ok();
    }

    #[test]
    fn agent_exit_code_maps_success_and_failure() {
        // agent_main exits with this code: 0 for a clean serve, 1 for an error.
        assert_eq!(super::agent_exit_code(Ok(())), 0);
        assert_eq!(
            super::agent_exit_code(Err(std::io::Error::other("boom"))),
            1
        );
    }

    #[test]
    fn a_missing_children_file_means_no_children() {
        let missing = std::path::Path::new("/no/such/rayonet/children");
        assert!(super::read_children_file(missing).is_empty());
    }

    #[test]
    fn child_launchers_build_one_per_child() {
        let children = parse_host_list("a b c");
        let cfg = config(Registry::new());
        assert!(format!("{cfg:?}").contains("NodeConfig"));
        let launchers = child_launchers(children, &cfg);
        assert_eq!(launchers.len(), 3);
    }

    #[tokio::test]
    async fn a_childless_node_serves_as_a_leaf() {
        // With no children, dispatch serves the registry directly over `parent`.
        let registry = Registry::new().with("double", handler(|x: u32| x * 2));
        let (coord, node_side) = connection_pair(256);
        let node = dispatch(node_side, Vec::new(), config(registry));
        let driver = async {
            let (mut tx, mut rx) = coord.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "double".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
            let ready: FromAgent = rx.recv().await.unwrap().unwrap();
            assert_eq!(ready, FromAgent::Ready { slots: 1 });
            tx.send(&ToAgent::Assign {
                task_id: 0,
                payload: postcard::to_allocvec(&21u32).unwrap(),
            })
            .await
            .unwrap();
            // Drain until the completion, then shut the leaf down.
            loop {
                match rx.recv::<FromAgent>().await.unwrap().unwrap() {
                    FromAgent::Completed { output, .. } => {
                        assert_eq!(postcard::from_bytes::<u32>(&output).unwrap(), 42);
                        break;
                    }
                    FromAgent::Started { .. } => {}
                    other => panic!("unexpected {other:?}"),
                }
            }
            tx.send(&ToAgent::Shutdown).await.unwrap();
        };
        let (res, ()) = tokio::join!(node, driver);
        res.unwrap();
    }

    #[tokio::test]
    #[ignore = "spawns ssh to an unresolvable host; run with --include-ignored"]
    async fn a_node_with_children_takes_the_relay_path() {
        // A child on an unresolvable host fails to launch, so the relay finds no
        // usable child and errors: this drives the relay branch of dispatch.
        let children = vec![SshConfig::new("rayonet-child.invalid")];
        let (coord, node_side) = connection_pair(256);
        let node = dispatch(node_side, children, config(Registry::new()));
        let driver = async {
            let (mut tx, _rx) = coord.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "double".to_string(),
                heartbeat: crate::heartbeat::HeartbeatConfig::default(),
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(node, driver);
        assert!(res.is_err());
    }
}
