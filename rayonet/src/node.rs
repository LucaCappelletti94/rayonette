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
use crate::framing::Connection;
use crate::observability::{EventSink, NoopSink};
use crate::process::agent_connection;
use crate::relay::relay;
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
    pub registry: Registry,
    /// The crate source tarball to ship to children (a relay's `__rayonet_source()`).
    pub source: Vec<u8>,
    /// The agent binary name to build on children.
    pub binary_name: String,
    /// The rust toolchain to build children with.
    pub toolchain: String,
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

/// Run over `parent` as a leaf (no children) or a relay (cascading to children).
async fn dispatch<P>(
    parent: Connection<P>,
    children: Vec<SshConfig>,
    config: NodeConfig,
    events: &dyn EventSink,
) -> io::Result<()>
where
    P: AsyncRead + AsyncWrite + Unpin + Send,
{
    if children.is_empty() {
        serve(parent, config.registry).await
    } else {
        relay(parent, child_launchers(children, &config), events).await
    }
}

/// Run this node in agent mode over its stdio: a leaf if it has no children file,
/// else a relay over the children it names. Call from the consumer's `main` when
/// [`crate::process::is_agent`] is true.
///
/// # Errors
/// Returns an error on a protocol violation or a transport failure.
pub async fn run_node(config: NodeConfig) -> io::Result<()> {
    dispatch(agent_connection(), load_children(), config, &NoopSink).await
}

#[cfg(test)]
mod tests {
    use super::{child_launchers, children_path, dispatch, load_children, NodeConfig};
    use crate::agent::{handler, Registry};
    use crate::observability::NoopSink;
    use crate::protocol::{FromAgent, ToAgent, PROTOCOL_VERSION};
    use crate::ssh::{parse_host_list, SshConfig};
    use crate::testing::connection_pair;

    fn config(registry: Registry) -> NodeConfig {
        NodeConfig {
            registry,
            source: b"source-bundle".to_vec(),
            binary_name: "consumer".to_string(),
            toolchain: "stable".to_string(),
        }
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
        let node = dispatch(node_side, Vec::new(), config(registry), &NoopSink);
        let driver = async {
            let (mut tx, mut rx) = coord.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "double".to_string(),
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
        let node = dispatch(node_side, children, config(Registry::new()), &NoopSink);
        let driver = async {
            let (mut tx, _rx) = coord.split();
            tx.send(&ToAgent::Hello {
                protocol_version: PROTOCOL_VERSION,
                fn_key: "double".to_string(),
            })
            .await
            .unwrap();
        };
        let (res, ()) = tokio::join!(node, driver);
        assert!(res.is_err());
    }
}
