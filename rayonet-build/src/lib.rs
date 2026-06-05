//! Build-time extraction for rayonet, called from a consumer crate's `build.rs`.
//! It generates the agent task registry and bundles a buildable source tree to
//! ship to workers, both included by `rayonet::embed_microcrates!()`.
//!
//! Task functions are identified by their `.netmap(IDENT)` call sites: each
//! named function passed to `.netmap` is registered on the agent under its
//! `type_name`. The source bundle is the consumer's whole workspace (excluding
//! build output), so a worker can resolve and build it even when rayonet is an
//! unpublished path dependency.

/// Find the named functions passed to `.netmap(...)` call sites in `source`,
/// in source order. Closures and other non-path arguments are skipped (only
/// named functions can be registered by name).
///
/// # Errors
/// Returns an error if `source` is not parseable Rust.
pub fn find_netmap_calls(source: &str) -> syn::Result<Vec<String>> {
    let file = syn::parse_file(source)?;
    let mut finder = NetmapFinder::default();
    syn::visit::Visit::visit_file(&mut finder, &file);
    Ok(finder.calls)
}

#[derive(Default)]
struct NetmapFinder {
    calls: Vec<String>,
}

impl<'ast> syn::visit::Visit<'ast> for NetmapFinder {
    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        if call.method == "netmap" {
            if let Some(syn::Expr::Path(arg)) = call.args.first() {
                self.calls.push(path_string(&arg.path));
            }
        }
        // Recurse so nested and chained `.netmap(..)` calls are found too.
        syn::visit::visit_expr_method_call(self, call);
    }
}

fn path_string(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

/// Generate the agent-registry expression for the discovered task functions.
///
/// Produces `rayonet::agent::Registry::new().with_fn(a).with_fn(b)`, included
/// verbatim by `rayonet::embed_microcrates!()` in the consumer crate.
#[must_use]
pub fn generate_registry(functions: &[String]) -> String {
    use std::fmt::Write as _;
    let mut code = String::from("rayonet::agent::Registry::new()");
    for function in functions {
        let _ = write!(code, ".with_fn({function})");
    }
    code
}

/// Build-script entry point.
///
/// Writes the generated agent registry to `OUT_DIR/rayonet_registry.rs` and the
/// shippable source bundle to `OUT_DIR/rayonet_source.tar`, both for
/// `rayonet::embed_microcrates!` to include.
///
/// # Errors
/// Returns an error if the build environment is missing, a source file cannot be
/// read or parsed, or an output cannot be written.
pub fn extract() -> std::io::Result<()> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .ok_or_else(|| std::io::Error::other("CARGO_MANIFEST_DIR is unset (run from build.rs)"))?;
    let out_dir = std::env::var_os("OUT_DIR")
        .ok_or_else(|| std::io::Error::other("OUT_DIR is unset (run from build.rs)"))?;

    let manifest_dir = std::path::Path::new(&manifest_dir);
    check_path_dependencies(manifest_dir)?;
    let bundled = extract_into(manifest_dir, std::path::Path::new(&out_dir))?;
    // Re-run (re-bundle) whenever any bundled source file changes.
    for path in bundled {
        println!("cargo:rerun-if-changed={}", path.display());
    }
    Ok(())
}

/// Scan the crate's `src/` for `.netmap` task functions, write the registry, and
/// bundle the buildable source tree.
///
/// Returns the bundled files (for rerun-if-changed).
fn extract_into(
    manifest_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut src_files = Vec::new();
    collect_rs_files(manifest_dir, std::path::Path::new("src"), &mut src_files)?;

    let mut functions = Vec::new();
    for source in &src_files {
        let text = std::fs::read_to_string(manifest_dir.join(source))?;
        let calls = find_netmap_calls(&text)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        for call in calls {
            if !functions.contains(&call) {
                functions.push(call);
            }
        }
    }

    std::fs::write(
        out_dir.join("rayonet_registry.rs"),
        generate_registry(&functions),
    )?;
    bundle_source(&workspace_root(manifest_dir), out_dir)
}

/// The consumer's workspace root: the highest ancestor whose `Cargo.toml`
/// declares a `[workspace]`, or the crate itself if there is none.
fn workspace_root(manifest_dir: &std::path::Path) -> std::path::PathBuf {
    let mut root = manifest_dir.to_path_buf();
    for dir in manifest_dir.ancestors() {
        if let Ok(text) = std::fs::read_to_string(dir.join("Cargo.toml")) {
            if text
                .parse::<toml::Table>()
                .is_ok_and(|manifest| manifest.contains_key("workspace"))
            {
                root = dir.to_path_buf();
            }
        }
    }
    root
}

/// Tar the source tree under `root` (a buildable tree: the whole workspace, so
/// path dependencies resolve) into `OUT_DIR/rayonet_source.tar`, skipping build
/// output and VCS metadata. Returns the bundled files.
fn bundle_source(
    root: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let file = std::fs::File::create(out_dir.join("rayonet_source.tar"))?;
    let mut tar = tar::Builder::new(file);
    let mut bundled = Vec::new();
    append_tree(
        &mut tar,
        root,
        std::path::Path::new(""),
        out_dir,
        &mut bundled,
    )?;
    tar.finish()?;
    Ok(bundled)
}

/// Recursively add the files under `root.join(rel)` to `tar` (named by their
/// path relative to `root`), skipping `target`, `.git`, and `out_dir`.
fn append_tree<W: std::io::Write>(
    tar: &mut tar::Builder<W>,
    root: &std::path::Path,
    rel: &std::path::Path,
    out_dir: &std::path::Path,
    bundled: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root.join(rel))? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "target" || name == ".git" {
            continue;
        }
        let absolute = entry.path();
        if absolute == out_dir {
            continue;
        }
        let child = rel.join(&name);
        if entry.file_type()?.is_dir() {
            append_tree(tar, root, &child, out_dir, bundled)?;
        } else {
            tar.append_path_with_name(&absolute, &child)?;
            bundled.push(absolute);
        }
    }
    Ok(())
}

/// Error if the consumer's `Cargo.toml` has a non-rayonet local `path`
/// dependency (v1 cannot ship local path crates).
/// Detects direct path deps; the transitive cascade is a future enhancement.
fn check_path_dependencies(manifest_dir: &std::path::Path) -> std::io::Result<()> {
    let text = std::fs::read_to_string(manifest_dir.join("Cargo.toml"))?;
    let manifest: toml::Table = text.parse().map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Cargo.toml: {e}"))
    })?;

    for section in ["dependencies", "build-dependencies", "dev-dependencies"] {
        let Some(deps) = manifest.get(section).and_then(toml::Value::as_table) else {
            continue;
        };
        for (name, spec) in deps {
            if name == "rayonet" || name == "rayonet-build" {
                continue;
            }
            if spec.as_table().is_some_and(|t| t.contains_key("path")) {
                return Err(std::io::Error::other(format!(
                    "rayonet does not support local path dependencies yet: `{name}` uses \
                     `path = ...`; publish, vendor, or inline it"
                )));
            }
        }
    }
    Ok(())
}

/// Recursively collect `.rs` files under `base.join(rel)`, pushing each as a
/// path relative to `base` (so `rel` accumulates down the tree). Relative paths
/// double as archive entry names in [`bundle_source`] and as rerun-if-changed
/// keys, with no fallible prefix stripping.
fn collect_rs_files(
    base: &std::path::Path,
    rel: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(base.join(rel))? {
        let entry = entry?;
        let child = rel.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            collect_rs_files(base, &child, out)?;
        } else if child.extension().is_some_and(|ext| ext == "rs") {
            out.push(child);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::find_netmap_calls;

    #[test]
    fn extract_into_scans_subdirs() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]\nname = \"c\"\n").unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("main.rs"), "fn a() { let _ = x.netmap(double); }").unwrap();
        std::fs::write(
            src.join("sub").join("more.rs"),
            "fn b() { let _ = y.netmap(triple); }",
        )
        .unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        let bundled = super::extract_into(tmp.path(), &out).unwrap();
        // Both nested `.netmap` functions are registered, and the nested source
        // file is in the bundle.
        let registry = std::fs::read_to_string(out.join("rayonet_registry.rs")).unwrap();
        assert!(registry.contains("with_fn(double)"));
        assert!(registry.contains("with_fn(triple)"));
        assert!(
            bundled.iter().any(|p| p.ends_with("more.rs")),
            "{bundled:?}"
        );
    }

    #[test]
    fn bundles_from_the_workspace_root() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Cargo.toml"),
            "[workspace]\nmembers = [\"member\"]\n",
        )
        .unwrap();
        // A sibling path-dependency crate the member would build against.
        std::fs::create_dir_all(tmp.path().join("dep/src")).unwrap();
        std::fs::write(
            tmp.path().join("dep/Cargo.toml"),
            "[package]\nname = \"dep\"\n",
        )
        .unwrap();
        std::fs::write(tmp.path().join("dep/src/lib.rs"), "").unwrap();
        // The consumer member.
        std::fs::create_dir_all(tmp.path().join("member/src")).unwrap();
        std::fs::write(
            tmp.path().join("member/Cargo.toml"),
            "[package]\nname = \"member\"\n",
        )
        .unwrap();
        std::fs::write(
            tmp.path().join("member/src/main.rs"),
            "fn a() { let _ = x.netmap(go); }",
        )
        .unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        super::extract_into(&tmp.path().join("member"), &out).unwrap();

        // The bundle is rooted at the workspace, so the sibling dep is included.
        let archive = std::fs::File::open(out.join("rayonet_source.tar")).unwrap();
        let names: Vec<String> = tar::Archive::new(archive)
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n == "dep/src/lib.rs"), "{names:?}");
        assert!(names.iter().any(|n| n == "member/src/main.rs"), "{names:?}");
    }

    #[test]
    fn bundle_skips_build_output_and_vcs() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]\nname = \"c\"\n").unwrap();
        std::fs::create_dir(tmp.path().join("src")).unwrap();
        std::fs::write(
            tmp.path().join("src").join("main.rs"),
            "fn a() { let _ = x.netmap(double); }",
        )
        .unwrap();
        // Build output and VCS metadata that must never be shipped.
        std::fs::create_dir(tmp.path().join("target")).unwrap();
        std::fs::write(tmp.path().join("target").join("junk.rs"), "garbage").unwrap();
        std::fs::create_dir(tmp.path().join(".git")).unwrap();
        std::fs::write(tmp.path().join(".git").join("config"), "[core]").unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        super::extract_into(tmp.path(), &out).unwrap();

        let archive = std::fs::File::open(out.join("rayonet_source.tar")).unwrap();
        let names: Vec<String> = tar::Archive::new(archive)
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n.ends_with("main.rs")), "{names:?}");
        assert!(
            !names
                .iter()
                .any(|n| n.starts_with("target") || n.starts_with(".git")),
            "build output or vcs leaked: {names:?}"
        );
    }

    #[test]
    fn extract_into_rejects_unparseable_source() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("src")).unwrap();
        std::fs::write(tmp.path().join("src").join("bad.rs"), "fn (").unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();
        assert!(super::extract_into(tmp.path(), &out).is_err());
    }

    #[test]
    fn extract_into_bundles_source_and_lockfile() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]\nname = \"c\"\n").unwrap();
        std::fs::write(tmp.path().join("Cargo.lock"), "# lock\n").unwrap();
        std::fs::create_dir(tmp.path().join("src")).unwrap();
        std::fs::write(
            tmp.path().join("src").join("main.rs"),
            "fn a() { let _ = x.netmap(double); }",
        )
        .unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        super::extract_into(tmp.path(), &out).unwrap();

        let archive = std::fs::File::open(out.join("rayonet_source.tar")).unwrap();
        let names: Vec<String> = tar::Archive::new(archive)
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n == "Cargo.toml"), "{names:?}");
        assert!(names.iter().any(|n| n == "Cargo.lock"), "{names:?}");
        assert!(names.iter().any(|n| n.ends_with("main.rs")), "{names:?}");
    }

    #[test]
    fn extract_reads_build_env_and_errors_when_unset() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Cargo.toml"),
            "[dependencies]\nrayonet = { path = \"../rayonet\" }\n",
        )
        .unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("main.rs"), "fn a() { let _ = x.netmap(double); }").unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        std::env::set_var("CARGO_MANIFEST_DIR", tmp.path());
        std::env::set_var("OUT_DIR", &out);
        super::extract().unwrap();
        let registry = std::fs::read_to_string(out.join("rayonet_registry.rs")).unwrap();
        assert_eq!(registry, "rayonet::agent::Registry::new().with_fn(double)");

        std::env::remove_var("OUT_DIR");
        assert!(super::extract().is_err());
        std::env::remove_var("CARGO_MANIFEST_DIR");
        assert!(super::extract().is_err());
    }

    #[test]
    fn rejects_non_rayonet_path_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Cargo.toml"),
            "[dependencies]\n\
             rayonet = { path = \"../rayonet\" }\n\
             helper = { path = \"../helper\" }\n\
             serde = \"1\"\n",
        )
        .unwrap();
        let err = super::check_path_dependencies(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("helper"));
    }

    #[test]
    fn accepts_rayonet_path_and_registry_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Cargo.toml"),
            "[dependencies]\n\
             rayonet = { path = \"../rayonet\" }\n\
             serde = \"1\"\n\
             [build-dependencies]\n\
             rayonet-build = { path = \"../rayonet-build\" }\n",
        )
        .unwrap();
        super::check_path_dependencies(tmp.path()).unwrap();
    }

    #[test]
    fn rejects_unparseable_cargo_toml() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "not = = valid").unwrap();
        assert!(super::check_path_dependencies(tmp.path()).is_err());
    }

    #[test]
    fn finds_named_function_netmap_call_sites() {
        let source = r"
            fn run() {
                let a = data.into_par_iter().netmap(evolve).collect();
                things.netmap(score);
                ignored.netmap(|x| x);
                regular_call(foo);
            }
        ";
        let calls = find_netmap_calls(source).unwrap();
        assert_eq!(calls, vec!["evolve".to_string(), "score".to_string()]);
    }

    #[test]
    fn rejects_unparseable_source() {
        assert!(find_netmap_calls("fn (").is_err());
    }

    #[test]
    fn generates_registry_from_functions() {
        let code = super::generate_registry(&["double".to_string(), "tasks::score".to_string()]);
        assert_eq!(
            code,
            "rayonet::agent::Registry::new().with_fn(double).with_fn(tasks::score)"
        );
    }

    #[test]
    fn generates_an_empty_registry() {
        assert_eq!(
            super::generate_registry(&[]),
            "rayonet::agent::Registry::new()"
        );
    }
}
