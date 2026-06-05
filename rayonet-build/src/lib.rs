//! Build-time extraction for rayonet, called from a consumer crate's `build.rs`.
//! It parses the consumer crate, bundles whole-crate source for shipping, and
//! generates the agent task registry.
//!
//! Task functions are identified by their `.netmap(IDENT)` call sites: each
//! named function passed to `.netmap` is registered on the agent under its
//! `type_name`.

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
/// Discovers task functions from `.netmap` call sites in the consumer crate's
/// `src/` and writes the generated agent registry to `OUT_DIR/rayonet_registry.rs`
/// for `rayonet::embed_microcrates!` to include.
///
/// # Errors
/// Returns an error if the build environment is missing, a source file cannot be
/// read or parsed, or the registry cannot be written.
pub fn extract() -> std::io::Result<()> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR")
        .ok_or_else(|| std::io::Error::other("CARGO_MANIFEST_DIR is unset (run from build.rs)"))?;
    let out_dir = std::env::var_os("OUT_DIR")
        .ok_or_else(|| std::io::Error::other("OUT_DIR is unset (run from build.rs)"))?;

    let manifest_dir = std::path::Path::new(&manifest_dir);
    check_path_dependencies(manifest_dir)?;
    let sources = extract_into(manifest_dir, std::path::Path::new(&out_dir))?;
    for source in sources {
        println!("cargo:rerun-if-changed={}", source.display());
    }
    Ok(())
}

/// Scan the crate's `src/` for `.netmap` task functions, write the registry, and
/// bundle the whole-crate source for shipping.
///
/// Returns the source files scanned (for rerun-if-changed).
fn extract_into(
    manifest_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut sources = Vec::new();
    collect_rs_files(manifest_dir, std::path::Path::new("src"), &mut sources)?;

    let mut functions = Vec::new();
    for source in &sources {
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
    bundle_source(manifest_dir, &sources, out_dir)?;
    Ok(sources)
}

/// Tar the whole-crate source (`Cargo.toml`, `Cargo.lock` if present, and the
/// scanned `src/` files) into `OUT_DIR/rayonet_source.tar` for Phase 4 to ship
/// to remote hosts and compile there.
fn bundle_source(
    manifest_dir: &std::path::Path,
    sources: &[std::path::PathBuf],
    out_dir: &std::path::Path,
) -> std::io::Result<()> {
    let file = std::fs::File::create(out_dir.join("rayonet_source.tar"))?;
    let mut tar = tar::Builder::new(file);

    tar.append_path_with_name(manifest_dir.join("Cargo.toml"), "Cargo.toml")?;
    let lock = manifest_dir.join("Cargo.lock");
    if lock.exists() {
        tar.append_path_with_name(&lock, "Cargo.lock")?;
    }
    // `sources` are already relative to `manifest_dir` (see `collect_rs_files`),
    // so they serve directly as the archive entry names.
    for source in sources {
        tar.append_path_with_name(manifest_dir.join(source), source)?;
    }
    tar.finish()
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

        let sources = super::extract_into(tmp.path(), &out).unwrap();
        assert_eq!(sources.len(), 2);
        let registry = std::fs::read_to_string(out.join("rayonet_registry.rs")).unwrap();
        assert!(registry.contains("with_fn(double)"));
        assert!(registry.contains("with_fn(triple)"));
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
