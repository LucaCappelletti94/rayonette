//! Build-time extraction for rayonet, called from a consumer crate's `build.rs`
//! (DECISIONS.md decision 9). It parses the consumer crate, bundles whole-crate
//! source for shipping, and generates the agent task registry.
//!
//! Task functions are identified by their `.netmap(IDENT)` call sites
//! (DECISIONS.md decision 12, option 1): each named function passed to `.netmap`
//! is registered on the agent under its `type_name`.

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

/// Build-script entry point (DECISIONS.md decision 9).
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

    let src_dir = std::path::Path::new(&manifest_dir).join("src");
    let sources = extract_into(&src_dir, std::path::Path::new(&out_dir))?;
    for source in sources {
        println!("cargo:rerun-if-changed={}", source.display());
    }
    Ok(())
}

/// Scan `src_dir` for `.netmap` task functions and write the registry.
///
/// Returns the source files scanned (for rerun-if-changed).
fn extract_into(
    src_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let mut sources = Vec::new();
    collect_rs_files(src_dir, &mut sources)?;

    let mut functions = Vec::new();
    for source in &sources {
        let text = std::fs::read_to_string(source)?;
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
    Ok(sources)
}

fn collect_rs_files(
    dir: &std::path::Path,
    out: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_rs_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
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

        let sources = super::extract_into(&src, &out).unwrap();
        assert_eq!(sources.len(), 2);
        let registry = std::fs::read_to_string(out.join("rayonet_registry.rs")).unwrap();
        assert!(registry.contains("with_fn(double)"));
        assert!(registry.contains("with_fn(triple)"));
    }

    #[test]
    fn extract_into_rejects_unparseable_source() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("bad.rs"), "fn (").unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();
        assert!(super::extract_into(&src, &out).is_err());
    }

    #[test]
    fn extract_reads_build_env_and_errors_when_unset() {
        let tmp = tempfile::tempdir().unwrap();
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
