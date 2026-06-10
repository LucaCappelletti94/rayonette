//! Build-time source bundling for rayonette, called from a consumer crate's
//! `build.rs`. It tars a buildable source tree to ship to workers, included by
//! `rayonette::embed_microcrates!()`.
//!
//! The source bundle is the consumer's whole workspace (excluding build output),
//! so a worker can resolve and build it even when rayonette is an unpublished
//! path dependency. Task discovery is no longer a build-time concern: the
//! `#[rayonette::tasks]` macro registers tasks at compile time and the agent
//! builds its registry from that inventory at boot.

// In non-test code the only sanctioned panic surface is a documented `expect()`,
// so these bans keep `unwrap`, `panic!`, `unreachable!`, and a message-less
// assert out. Test code is exempt, and any binaries are separate crates this
// attribute never reaches.
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_in_result,
        clippy::panic_in_result_fn,
        clippy::get_unwrap,
        clippy::missing_assert_message
    )
)]

/// Build-script entry point.
///
/// Writes the shippable source bundle to `OUT_DIR/rayonette_source.tar`, for
/// `rayonette::embed_microcrates!` to include.
///
/// # Errors
/// Returns an error if the build environment is missing, a source file cannot be
/// read, or the output cannot be written.
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

/// Bundle the buildable source tree rooted at the consumer's workspace.
///
/// Returns the bundled files (for rerun-if-changed).
fn extract_into(
    manifest_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
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
/// path dependencies resolve) into `OUT_DIR/rayonette_source.tar`, skipping build
/// output and VCS metadata. Returns the bundled files.
fn bundle_source(
    root: &std::path::Path,
    out_dir: &std::path::Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    let file = std::fs::File::create(out_dir.join("rayonette_source.tar"))?;
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
///
/// Entries are sorted by name so the archive order does not depend on the
/// filesystem's `read_dir` order.
fn append_tree<W: std::io::Write>(
    tar: &mut tar::Builder<W>,
    root: &std::path::Path,
    rel: &std::path::Path,
    out_dir: &std::path::Path,
    bundled: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    let mut entries = std::fs::read_dir(root.join(rel))?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
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
            append_file(tar, &absolute, &child)?;
            bundled.push(absolute);
        }
    }
    Ok(())
}

/// Append one file under a canonical header (zeroed mtime and ownership, fixed
/// mode), so a tree's archive bytes depend only on file names and contents, not
/// on filesystem metadata that varies build to build. This keeps the bundle's
/// content hash stable, which is what the remote build cache keys on.
fn append_file<W: std::io::Write>(
    tar: &mut tar::Builder<W>,
    absolute: &std::path::Path,
    name: &std::path::Path,
) -> std::io::Result<()> {
    let mut file = std::fs::File::open(absolute)?;
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(file.metadata()?.len());
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_uid(0);
    header.set_gid(0);
    // `append_data` sets the path and recomputes the checksum.
    tar.append_data(&mut header, name, &mut file)
}

/// Error if the consumer's `Cargo.toml` has a non-rayonette local `path`
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
            if name == "rayonette" || name == "rayonette-build" {
                continue;
            }
            if spec.as_table().is_some_and(|t| t.contains_key("path")) {
                return Err(std::io::Error::other(format!(
                    "rayonette does not support local path dependencies yet: `{name}` uses \
                     `path = ...`; publish, vendor, or inline it"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn extract_into_bundles_nested_subdirs() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]\nname = \"c\"\n").unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("main.rs"), "fn a() {}").unwrap();
        std::fs::write(src.join("sub").join("more.rs"), "fn b() {}").unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        let bundled = super::extract_into(tmp.path(), &out).unwrap();
        // The nested source file is bundled (the tree walk recurses subdirs).
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
            "fn a() { let _ = x.net_map(go); }",
        )
        .unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        super::extract_into(&tmp.path().join("member"), &out).unwrap();

        // The bundle is rooted at the workspace, so the sibling dep is included.
        let archive = std::fs::File::open(out.join("rayonette_source.tar")).unwrap();
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
            "fn a() { let _ = x.net_map(double); }",
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

        let archive = std::fs::File::open(out.join("rayonette_source.tar")).unwrap();
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
    fn bundle_is_byte_reproducible_despite_mtimes() {
        use std::time::{Duration, SystemTime};

        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("Cargo.toml"), "[package]\nname = \"c\"\n").unwrap();
        std::fs::create_dir(src.path().join("src")).unwrap();
        let main = src.path().join("src").join("main.rs");
        std::fs::write(&main, "fn a() { let _ = x.net_map(double); }").unwrap();

        // Bundle into an out dir outside the source tree (so it is not itself
        // bundled), capturing the bytes.
        let out_a = tempfile::tempdir().unwrap();
        super::extract_into(src.path(), out_a.path()).unwrap();
        let tar_a = std::fs::read(out_a.path().join("rayonette_source.tar")).unwrap();

        // Change a file's mtime without touching its content, then re-bundle.
        let file = std::fs::File::options().write(true).open(&main).unwrap();
        file.set_modified(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000))
            .unwrap();
        let out_b = tempfile::tempdir().unwrap();
        super::extract_into(src.path(), out_b.path()).unwrap();
        let tar_b = std::fs::read(out_b.path().join("rayonette_source.tar")).unwrap();

        assert_eq!(
            tar_a, tar_b,
            "the source bundle must be byte-reproducible regardless of file mtimes"
        );
    }

    #[test]
    fn extract_into_bundles_source_and_lockfile() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("Cargo.toml"), "[package]\nname = \"c\"\n").unwrap();
        std::fs::write(tmp.path().join("Cargo.lock"), "# lock\n").unwrap();
        std::fs::create_dir(tmp.path().join("src")).unwrap();
        std::fs::write(
            tmp.path().join("src").join("main.rs"),
            "fn a() { let _ = x.net_map(double); }",
        )
        .unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        super::extract_into(tmp.path(), &out).unwrap();

        let archive = std::fs::File::open(out.join("rayonette_source.tar")).unwrap();
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
            "[dependencies]\nrayonette = { path = \"../rayonette\" }\n",
        )
        .unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir(&src).unwrap();
        std::fs::write(src.join("main.rs"), "fn a() { let _ = x.net_map(double); }").unwrap();
        let out = tmp.path().join("out");
        std::fs::create_dir(&out).unwrap();

        std::env::set_var("CARGO_MANIFEST_DIR", tmp.path());
        std::env::set_var("OUT_DIR", &out);
        super::extract().unwrap();
        // The source bundle is produced from the build environment.
        assert!(out.join("rayonette_source.tar").exists());

        std::env::remove_var("OUT_DIR");
        assert!(super::extract().is_err());
        std::env::remove_var("CARGO_MANIFEST_DIR");
        assert!(super::extract().is_err());
    }

    #[test]
    fn rejects_non_rayonette_path_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Cargo.toml"),
            "[dependencies]\n\
             rayonette = { path = \"../rayonette\" }\n\
             helper = { path = \"../helper\" }\n\
             serde = \"1\"\n",
        )
        .unwrap();
        let err = super::check_path_dependencies(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("helper"));
    }

    #[test]
    fn accepts_rayonette_path_and_registry_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("Cargo.toml"),
            "[dependencies]\n\
             rayonette = { path = \"../rayonette\" }\n\
             serde = \"1\"\n\
             [build-dependencies]\n\
             rayonette-build = { path = \"../rayonette-build\" }\n",
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
}
