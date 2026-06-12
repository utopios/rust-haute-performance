// =============================================================================
// build.rs — exécuté AUTOMATIQUEMENT par cargo AVANT la compilation.
// Il appelle tonic-build qui lit proto/kvstore.proto et génère un fichier Rust
// (client + serveur + types des messages) dans le dossier OUT_DIR de cargo.
// Ce code généré est ensuite inclus via tonic::include_proto!("kvstore").
// =============================================================================
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/kvstore.proto")?;
    Ok(())
}
