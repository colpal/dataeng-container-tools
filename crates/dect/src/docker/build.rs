use bollard::Docker;
use bollard::query_parameters::BuildImageOptionsBuilder;
use bollard::query_parameters::BuilderVersion;
use bollard::models::BuildInfoAux;
use std::path::Path;
use futures_util::stream::StreamExt;
use http_body_util::{Either, Full};
use bytes::Bytes;

pub fn get_image_tag(path: &Path) -> String {
    let absolute_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let folder_name = absolute_path.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("dataeng");
    format!("{}:test", folder_name)
}

pub async fn build_image(path: &Path, verbose: bool) -> Result<String, Box<dyn std::error::Error>> {
    let docker = Docker::connect_with_local_defaults()?;
    
    println!("Building image from: {}", path.display());
    
    // Create a compressed tar archive of the build context
    let tar = create_tar_archive(path)?;
    
    let image_tag = get_image_tag(path);
    
    let options = BuildImageOptionsBuilder::default()
        .t(&image_tag)
        .rm(true)
        .dockerfile("Dockerfile")
        .version(BuilderVersion::BuilderBuildKit)
        .session(&image_tag)
        .build();
    
    let mut stream = docker.build_image(
        options,
        None,
        Some(Either::Left(Full::new(Bytes::from(tar))))
    );
    
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(output) => {
                if let Some(BuildInfoAux::BuildKit(inner)) = output.aux {
                    if verbose {
                        println!("{:?}", inner);
                    }
                }
                if let Some(error) = output.error {
                    eprintln!("Build error: {}", error);
                    return Err(error.into());
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    
    println!("\n✓ Image built successfully: {}", image_tag);
    Ok(image_tag)
}

fn create_tar_archive(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    use std::io::Write;
    
    let mut tar = tar::Builder::new(Vec::new());
    tar.append_dir_all(".", path)?;
    tar.finish()?;
    
    let uncompressed = tar.into_inner()?;
    let mut c = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    c.write_all(&uncompressed)?;
    let compressed = c.finish()?;
    
    Ok(compressed)
}
